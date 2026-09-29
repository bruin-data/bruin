package config

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type CloudConnectionRef struct {
	Environment string
	Connection  BruinCloudConnection
}

func (c *Config) CloudConnections() []CloudConnectionRef {
	var refs []CloudConnectionRef
	for name, env := range c.Environments {
		if env.Connections == nil {
			continue
		}
		for _, connection := range env.Connections.BruinCloud {
			refs = append(refs, CloudConnectionRef{Environment: name, Connection: connection})
		}
	}
	sort.SliceStable(refs, func(i, j int) bool {
		if refs[i].Environment == refs[j].Environment {
			return refs[i].Connection.Name < refs[j].Connection.Name
		}
		return refs[i].Environment < refs[j].Environment
	})
	return refs
}

func (c *Config) ResolveCloudConnection() (*CloudConnectionRef, error) {
	refs := c.CloudConnections()
	switch len(refs) {
	case 0:
		return nil, nil
	case 1:
		if refs[0].Connection.APIToken == "" {
			return nil, errors.New("configured Bruin Cloud connection has no API token")
		}
		return &refs[0], nil
	default:
		return nil, errors.New("multiple Bruin Cloud connections found; keep a single bruin connection for Cloud commands")
	}
}

func SaveCloudConnection(files afero.Fs, path string, expected []byte, ref CloudConnectionRef, team string) error {
	current, err := afero.ReadFile(files, path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if !bytes.Equal(current, expected) {
		return errors.New("configuration changed during login; retry without overwriting the changes")
	}
	data, err := MarshalCloudConnection(current, ref, team)
	if err != nil {
		return err
	}
	if err := ensureConfigIsInGitignore(files, path); err != nil {
		return err
	}
	return WriteCloudFile(files, path, expected, data)
}

func MarshalCloudConnection(current []byte, ref CloudConnectionRef, team string) ([]byte, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(current, &doc); err != nil {
		return nil, errors.New("cannot update invalid YAML configuration")
	}
	if len(doc.Content) > 0 && doc.Content[0].Kind != yaml.MappingNode {
		return nil, errors.New("configuration must be a YAML mapping")
	}
	root := documentMapping(&doc)
	if mappingValue(root, "default_environment") == nil {
		setScalarChild(root, "default_environment", ref.Environment)
	}
	environments, err := cloudChildMapping(root, "environments")
	if err != nil {
		return nil, err
	}
	environment, err := cloudChildMapping(environments, ref.Environment)
	if err != nil {
		return nil, err
	}
	connections, err := cloudChildMapping(environment, "connections")
	if err != nil {
		return nil, err
	}
	bruin := mappingValue(connections, "bruin")
	if bruin == nil {
		bruin = &yaml.Node{Kind: yaml.SequenceNode}
		appendMappingKey(connections, "bruin", bruin)
	}
	if bruin.Kind != yaml.SequenceNode {
		return nil, errors.New("bruin connections must be a YAML sequence")
	}
	var target *yaml.Node
	for _, entry := range bruin.Content {
		if entry.Kind != yaml.MappingNode {
			return nil, errors.New("bruin connection must be a YAML mapping")
		}
		name := mappingValue(entry, "name")
		if name != nil && name.Value == ref.Connection.Name {
			if target != nil {
				return nil, errors.New("duplicate Bruin Cloud connection name")
			}
			target = entry
		}
	}
	if target == nil {
		target = &yaml.Node{Kind: yaml.MappingNode}
		setScalarChild(target, "name", ref.Connection.Name)
		bruin.Content = append(bruin.Content, target)
	}
	setScalarChild(target, "api_token", ref.Connection.APIToken)
	setScalarChild(target, "api_url", ref.Connection.APIURL)
	cloud, err := cloudChildMapping(root, "cloud")
	if err != nil {
		return nil, err
	}
	setScalarChild(cloud, "default_team", team)
	return yaml.Marshal(&doc)
}

func cloudChildMapping(parent *yaml.Node, key string) (*yaml.Node, error) {
	child := mappingValue(parent, key)
	if child == nil {
		child = &yaml.Node{Kind: yaml.MappingNode}
		appendMappingKey(parent, key, child)
	}
	if child.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s must be a YAML mapping", key)
	}
	return child, nil
}

func WriteCloudFile(files afero.Fs, path string, expected, data []byte) error {
	current, err := afero.ReadFile(files, path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if !bytes.Equal(current, expected) {
		return errors.New("configuration changed during login; retry")
	}
	temporary, err := afero.TempFile(files, filepath.Dir(path), ".bruin-login-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer func() { _ = files.Remove(name) }()
	if err = files.Chmod(name, 0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err = temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err = temporary.Close(); err != nil {
		return err
	}
	current, err = afero.ReadFile(files, path)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if !bytes.Equal(current, expected) {
		return errors.New("configuration changed during login; retry")
	}
	return files.Rename(name, path)
}

func RemoveCloudConnection(files afero.Fs, path string, expected []byte, ref CloudConnectionRef) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(expected, &doc); err != nil {
		return errors.New("invalid repository configuration")
	}
	if len(doc.Content) == 0 || doc.Content[0].Kind != yaml.MappingNode {
		return errors.New("configuration must be a YAML mapping")
	}
	root := doc.Content[0]
	node := root
	for _, key := range []string{"environments", ref.Environment, "connections"} {
		node = mappingValue(node, key)
		if node == nil || node.Kind != yaml.MappingNode {
			return errors.New("connection no longer exists")
		}
	}
	bruin := mappingValue(node, "bruin")
	if bruin == nil || bruin.Kind != yaml.SequenceNode {
		return errors.New("connection no longer exists")
	}
	found := false
	for i, entry := range bruin.Content {
		name := mappingValue(entry, "name")
		if name != nil && name.Value == ref.Connection.Name {
			bruin.Content = append(bruin.Content[:i], bruin.Content[i+1:]...)
			found = true
			break
		}
	}
	if !found {
		return errors.New("connection no longer exists")
	}
	cloud := mappingValue(root, "cloud")
	if cloud != nil && cloud.Kind == yaml.MappingNode {
		deleteMappingKey(cloud, "default_team")
	}
	data, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	return WriteCloudFile(files, path, expected, data)
}
