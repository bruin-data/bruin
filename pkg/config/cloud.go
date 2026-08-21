package config

import (
	"errors"
	fs2 "io/fs"

	errors2 "github.com/pkg/errors"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

// UpsertDefaultTeam writes cloud.default_team into the config file at path,
// editing the YAML in place rather than re-serializing a decoded Config. That
// matters: decoding expands ${VAR} secret references and drops env-injected
// content, so a full round-trip on save could bake resolved credentials into
// .bruin.yml or clobber it. Node-level editing leaves everything else —
// placeholders, comments, key order — untouched.
//
// An empty team clears the setting, removing the cloud section when it then
// holds nothing. The file (and its .gitignore entry) is created if missing.
func UpsertDefaultTeam(fs afero.Fs, path, team string) error {
	buf, err := afero.ReadFile(fs, path)
	if err != nil && !errors.Is(err, fs2.ErrNotExist) {
		return errors2.Wrapf(err, "failed to read config file %s", path)
	}

	var doc yaml.Node
	if len(buf) > 0 {
		if err := yaml.Unmarshal(buf, &doc); err != nil {
			return errors2.Wrapf(err, "failed to parse config file %s", path)
		}
	}

	root := documentMapping(&doc)
	cloud := mappingValue(root, "cloud")

	if team == "" {
		if cloud != nil {
			deleteMappingKey(cloud, "default_team")
			if len(cloud.Content) == 0 {
				deleteMappingKey(root, "cloud")
			}
		}
	} else {
		if cloud == nil {
			cloud = &yaml.Node{Kind: yaml.MappingNode}
			appendMappingKey(root, "cloud", cloud)
		}
		setScalarChild(cloud, "default_team", team)
	}

	out, err := yaml.Marshal(&doc)
	if err != nil {
		return errors2.Wrapf(err, "failed to serialize config")
	}
	if err := afero.WriteFile(fs, path, out, 0o644); err != nil {
		return errors2.Wrapf(err, "failed to write config file %s", path)
	}
	return ensureConfigIsInGitignore(fs, path)
}

// documentMapping returns the top-level mapping node of doc, creating an empty
// document/mapping when doc is empty or its root isn't a mapping.
func documentMapping(doc *yaml.Node) *yaml.Node {
	if len(doc.Content) == 0 {
		doc.Kind = yaml.DocumentNode
		root := &yaml.Node{Kind: yaml.MappingNode}
		doc.Content = []*yaml.Node{root}
		return root
	}
	root := doc.Content[0]
	if root.Kind != yaml.MappingNode {
		root = &yaml.Node{Kind: yaml.MappingNode}
		doc.Content[0] = root
	}
	return root
}

// mappingValue returns the value node for key in a mapping node, or nil.
func mappingValue(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
}

// deleteMappingKey removes the key/value pair for key from a mapping node.
func deleteMappingKey(m *yaml.Node, key string) {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			m.Content = append(m.Content[:i], m.Content[i+2:]...)
			return
		}
	}
}

// appendMappingKey appends a new key/value pair to a mapping node.
func appendMappingKey(m *yaml.Node, key string, value *yaml.Node) {
	m.Content = append(m.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key},
		value)
}

// setScalarChild sets key to a string scalar in a mapping node, updating the
// existing value node in place when present so surrounding content is kept.
func setScalarChild(m *yaml.Node, key, value string) {
	if v := mappingValue(m, key); v != nil {
		v.Kind = yaml.ScalarNode
		v.Tag = "!!str"
		v.Value = value
		v.Content = nil
		return
	}
	appendMappingKey(m, key, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value})
}
