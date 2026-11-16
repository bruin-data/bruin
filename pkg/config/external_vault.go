package config

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

const externalVaultKey = "bruin_external_vault"

type externalVaultRef struct {
	uri    string
	scheme string
	path   string
}

type secretItemFetcher interface {
	FetchItem(ref externalVaultRef) (map[string]string, error)
}

// vaultProvider reads one item from a secret manager; path is the reference after "<scheme>://".
type vaultProvider interface {
	FetchItem(path string) (map[string]string, error)
}

var defaultVaultProviders = vaultProviders{
	"op": onePasswordCLI{},
}

var defaultExternalVaultFetcher secretItemFetcher = &cachingFetcher{next: defaultVaultProviders}

func parseExternalVaultRef(uri string) (externalVaultRef, error) {
	scheme, path, ok := strings.Cut(uri, "://")
	if !ok || scheme == "" || path == "" {
		return externalVaultRef{}, fmt.Errorf("%q is not a vault reference, expected <scheme>://<path> such as op://<vault>/<item>", uri)
	}
	return externalVaultRef{uri: uri, scheme: scheme, path: path}, nil
}

type vaultProviders map[string]vaultProvider

func (p vaultProviders) provider(scheme string) (vaultProvider, error) {
	provider, ok := p[scheme]
	if !ok {
		supported := make([]string, 0, len(p))
		for scheme := range p {
			supported = append(supported, scheme+"://")
		}
		sort.Strings(supported)
		return nil, fmt.Errorf("unsupported vault scheme %q, supported: %s", scheme, strings.Join(supported, ", "))
	}
	return provider, nil
}

func (p vaultProviders) FetchItem(ref externalVaultRef) (map[string]string, error) {
	provider, err := p.provider(ref.scheme)
	if err != nil {
		return nil, err
	}
	return provider.FetchItem(ref.path)
}

type onePasswordCLI struct{}

func parseOnePasswordPath(path string) (string, string, error) {
	vault, item, ok := strings.Cut(path, "/")
	if !ok || vault == "" || item == "" || strings.Contains(item, "/") {
		return "", "", errors.New("not a valid 1Password reference, expected op://<vault>/<item>")
	}
	return vault, item, nil
}

type onePasswordItem struct {
	Fields []struct {
		Label string `json:"label"`
		Value string `json:"value"`
	} `json:"fields"`
}

func (onePasswordCLI) FetchItem(path string) (map[string]string, error) {
	vault, item, err := parseOnePasswordPath(path)
	if err != nil {
		return nil, err
	}

	stdout, err := runOnePassword("item", "get", item, "--vault", vault, "--format", "json", "--reveal")
	if err != nil {
		return nil, err
	}

	var parsed onePasswordItem
	// The decode error is dropped because it can quote the item's contents.
	if err := json.Unmarshal(stdout, &parsed); err != nil {
		return nil, errors.New("could not parse the output of op item get")
	}

	fields := make(map[string]string, len(parsed.Fields))
	for _, f := range parsed.Fields {
		if f.Label == "" || f.Value == "" {
			continue
		}
		if _, ok := fields[f.Label]; ok {
			return nil, fmt.Errorf("the item has more than one field labelled %q", f.Label)
		}
		fields[f.Label] = f.Value
	}

	return fields, nil
}

// runOnePassword runs op with the terminal as stdin so it can prompt for sign-in.
func runOnePassword(args ...string) ([]byte, error) {
	cmd := exec.CommandContext(context.Background(), "op", args...)
	cmd.Stdin = os.Stdin
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		if errors.Is(err, exec.ErrNotFound) {
			return nil, errors.New("the 1Password CLI (op) is not installed or not on PATH, see https://developer.1password.com/docs/cli/get-started/")
		}
		action := strings.Join(args[:min(2, len(args))], " ")
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("op %s failed: %s", action, msg)
		}
		return nil, fmt.Errorf("op %s failed: %w", action, err)
	}

	return stdout.Bytes(), nil
}

// cachingFetcher fetches each item once per process; failures are not cached so a later load can retry.
type cachingFetcher struct {
	next  secretItemFetcher
	mu    sync.Mutex
	items map[string]map[string]string
}

func (c *cachingFetcher) FetchItem(ref externalVaultRef) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if fields, ok := c.items[ref.uri]; ok {
		return fields, nil
	}

	fields, err := c.next.FetchItem(ref)
	if err != nil {
		return nil, err
	}
	if c.items == nil {
		c.items = make(map[string]map[string]string)
	}
	c.items[ref.uri] = fields

	return fields, nil
}

type externalVaultConnection struct {
	env      string
	connType string
	name     string
	ref      externalVaultRef
	yamlKeys map[string]bool
}

func (v externalVaultConnection) where() string {
	return fmt.Sprintf("connection '%s' (%s) in environment '%s'", v.name, v.connType, v.env)
}

type ExternalVaultReference struct {
	Environment string
	Type        string
	Name        string
	Reference   string
}

func (c *Config) ExternalVaultReferences() []ExternalVaultReference {
	refs := make([]ExternalVaultReference, 0, len(c.externalVault))
	for _, ref := range c.externalVault {
		refs = append(refs, ExternalVaultReference{Environment: ref.env, Type: ref.connType, Name: ref.name, Reference: ref.ref.uri})
	}
	return refs
}

// collectExternalVaultReferences strips bruin_external_vault from every connection; nothing is fetched here.
func collectExternalVaultReferences(root *yaml.Node) ([]externalVaultConnection, error) {
	connectionsType := reflect.TypeOf(Connections{})
	var refs []externalVaultConnection

	envs := yamlMappingValue(yamlDocumentRoot(root), "environments")
	for envName, envNode := range yamlMappingPairs(envs) {
		for connType, seq := range yamlMappingPairs(yamlMappingValue(envNode, "connections")) {
			if _, ok := yamlFieldType(connectionsType, connType); !ok || seq.Kind != yaml.SequenceNode {
				continue
			}

			for _, node := range seq.Content {
				conn := resolveYAMLAlias(node)
				idx := yamlMappingKeyIndex(conn, externalVaultKey)
				if idx < 0 {
					continue
				}

				ref := externalVaultConnection{env: envName, connType: connType, yamlKeys: map[string]bool{}}
				if n := yamlMappingValue(conn, "name"); n != nil {
					ref.name = n.Value
				}
				uriNode := resolveYAMLAlias(conn.Content[idx+1])
				conn.Content = slices.Delete(conn.Content, idx, idx+2)
				for key := range yamlMappingPairs(conn) {
					ref.yamlKeys[key] = true
				}

				if uriNode.Kind != yaml.ScalarNode {
					return nil, fmt.Errorf("%s: %s must be a string like op://<vault>/<item>", ref.where(), externalVaultKey)
				}
				parsed, err := parseExternalVaultRef(uriNode.Value)
				if err != nil {
					return nil, fmt.Errorf("%s: %w", ref.where(), err)
				}
				ref.ref = parsed
				refs = append(refs, ref)
			}
		}
	}

	return refs, nil
}

func (c *Config) fetcher() secretItemFetcher {
	if c.vaultFetcher != nil {
		return c.vaultFetcher
	}
	return defaultExternalVaultFetcher
}

func (c *Config) selectedExternalVaultRef(name string) (externalVaultConnection, bool) {
	for _, ref := range c.externalVault {
		if ref.env == c.SelectedEnvironmentName && ref.name == name {
			return ref, true
		}
	}
	return externalVaultConnection{}, false
}

// HasExternalVaultConnections reports whether any connection of the selected environment reads from a vault.
func (c *Config) HasExternalVaultConnections() bool {
	for _, ref := range c.externalVault {
		if ref.env == c.SelectedEnvironmentName {
			return true
		}
	}
	return false
}

// UsesExternalVault reports whether the named connection of the selected environment reads from a vault.
func (c *Config) UsesExternalVault(name string) bool {
	_, ok := c.selectedExternalVaultRef(name)
	return ok
}

// WithoutExternalVaultConnections returns a copy of the config whose selected environment leaves out the
// connections that read from a vault, so they can be built later, only if a command asks for them.
func (c *Config) WithoutExternalVaultConnections() *Config {
	connections := *c.SelectedEnvironment.Connections
	connections.byKey = nil
	connections.typeNameMap = nil
	value := reflect.ValueOf(&connections).Elem()

	for _, ref := range c.externalVault {
		if ref.env != c.SelectedEnvironmentName {
			continue
		}
		list, ok := connectionsFieldByYAMLKey(value, ref.connType)
		if !ok || list.Kind() != reflect.Slice {
			continue
		}
		kept := reflect.MakeSlice(list.Type(), 0, list.Len())
		for i := range list.Len() {
			if named, ok := list.Index(i).Addr().Interface().(interface{ GetName() string }); ok && named.GetName() == ref.name {
				continue
			}
			kept = reflect.Append(kept, list.Index(i))
		}
		list.Set(kept)
	}

	env := *c.SelectedEnvironment
	env.Connections = &connections
	filtered := *c
	filtered.SelectedEnvironment = &env
	filtered.externalVault = nil
	return &filtered
}

// ExternalVaultConnection reads the named connection's vault item and returns a config that holds only that
// connection, complete. The receiver is not changed, so fetched secrets never reach a config that gets saved.
func (c *Config) ExternalVaultConnection(name string) (*Config, error) {
	ref, ok := c.selectedExternalVaultRef(name)
	if !ok {
		return nil, fmt.Errorf("connection '%s' does not use %s in environment '%s'", name, externalVaultKey, c.SelectedEnvironmentName)
	}

	source, ok := connectionsFieldByYAMLKey(reflect.ValueOf(c.SelectedEnvironment.Connections).Elem(), ref.connType)
	if !ok {
		return nil, fmt.Errorf("%s: unknown connection type", ref.where())
	}
	item, ok := connectionByName(source, name)
	if !ok {
		return nil, fmt.Errorf("%s: could not find the connection", ref.where())
	}

	connections := &Connections{}
	target, _ := connectionsFieldByYAMLKey(reflect.ValueOf(connections).Elem(), ref.connType)
	target.Set(reflect.Append(reflect.MakeSlice(target.Type(), 0, 1), item.Elem()))
	if err := fillFromExternalVault(connections, ref, c.fetcher()); err != nil {
		return nil, err
	}
	if c.path != "" {
		if absolutePath, err := filepath.Abs(c.path); err == nil {
			makeConnectionPathsAbsolute(connections, filepath.Dir(absolutePath))
		}
	}

	env := *c.SelectedEnvironment
	env.Connections = connections
	return &Config{
		DefaultEnvironmentName:  c.SelectedEnvironmentName,
		SelectedEnvironmentName: c.SelectedEnvironmentName,
		SelectedEnvironment:     &env,
		Environments:            map[string]Environment{c.SelectedEnvironmentName: env},
	}, nil
}

func fillFromExternalVault(connections *Connections, ref externalVaultConnection, fetcher secretItemFetcher) error {
	list, ok := connectionsFieldByYAMLKey(reflect.ValueOf(connections).Elem(), ref.connType)
	if !ok || list.Kind() != reflect.Slice {
		return fmt.Errorf("%s: could not find the connection to fill from %s", ref.where(), ref.ref.uri)
	}
	target, ok := connectionByName(list, ref.name)
	if !ok {
		return fmt.Errorf("%s: could not find the connection to fill from %s", ref.where(), ref.ref.uri)
	}
	structType := target.Elem().Type()

	fields, err := fetcher.FetchItem(ref.ref)
	if err != nil {
		return fmt.Errorf("%s: failed to read %s: %w", ref.where(), ref.ref.uri, err)
	}

	settings, err := vaultItemSettings(structType, fields)
	if err != nil {
		return fmt.Errorf("%s: %s: %w", ref.where(), ref.ref.uri, err)
	}

	keys := make([]string, 0, len(settings))
	for key := range settings {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	mapping := &yaml.Node{Kind: yaml.MappingNode}
	for _, key := range keys {
		if ref.yamlKeys[key] {
			return fmt.Errorf("%s: '%s' is set both in the config file and in %s, remove it from one of them", ref.where(), key, ref.ref.uri)
		}

		fieldType, _ := yamlFieldType(structType, key)
		value, err := scalarNodeForType(settings[key], dereferenceType(fieldType))
		if err != nil {
			return fmt.Errorf("%s: field '%s' in %s is not a valid %s", ref.where(), key, ref.ref.uri, dereferenceType(fieldType).Kind())
		}
		mapping.Content = append(mapping.Content, &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: key}, value)
	}

	if err := mapping.Decode(target.Interface()); err != nil {
		return fmt.Errorf("%s: could not apply the fields from %s", ref.where(), ref.ref.uri)
	}
	return nil
}

// labelAliases maps labels of 1Password's built-in item types to Bruin setting names.
var labelAliases = map[string]string{
	"server":   "host",
	"hostname": "host",
}

// vaultItemSettings matches an item's field labels to the connection's settings, ignoring case, spaces and
// dashes, so items made by hand or from 1Password's own templates work too. Labels that match nothing are ignored.
func vaultItemSettings(structType reflect.Type, fields map[string]string) (map[string]string, error) {
	labels := make([]string, 0, len(fields))
	for label := range fields {
		labels = append(labels, label)
	}
	sort.Strings(labels)

	settings := map[string]string{}
	matchedLabel := map[string]string{}
	for _, label := range labels {
		key := settingKeyForLabel(structType, label)
		if key == "" {
			continue
		}
		if previous, ok := matchedLabel[key]; ok {
			return nil, fmt.Errorf("the fields '%s' and '%s' both set '%s', keep only one of them", previous, label, key)
		}
		matchedLabel[key] = label
		settings[key] = fields[label]
	}
	return settings, nil
}

func settingKeyForLabel(structType reflect.Type, label string) string {
	key := strings.NewReplacer(" ", "_", "-", "_").Replace(strings.ToLower(strings.TrimSpace(label)))
	if alias, ok := labelAliases[key]; ok {
		if _, isSetting := yamlFieldType(structType, key); !isSetting {
			key = alias
		}
	}

	fieldType, ok := yamlFieldType(structType, key)
	if !ok || key == "name" || !isYAMLScalarKind(dereferenceType(fieldType).Kind()) {
		return ""
	}
	return key
}

func connectionByName(list reflect.Value, name string) (reflect.Value, bool) {
	for i := range list.Len() {
		item := list.Index(i).Addr()
		if named, ok := item.Interface().(interface{ GetName() string }); ok && named.GetName() == name {
			return item, true
		}
	}
	return reflect.Value{}, false
}

// yamlWithVaultReferences renders the config with each vault-backed connection reduced to the keys its
// file entry had, plus its reference, so saving never adds keys that would clash with the vault item.
func (c *Config) yamlWithVaultReferences() ([]byte, error) {
	var root yaml.Node
	if err := root.Encode(c); err != nil {
		return nil, err
	}

	environments := yamlMappingValue(&root, "environments")
	for _, ref := range c.externalVault {
		conn := vaultConnectionNode(environments, ref)
		if conn == nil {
			return nil, fmt.Errorf("refusing to write %s: %s would lose its %s reference", c.path, ref.where(), externalVaultKey)
		}

		kept := make([]*yaml.Node, 0, len(conn.Content)+2)
		for i := 0; i+1 < len(conn.Content); i += 2 {
			if ref.yamlKeys[conn.Content[i].Value] {
				kept = append(kept, conn.Content[i], conn.Content[i+1])
			}
		}
		conn.Content = append(
			kept,
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: externalVaultKey},
			&yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: ref.ref.uri},
		)
	}

	return yaml.Marshal(&root)
}

func vaultConnectionNode(environments *yaml.Node, ref externalVaultConnection) *yaml.Node {
	connections := yamlMappingValue(yamlMappingValue(environments, ref.env), "connections")
	list := yamlMappingValue(connections, ref.connType)
	if list == nil || list.Kind != yaml.SequenceNode {
		return nil
	}
	for _, conn := range list.Content {
		if name := yamlMappingValue(conn, "name"); name != nil && name.Value == ref.name {
			return conn
		}
	}
	return nil
}

func (c *Config) dropVaultReferences(drop func(externalVaultConnection) bool) {
	c.externalVault = slices.DeleteFunc(c.externalVault, drop)
}

func connectionsFieldByYAMLKey(connections reflect.Value, key string) (reflect.Value, bool) {
	for i := range connections.NumField() {
		field := connections.Type().Field(i)
		if !field.IsExported() {
			continue
		}
		if name, _ := yamlTagName(field); name == key {
			return connections.Field(i), true
		}
	}
	return reflect.Value{}, false
}

func scalarNodeForType(value string, targetType reflect.Type) (*yaml.Node, error) {
	if targetType.Kind() == reflect.String {
		return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: value}, nil
	}

	node := &yaml.Node{Kind: yaml.ScalarNode, Value: value}
	if err := node.Decode(reflect.New(targetType).Interface()); err != nil {
		return nil, err
	}

	return node, nil
}

func isYAMLScalarKind(kind reflect.Kind) bool {
	switch kind { //nolint:exhaustive
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

func yamlDocumentRoot(node *yaml.Node) *yaml.Node {
	if node != nil && node.Kind == yaml.DocumentNode && len(node.Content) > 0 {
		return resolveYAMLAlias(node.Content[0])
	}
	return node
}

func resolveYAMLAlias(node *yaml.Node) *yaml.Node {
	for node != nil && node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	return node
}

func yamlMappingKeyIndex(node *yaml.Node, key string) int {
	if node == nil || node.Kind != yaml.MappingNode {
		return -1
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return i
		}
	}
	return -1
}

func yamlMappingValue(node *yaml.Node, key string) *yaml.Node {
	idx := yamlMappingKeyIndex(node, key)
	if idx < 0 {
		return nil
	}
	return resolveYAMLAlias(node.Content[idx+1])
}

func yamlMappingPairs(node *yaml.Node) iter.Seq2[string, *yaml.Node] {
	return func(yield func(string, *yaml.Node) bool) {
		if node == nil || node.Kind != yaml.MappingNode {
			return
		}
		for i := 0; i+1 < len(node.Content); i += 2 {
			if !yield(node.Content[i].Value, resolveYAMLAlias(node.Content[i+1])) {
				return
			}
		}
	}
}
