package secrets

import (
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
)

// parseConnectionSecret turns a secret payload of the form
// {"type": "<connection type>", "details": {...}} into a one-entry config.Connections
// whose connection is called name.
func parseConnectionSecret(name string, payload []byte) (*config.Connections, error) {
	var secretData map[string]any
	if err := json.Unmarshal(payload, &secretData); err != nil {
		return nil, errors.Wrapf(err, "failed to parse secret '%s' as JSON", name)
	}

	detailsRaw, okDetails := secretData["details"]
	secretType, okType := secretData["type"].(string)
	details, detailsIsMap := detailsRaw.(map[string]any)
	if !okDetails || !detailsIsMap || !okType || strings.TrimSpace(secretType) == "" {
		return nil, errors.Errorf("secret '%s' must contain both 'type' (non-empty string) and 'details' (object)", name)
	}

	details["name"] = name

	// Round-trip through JSON so the existing config.Connections decoding picks the right struct for the type.
	serialized, err := json.Marshal(map[string][]map[string]any{secretType: {details}})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to process secret '%s'", name)
	}

	var connections config.Connections
	if err := json.Unmarshal(serialized, &connections); err != nil {
		return nil, errors.Wrapf(err, "failed to parse secret '%s' configuration", name)
	}

	return &connections, nil
}

// newConnectionManager builds a connection manager holding just the given connections.
func newConnectionManager(name string, connections *config.Connections) (config.ConnectionAndDetailsGetter, error) {
	environment := config.Environment{Connections: connections}
	cfg := config.Config{
		Environments:            map[string]config.Environment{"default": environment},
		SelectedEnvironmentName: "default",
		SelectedEnvironment:     &environment,
		DefaultEnvironmentName:  "default",
	}

	manager, errs := connection.NewManagerFromConfig(&cfg)
	if len(errs) > 0 {
		return nil, errors.Wrapf(errs[0], "failed to configure connection '%s'", name)
	}

	return manager, nil
}

// connectionSecretStore is the part a cloud secret store has to provide: read one
// connection secret, and enumerate the connection names it holds. Names are
// connection names; any secret-name prefix is applied and stripped by the store.
type connectionSecretStore interface {
	readConnections(name string) (*config.Connections, error)
	listConnectionNames() ([]string, error)
}

// cachedSecretBackend implements the bruin connection interfaces on top of a
// connectionSecretStore. Each connection's manager is fetched once and cached,
// and concurrent lookups of the same name share a single fetch, so
// GetConnection, GetConnectionDetails and GetConnectionType cost one API call
// per name for the lifetime of the client.
type cachedSecretBackend struct {
	store    connectionSecretStore
	logger   logger.Logger
	mu       sync.RWMutex
	managers map[string]config.ConnectionAndDetailsGetter
	requests singleflight.Group
}

func newCachedSecretBackend(store connectionSecretStore, logger logger.Logger) *cachedSecretBackend {
	return &cachedSecretBackend{
		store:    store,
		logger:   logger,
		managers: make(map[string]config.ConnectionAndDetailsGetter),
	}
}

func (b *cachedSecretBackend) GetConnection(name string) any {
	conn, err := b.ResolveConnection(name)
	if err != nil {
		logLookupFailure(b.logger, err)
		return nil
	}

	return conn
}

// ResolveConnection implements config.ConnectionResolver.
func (b *cachedSecretBackend) ResolveConnection(name string) (any, error) {
	manager, err := b.manager(name)
	if err != nil {
		return nil, err
	}

	return manager.GetConnection(name), nil
}

func (b *cachedSecretBackend) GetConnectionDetails(name string) any {
	manager, err := b.manager(name)
	if err != nil {
		logLookupFailure(b.logger, err)
		return nil
	}

	return manager.GetConnectionDetails(name)
}

func (b *cachedSecretBackend) GetConnectionType(name string) string {
	manager, err := b.manager(name)
	if err != nil {
		return ""
	}

	return manager.GetConnectionType(name)
}

// GetConnectionsConfig implements config.ConnectionsConfigGetter by reading every
// connection the store lists and merging them into one config.Connections.
// Secrets that are not bruin connections are skipped so unrelated secrets in the
// same account or vault don't break the listing; other read errors are returned.
func (b *cachedSecretBackend) GetConnectionsConfig() (*config.Connections, error) {
	names, err := b.store.listConnectionNames()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)

	aggregate := &config.Connections{}
	for _, name := range names {
		connections, err := b.store.readConnections(name)
		if err != nil {
			var formatErr *invalidConnectionSecretError
			switch {
			case isSecretNotFound(err):
				// Deleted between listing and reading.
				b.logger.Debugf("%v", err)
				continue
			case errors.As(err, &formatErr):
				// Debug, not warn: logs go to stdout and would corrupt `connections list -o json`.
				b.logger.Debugf("skipping secret for connection '%s': %v", name, formatErr.err)
				continue
			}
			return nil, err
		}
		if err := aggregate.MergeFrom(connections); err != nil {
			return nil, errors.Wrapf(err, "failed to merge connection '%s'", name)
		}
	}

	return aggregate, nil
}

func (b *cachedSecretBackend) manager(name string) (config.ConnectionAndDetailsGetter, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("cannot read a secret with an empty connection name")
	}

	b.mu.RLock()
	manager, ok := b.managers[name]
	b.mu.RUnlock()
	if ok {
		return manager, nil
	}

	result, err, _ := b.requests.Do(name, func() (any, error) {
		b.mu.RLock()
		manager, ok := b.managers[name]
		b.mu.RUnlock()
		if ok {
			return manager, nil
		}

		connections, err := b.store.readConnections(name)
		if err != nil {
			return nil, err
		}
		manager, err = newConnectionManager(name, connections)
		if err != nil {
			return nil, err
		}

		b.mu.Lock()
		b.managers[name] = manager
		b.mu.Unlock()

		return manager, nil
	})
	if err != nil {
		return nil, err
	}

	manager, ok = result.(config.ConnectionAndDetailsGetter)
	if !ok {
		return nil, errors.Errorf("failed to process secret for connection '%s'", name)
	}

	return manager, nil
}

// invalidConnectionSecretError marks a secret that exists but doesn't hold a bruin
// connection, so listing can skip it.
type invalidConnectionSecretError struct{ err error }

func (e *invalidConnectionSecretError) Error() string { return e.err.Error() }
func (e *invalidConnectionSecretError) Unwrap() error { return e.err }

// readConnectionSecret parses payload and tags format errors as invalidConnectionSecretError.
func readConnectionSecret(name string, payload *string) (*config.Connections, error) {
	if payload == nil {
		return nil, &invalidConnectionSecretError{errors.Errorf("secret '%s' has no string value", name)}
	}
	connections, err := parseConnectionSecret(name, []byte(*payload))
	if err != nil {
		return nil, &invalidConnectionSecretError{err}
	}

	return connections, nil
}
