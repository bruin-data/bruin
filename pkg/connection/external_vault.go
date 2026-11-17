package connection

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/snowflake"
)

// externalVaultManager builds the connections that read from an external vault the first time they are asked
// for, so a command that never opens one never contacts the vault.
type externalVaultManager struct {
	context func() context.Context
	cm      *config.Config
	base    config.ConnectionAndDetailsGetter
	types   map[string]string

	mu       sync.Mutex
	resolved map[string]*externalVaultResult
}

type externalVaultResult struct {
	manager  config.ConnectionAndDetailsGetter
	err      error
	reported bool
}

func newExternalVaultManager(ctx context.Context, cm *config.Config) (config.ConnectionAndDetailsGetter, []error) {
	base, errs := NewManagerFromConfigWithContext(ctx, cm.WithoutExternalVaultConnections())
	return &externalVaultManager{
		context:  func() context.Context { return ctx },
		cm:       cm,
		base:     base,
		types:    cm.SelectedEnvironment.Connections.ConnectionsSummaryList(),
		resolved: map[string]*externalVaultResult{},
	}, errs
}

// resolve keeps failures too: within one command, asking the vault again would only repeat the prompt and the error.
// It reads one item at a time on purpose, so the user isn't shown several 1Password prompts at once.
func (m *externalVaultManager) resolve(name string) *externalVaultResult {
	m.mu.Lock()
	defer m.mu.Unlock()

	if result, ok := m.resolved[name]; ok {
		return result
	}

	result := &externalVaultResult{}
	single, err := m.cm.ExternalVaultConnection(m.context(), name)
	if err == nil {
		// File settings were expanded when the config loaded; values from the vault are used exactly as stored.
		manager, errs := buildManager(m.context(), single)
		if len(errs) > 0 {
			err = fmt.Errorf("failed to configure connection '%s': %w", name, errs[0])
		} else {
			result.manager = manager
		}
	}
	result.err = err
	m.resolved[name] = result
	return result
}

// resolveOrReport is for the getters that can only return nil, which callers report as a missing connection.
func (m *externalVaultManager) resolveOrReport(name string) config.ConnectionAndDetailsGetter {
	result := m.resolve(name)
	if result.err == nil {
		return result.manager
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if !result.reported {
		result.reported = true
		fmt.Fprintf(os.Stderr, "Warning: %v\n", result.err)
	}
	return nil
}

func (m *externalVaultManager) ResolveConnection(name string) (any, error) {
	if !m.cm.UsesExternalVault(name) {
		if resolver, ok := m.base.(config.ConnectionResolver); ok {
			return resolver.ResolveConnection(name)
		}
		return m.base.GetConnection(name), nil
	}

	result := m.resolve(name)
	if result.err != nil {
		return nil, result.err
	}
	return result.manager.GetConnection(name), nil
}

func (m *externalVaultManager) GetConnection(name string) any {
	if !m.cm.UsesExternalVault(name) {
		return m.base.GetConnection(name)
	}
	if manager := m.resolveOrReport(name); manager != nil {
		return manager.GetConnection(name)
	}
	return nil
}

func (m *externalVaultManager) GetConnectionDetails(name string) any {
	if !m.cm.UsesExternalVault(name) {
		return m.base.GetConnectionDetails(name)
	}
	if manager := m.resolveOrReport(name); manager != nil {
		return manager.GetConnectionDetails(name)
	}
	return nil
}

func (m *externalVaultManager) GetSfConnectionWithWarehouse(name, warehouse string) (snowflake.SfClient, error) {
	manager := m.base
	if m.cm.UsesExternalVault(name) {
		result := m.resolve(name)
		if result.err != nil {
			return nil, result.err
		}
		manager = result.manager
	}

	getter, ok := manager.(interface {
		GetSfConnectionWithWarehouse(name, warehouse string) (snowflake.SfClient, error)
	})
	if !ok {
		return nil, fmt.Errorf("connection '%s' cannot use another Snowflake warehouse", name)
	}
	return getter.GetSfConnectionWithWarehouse(name, warehouse)
}

// GetConnectionType answers from the config file, so asking for a type never contacts the vault.
func (m *externalVaultManager) GetConnectionType(name string) string {
	if m.cm.UsesExternalVault(name) {
		return m.types[name]
	}
	return m.base.GetConnectionType(name)
}
