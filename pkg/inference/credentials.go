package inference

import (
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// resolveGroups resolves credentials once, before reading input or making requests.
// Provider type checking prevents sending a different provider's key to an endpoint.
func (o *Operator) resolveGroups(pipe *pipeline.Pipeline, cfg *assetConfig) ([]requestGroup, error) {
	var groups []requestGroup
	for _, group := range cfg.groups {
		group.connection = pipe.GetInferenceConnectionName(group.provider, group.connection)
		merged := false
		for i := range groups {
			if groups[i].provider == group.provider && groups[i].model == group.model && groups[i].connection == group.connection {
				groups[i].columns = append(groups[i].columns, group.columns...)
				merged = true
				break
			}
		}
		if merged {
			continue
		}
		var connection any
		if resolver, ok := o.conn.(config.ConnectionResolver); ok {
			var err error
			connection, err = resolver.ResolveConnection(group.connection)
			if err != nil {
				return nil, fmt.Errorf("could not resolve inference connection %q: %w", group.connection, err)
			}
		} else {
			connection = o.conn.GetConnection(group.connection)
		}
		if connection == nil {
			return nil, fmt.Errorf("inference connection %q does not exist", group.connection)
		}
		if o.conn.GetConnectionType(group.connection) != group.provider {
			return nil, fmt.Errorf("inference connection %q must have type %q", group.connection, group.provider)
		}
		credential, ok := o.conn.GetConnectionDetails(group.connection).(interface{ GetAPIKey() string })
		if !ok || strings.TrimSpace(credential.GetAPIKey()) == "" {
			return nil, fmt.Errorf("inference connection %q requires an API key", group.connection)
		}
		group.apiKey = credential.GetAPIKey()
		groups = append(groups, group)
	}
	return groups, nil
}
