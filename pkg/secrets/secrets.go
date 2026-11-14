package secrets

import "github.com/bruin-data/bruin/pkg/config"

// Every backend resolves a connection by name and can say why a lookup failed.
// GetConnection collapses that failure into a nil, so callers that need the
// reason reach for ResolveConnection instead. Backends that can enumerate their
// connections also back `bruin connections list`.
var (
	_ config.ConnectionAndDetailsGetter = (*Client)(nil)
	_ config.ConnectionAndDetailsGetter = (*DopplerClient)(nil)
	_ config.ConnectionAndDetailsGetter = (*AWSSecretsManagerClient)(nil)
	_ config.ConnectionAndDetailsGetter = (*AzureKeyVaultClient)(nil)

	_ config.ConnectionResolver = (*Client)(nil)
	_ config.ConnectionResolver = (*DopplerClient)(nil)
	_ config.ConnectionResolver = (*AWSSecretsManagerClient)(nil)
	_ config.ConnectionResolver = (*AzureKeyVaultClient)(nil)

	_ config.ConnectionsConfigGetter = (*Client)(nil)
	_ config.ConnectionsConfigGetter = (*AWSSecretsManagerClient)(nil)
	_ config.ConnectionsConfigGetter = (*AzureKeyVaultClient)(nil)
)
