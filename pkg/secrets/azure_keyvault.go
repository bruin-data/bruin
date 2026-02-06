package secrets

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
)

// azureKeyVaultSecretsClient defines the interface for Azure Key Vault operations.
type azureKeyVaultSecretsClient interface {
	GetSecret(ctx context.Context, name string, version string, options *azsecrets.GetSecretOptions) (azsecrets.GetSecretResponse, error)
}

// AzureKeyVaultClient manages secrets from Azure Key Vault.
type AzureKeyVaultClient struct {
	client                  azureKeyVaultSecretsClient
	logger                  logger.Logger
	cacheMu                 sync.RWMutex
	cacheConnections        map[string]any
	cacheConnectionsDetails map[string]any
}

// NewAzureKeyVaultClientFromEnv creates a new Azure Key Vault client from environment variables.
func NewAzureKeyVaultClientFromEnv(logger logger.Logger) (*AzureKeyVaultClient, error) {
	vaultURL := os.Getenv("BRUIN_AZURE_KEYVAULT_URL")
	if vaultURL == "" {
		return nil, errors.New("BRUIN_AZURE_KEYVAULT_URL env variable not set")
	}

	authMethod := os.Getenv("BRUIN_AZURE_AUTH_METHOD")
	if authMethod == "" {
		authMethod = "default"
	}

	switch authMethod {
	case "client_credentials":
		tenantID := os.Getenv("BRUIN_AZURE_TENANT_ID")
		if tenantID == "" {
			return nil, errors.New("BRUIN_AZURE_TENANT_ID env variable not set for client_credentials auth")
		}
		clientID := os.Getenv("BRUIN_AZURE_CLIENT_ID")
		if clientID == "" {
			return nil, errors.New("BRUIN_AZURE_CLIENT_ID env variable not set for client_credentials auth")
		}
		clientSecret := os.Getenv("BRUIN_AZURE_CLIENT_SECRET")
		if clientSecret == "" {
			return nil, errors.New("BRUIN_AZURE_CLIENT_SECRET env variable not set for client_credentials auth")
		}
		return NewAzureKeyVaultClient(logger, vaultURL, tenantID, clientID, clientSecret)

	case "managed_identity":
		clientID := os.Getenv("BRUIN_AZURE_CLIENT_ID") // Optional for user-assigned identity
		return NewAzureKeyVaultClientWithManagedIdentity(logger, vaultURL, clientID)

	case "cli":
		return NewAzureKeyVaultClientWithCLI(logger, vaultURL)

	case "default":
		return NewAzureKeyVaultClientWithDefaultCredential(logger, vaultURL)

	default:
		return nil, errors.Errorf("unsupported Azure auth method: %s", authMethod)
	}
}

// NewAzureKeyVaultClient creates a new Azure Key Vault client with client credentials.
func NewAzureKeyVaultClient(logger logger.Logger, vaultURL, tenantID, clientID, clientSecret string) (*AzureKeyVaultClient, error) {
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}
	if tenantID == "" {
		return nil, errors.New("tenant ID required for client credentials authentication")
	}
	if clientID == "" {
		return nil, errors.New("client ID required for client credentials authentication")
	}
	if clientSecret == "" {
		return nil, errors.New("client secret required for client credentials authentication")
	}

	cred, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure client secret credential")
	}

	return newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
}

// NewAzureKeyVaultClientWithManagedIdentity creates client using managed identity.
func NewAzureKeyVaultClientWithManagedIdentity(logger logger.Logger, vaultURL, clientID string) (*AzureKeyVaultClient, error) {
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}

	var opts *azidentity.ManagedIdentityCredentialOptions
	if clientID != "" {
		opts = &azidentity.ManagedIdentityCredentialOptions{
			ID: azidentity.ClientID(clientID),
		}
	}

	cred, err := azidentity.NewManagedIdentityCredential(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure managed identity credential")
	}

	return newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
}

// NewAzureKeyVaultClientWithCLI creates client using Azure CLI credentials.
func NewAzureKeyVaultClientWithCLI(logger logger.Logger, vaultURL string) (*AzureKeyVaultClient, error) {
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}

	cred, err := azidentity.NewAzureCLICredential(nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure CLI credential")
	}

	return newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
}

// NewAzureKeyVaultClientWithDefaultCredential creates client using DefaultAzureCredential.
func NewAzureKeyVaultClientWithDefaultCredential(logger logger.Logger, vaultURL string) (*AzureKeyVaultClient, error) {
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure default credential")
	}

	return newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
}

func validateVaultURL(vaultURL string) error {
	if vaultURL == "" {
		return errors.New("empty Azure Key Vault URL provided")
	}
	parsed, err := url.Parse(vaultURL)
	if err != nil || parsed.Scheme != "https" || !strings.HasSuffix(parsed.Host, ".vault.azure.net") {
		return errors.New("invalid Azure Key Vault URL: must be https://<name>.vault.azure.net")
	}
	return nil
}

func newAzureKeyVaultClientWithCredential(logger logger.Logger, vaultURL string, cred azcore.TokenCredential) (*AzureKeyVaultClient, error) {
	client, err := azsecrets.NewClient(vaultURL, cred, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure Key Vault secrets client")
	}

	return &AzureKeyVaultClient{
		client:                  client,
		logger:                  logger,
		cacheConnections:        make(map[string]any),
		cacheConnectionsDetails: make(map[string]any),
	}, nil
}

// GetConnection retrieves a connection by name from Azure Key Vault.
func (c *AzureKeyVaultClient) GetConnection(name string) any {
	c.cacheMu.RLock()
	if conn, ok := c.cacheConnections[name]; ok {
		c.cacheMu.RUnlock()
		return conn
	}
	c.cacheMu.RUnlock()

	manager, err := c.getAzureKeyVaultManager(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	conn := manager.GetConnection(name)

	c.cacheMu.Lock()
	c.cacheConnections[name] = conn
	c.cacheMu.Unlock()

	return conn
}

// GetConnectionDetails retrieves connection details by name from Azure Key Vault.
func (c *AzureKeyVaultClient) GetConnectionDetails(name string) any {
	c.cacheMu.RLock()
	if deets, ok := c.cacheConnectionsDetails[name]; ok {
		c.cacheMu.RUnlock()
		return deets
	}
	c.cacheMu.RUnlock()

	manager, err := c.getAzureKeyVaultManager(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	deets := manager.GetConnectionDetails(name)

	c.cacheMu.Lock()
	c.cacheConnectionsDetails[name] = deets
	c.cacheMu.Unlock()

	return deets
}

func (c *AzureKeyVaultClient) getAzureKeyVaultManager(name string) (config.ConnectionAndDetailsGetter, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()

	// Empty string for version gets the latest version
	result, err := c.client.GetSecret(ctx, name, "", nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read secret '%s' from Azure Key Vault", name)
	}

	if result.Value == nil {
		return nil, errors.Errorf("secret '%s' has no value", name)
	}

	var secretData map[string]any
	if err := json.Unmarshal([]byte(*result.Value), &secretData); err != nil {
		return nil, errors.Wrap(err, "failed to parse secret as JSON")
	}

	detailsRaw, okDetails := secretData["details"]
	secretType, okType := secretData["type"].(string)

	details, detailsIsMap := detailsRaw.(map[string]any)
	if !okDetails || !detailsIsMap || !okType || secretType == "" {
		return nil, errors.Errorf("secret '%s' must contain both 'type' (non-empty string) and 'details' (object)", name)
	}

	details["name"] = name

	connectionsMap := map[string][]map[string]any{
		secretType: {
			details,
		},
	}

	serialized, err := json.Marshal(connectionsMap)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to process secret '%s'", name)
	}

	var connections config.Connections

	if err := json.Unmarshal(serialized, &connections); err != nil {
		return nil, errors.Wrapf(err, "failed to parse secret '%s' configuration", name)
	}

	environment := config.Environment{
		Connections: &connections,
	}

	cfg := config.Config{
		Environments: map[string]config.Environment{
			"default": environment,
		},
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
