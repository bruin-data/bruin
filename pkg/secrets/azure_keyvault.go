package secrets

import (
	"context"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
)

const defaultAzureRequestTimeout = 30 * time.Second

// azureKeyVaultHostSuffixes are the Key Vault DNS suffixes of the public and sovereign Azure clouds.
var azureKeyVaultHostSuffixes = []string{
	".vault.azure.net",          // Azure public cloud
	".vault.azure.cn",           // Azure China (21Vianet)
	".vault.usgovcloudapi.net",  // Azure US Government
	".vault.microsoftazure.de",  // Azure Germany
}

var (
	// azureKeyVaultSecretName matches the names Key Vault accepts for secrets.
	azureKeyVaultSecretName = regexp.MustCompile(`^[0-9a-zA-Z-]{1,127}$`)
	azureKeyVaultNameChars  = regexp.MustCompile(`^[0-9a-zA-Z-]+$`)
)

// azureKeyVaultSecretsClient defines the interface for Azure Key Vault operations.
type azureKeyVaultSecretsClient interface {
	GetSecret(ctx context.Context, name string, version string, options *azsecrets.GetSecretOptions) (azsecrets.GetSecretResponse, error)
	NewListSecretPropertiesPager(options *azsecrets.ListSecretPropertiesOptions) *runtime.Pager[azsecrets.ListSecretPropertiesResponse]
}

// AzureKeyVaultClient manages secrets from Azure Key Vault.
type AzureKeyVaultClient struct {
	*cachedSecretBackend

	client azureKeyVaultSecretsClient
	// prefix is prepended to connection names to form secret names, e.g. "bruin-prod-".
	prefix string
}

// NewAzureKeyVaultClientFromEnv creates a new Azure Key Vault client from environment variables.
//
// BRUIN_AZURE_AUTH_METHOD picks the credential: client_credentials, managed_identity,
// workload_identity, cli, or default (DefaultAzureCredential, which itself tries
// environment credentials, AKS workload identity, managed identity and the Azure CLI).
func NewAzureKeyVaultClientFromEnv(logger logger.Logger) (*AzureKeyVaultClient, error) {
	vaultURL := os.Getenv("BRUIN_AZURE_KEYVAULT_URL")
	if vaultURL == "" {
		return nil, errors.New("BRUIN_AZURE_KEYVAULT_URL env variable not set")
	}
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}

	prefix := os.Getenv("BRUIN_AZURE_SECRET_PREFIX")
	if prefix != "" && !azureKeyVaultNameChars.MatchString(prefix) {
		return nil, errors.Errorf("BRUIN_AZURE_SECRET_PREFIX '%s' is not valid: Azure Key Vault secret names may only contain letters, digits and hyphens", prefix)
	}

	authMethod := os.Getenv("BRUIN_AZURE_AUTH_METHOD")
	if authMethod == "" {
		authMethod = "default"
	}

	cred, err := azureCredentialFromEnv(authMethod, azureClientOptionsForVault(vaultURL))
	if err != nil {
		return nil, err
	}
	logger.Debugf("Azure Key Vault: authenticating with the '%s' auth method", authMethod)

	client, err := newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
	if err != nil {
		return nil, err
	}
	client.prefix = prefix
	if prefix != "" {
		logger.Debugf("Azure Key Vault: reading connections from secrets prefixed with '%s'", prefix)
	}

	return client, nil
}

func azureCredentialFromEnv(authMethod string, clientOptions azcore.ClientOptions) (azcore.TokenCredential, error) {
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
		return newAzureClientSecretCredential(tenantID, clientID, clientSecret, clientOptions)

	case "managed_identity":
		// Optional, selects a user-assigned identity.
		return newAzureManagedIdentityCredential(os.Getenv("BRUIN_AZURE_CLIENT_ID"), clientOptions)

	case "workload_identity":
		// Optional overrides; the AKS webhook injects AZURE_CLIENT_ID and AZURE_TENANT_ID.
		return newAzureWorkloadIdentityCredential(os.Getenv("BRUIN_AZURE_CLIENT_ID"), os.Getenv("BRUIN_AZURE_TENANT_ID"), clientOptions)

	case "cli":
		return newAzureCLICredential()

	case "default":
		return newAzureDefaultCredential(clientOptions)

	default:
		return nil, errors.Errorf("unsupported Azure auth method: %s (supported: client_credentials, managed_identity, workload_identity, cli, default)", authMethod)
	}
}

// NewAzureKeyVaultClient creates a new Azure Key Vault client with client credentials.
func NewAzureKeyVaultClient(logger logger.Logger, vaultURL, tenantID, clientID, clientSecret string) (*AzureKeyVaultClient, error) {
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}

	cred, err := newAzureClientSecretCredential(tenantID, clientID, clientSecret, azureClientOptionsForVault(vaultURL))
	if err != nil {
		return nil, err
	}

	return newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
}

// NewAzureKeyVaultClientWithManagedIdentity creates client using managed identity.
func NewAzureKeyVaultClientWithManagedIdentity(logger logger.Logger, vaultURL, clientID string) (*AzureKeyVaultClient, error) {
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}

	cred, err := newAzureManagedIdentityCredential(clientID, azureClientOptionsForVault(vaultURL))
	if err != nil {
		return nil, err
	}

	return newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
}

// NewAzureKeyVaultClientWithWorkloadIdentity creates client using AKS workload identity.
// Empty clientID and tenantID fall back to AZURE_CLIENT_ID and AZURE_TENANT_ID.
func NewAzureKeyVaultClientWithWorkloadIdentity(logger logger.Logger, vaultURL, clientID, tenantID string) (*AzureKeyVaultClient, error) {
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}

	cred, err := newAzureWorkloadIdentityCredential(clientID, tenantID, azureClientOptionsForVault(vaultURL))
	if err != nil {
		return nil, err
	}

	return newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
}

// NewAzureKeyVaultClientWithCLI creates client using Azure CLI credentials.
func NewAzureKeyVaultClientWithCLI(logger logger.Logger, vaultURL string) (*AzureKeyVaultClient, error) {
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}

	cred, err := newAzureCLICredential()
	if err != nil {
		return nil, err
	}

	return newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
}

// NewAzureKeyVaultClientWithDefaultCredential creates client using DefaultAzureCredential.
func NewAzureKeyVaultClientWithDefaultCredential(logger logger.Logger, vaultURL string) (*AzureKeyVaultClient, error) {
	if err := validateVaultURL(vaultURL); err != nil {
		return nil, err
	}

	cred, err := newAzureDefaultCredential(azureClientOptionsForVault(vaultURL))
	if err != nil {
		return nil, err
	}

	return newAzureKeyVaultClientWithCredential(logger, vaultURL, cred)
}

func newAzureClientSecretCredential(tenantID, clientID, clientSecret string, clientOptions azcore.ClientOptions) (azcore.TokenCredential, error) {
	if tenantID == "" {
		return nil, errors.New("tenant ID required for client credentials authentication")
	}
	if clientID == "" {
		return nil, errors.New("client ID required for client credentials authentication")
	}
	if clientSecret == "" {
		return nil, errors.New("client secret required for client credentials authentication")
	}

	cred, err := azidentity.NewClientSecretCredential(tenantID, clientID, clientSecret, &azidentity.ClientSecretCredentialOptions{ClientOptions: clientOptions})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure client secret credential")
	}

	return cred, nil
}

func newAzureManagedIdentityCredential(clientID string, clientOptions azcore.ClientOptions) (azcore.TokenCredential, error) {
	opts := &azidentity.ManagedIdentityCredentialOptions{ClientOptions: clientOptions}
	if clientID != "" {
		opts.ID = azidentity.ClientID(clientID)
	}

	cred, err := azidentity.NewManagedIdentityCredential(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure managed identity credential")
	}

	return cred, nil
}

func newAzureWorkloadIdentityCredential(clientID, tenantID string, clientOptions azcore.ClientOptions) (azcore.TokenCredential, error) {
	cred, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
		ClientOptions: clientOptions,
		ClientID:      clientID,
		TenantID:      tenantID,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure workload identity credential; it needs AZURE_CLIENT_ID, AZURE_TENANT_ID and "+
			"AZURE_FEDERATED_TOKEN_FILE, which the AKS workload identity webhook injects into pods labelled azure.workload.identity/use: \"true\"")
	}

	return cred, nil
}

func newAzureCLICredential() (azcore.TokenCredential, error) {
	cred, err := azidentity.NewAzureCLICredential(nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure CLI credential")
	}

	return cred, nil
}

func newAzureDefaultCredential(clientOptions azcore.ClientOptions) (azcore.TokenCredential, error) {
	cred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{ClientOptions: clientOptions})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure default credential")
	}

	return cred, nil
}

// azureClientOptionsForVault points Microsoft Entra authentication at the cloud the
// vault lives in, so a sovereign-cloud vault works without extra settings.
// AZURE_AUTHORITY_HOST, when set, takes precedence.
func azureClientOptionsForVault(vaultURL string) azcore.ClientOptions {
	if os.Getenv("AZURE_AUTHORITY_HOST") != "" {
		return azcore.ClientOptions{}
	}

	parsed, err := url.Parse(vaultURL)
	if err != nil {
		return azcore.ClientOptions{}
	}

	host := strings.ToLower(parsed.Hostname())
	switch {
	case strings.HasSuffix(host, ".vault.azure.cn"):
		return azcore.ClientOptions{Cloud: cloud.AzureChina}
	case strings.HasSuffix(host, ".vault.usgovcloudapi.net"):
		return azcore.ClientOptions{Cloud: cloud.AzureGovernment}
	default:
		return azcore.ClientOptions{}
	}
}

func validateVaultURL(vaultURL string) error {
	if vaultURL == "" {
		return errors.New("empty Azure Key Vault URL provided")
	}

	parsed, err := url.Parse(vaultURL)
	if err == nil && parsed.Scheme == "https" {
		host := strings.ToLower(parsed.Hostname())
		for _, suffix := range azureKeyVaultHostSuffixes {
			if strings.HasSuffix(host, suffix) {
				return nil
			}
		}
	}

	return errors.New("invalid Azure Key Vault URL: must be https://<name>.vault.azure.net, or for sovereign clouds " +
		"https://<name>.vault.azure.cn, https://<name>.vault.usgovcloudapi.net or https://<name>.vault.microsoftazure.de")
}

func newAzureKeyVaultClientWithCredential(logger logger.Logger, vaultURL string, cred azcore.TokenCredential) (*AzureKeyVaultClient, error) {
	client, err := azsecrets.NewClient(vaultURL, cred, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Azure Key Vault secrets client")
	}

	return newAzureKeyVaultClient(logger, client, ""), nil
}

func newAzureKeyVaultClient(logger logger.Logger, client azureKeyVaultSecretsClient, prefix string) *AzureKeyVaultClient {
	c := &AzureKeyVaultClient{
		client: client,
		prefix: prefix,
	}
	c.cachedSecretBackend = newCachedSecretBackend(c, logger)

	return c
}

func (c *AzureKeyVaultClient) readConnections(name string) (*config.Connections, error) {
	secretName := c.prefix + name
	if !azureKeyVaultSecretName.MatchString(secretName) {
		return nil, errors.Errorf(
			"connection '%s' can't be read from Azure Key Vault: secret '%s' is not a valid Key Vault secret name, which may only contain "+
				"letters, digits and hyphens (0-9, a-z, A-Z, -) and be at most 127 characters; rename the connection to match",
			name, secretName,
		)
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultAzureRequestTimeout)
	defer cancel()

	// Empty string for version gets the latest version
	result, err := c.client.GetSecret(ctx, secretName, "", nil)
	if err != nil {
		var respErr *azcore.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return nil, newSecretNotFoundError("secret '%s' not found in Azure Key Vault", secretName)
		}
		return nil, errors.Wrapf(err, "failed to read secret '%s' from Azure Key Vault", secretName)
	}

	return readConnectionSecret(name, result.Value)
}

// listConnectionNames lists the enabled secrets under the configured prefix and returns their names with the prefix removed.
func (c *AzureKeyVaultClient) listConnectionNames() ([]string, error) {
	var names []string
	pager := c.client.NewListSecretPropertiesPager(nil)
	for pager.More() {
		ctx, cancel := context.WithTimeout(context.Background(), defaultAzureRequestTimeout)
		page, err := pager.NextPage(ctx)
		cancel()
		if err != nil {
			return nil, errors.Wrap(err, "failed to list secrets in Azure Key Vault")
		}

		for _, secret := range page.Value {
			if secret == nil || secret.ID == nil {
				continue
			}
			// Disabled secrets can't be read, and managed secrets back Key Vault certificates.
			if secret.Attributes != nil && secret.Attributes.Enabled != nil && !*secret.Attributes.Enabled {
				continue
			}
			if secret.Managed != nil && *secret.Managed {
				continue
			}

			// Key Vault secret names are case-insensitive, so the prefix is too.
			secretName := secret.ID.Name()
			if len(secretName) <= len(c.prefix) || !strings.EqualFold(secretName[:len(c.prefix)], c.prefix) {
				continue
			}
			names = append(names, secretName[len(c.prefix):])
		}
	}

	return names, nil
}
