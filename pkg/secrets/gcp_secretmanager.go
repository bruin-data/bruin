package secrets

import (
	"context"
	"encoding/json"
	"hash/crc32"
	"os"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/auth/credentials/impersonate"
	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	defaultGCPRequestTimeout = 30 * time.Second
	// Listing reads every page with one context, so it gets more time than a single read.
	defaultGCPListTimeout    = 2 * time.Minute
	defaultGCPProjectTimeout = 10 * time.Second
	defaultGCPSecretVersion  = "latest"
	gcpCloudPlatformScope    = "https://www.googleapis.com/auth/cloud-platform"
)

var (
	// gcpSecretID matches the IDs Secret Manager accepts for secrets.
	gcpSecretID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,255}$`)
	// gcpNameChars matches a secret ID prefix, and a version number, "latest" or a version alias.
	gcpNameChars = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

// gcpCredentialTypes are the credential file types Application Default Credentials accepts.
var gcpCredentialTypes = []credentials.CredType{
	credentials.ServiceAccount,
	credentials.AuthorizedUser,
	credentials.ExternalAccount,
	credentials.ExternalAccountAuthorizedUser,
	credentials.ImpersonatedServiceAccount,
	credentials.GDCHServiceAccount,
}

// gcpSecretManagerClient is the part of the Secret Manager API the backend uses.
type gcpSecretManagerClient interface {
	AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest) (*secretmanagerpb.AccessSecretVersionResponse, error)
	ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest) gcpSecretIterator
}

// gcpSecretIterator is implemented by *secretmanager.SecretIterator.
type gcpSecretIterator interface {
	Next() (*secretmanagerpb.Secret, error)
}

// gcpSecretManagerSDKClient adapts *secretmanager.Client to gcpSecretManagerClient.
type gcpSecretManagerSDKClient struct {
	client *secretmanager.Client
}

func (c gcpSecretManagerSDKClient) AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	return c.client.AccessSecretVersion(ctx, req)
}

//nolint:ireturn // returns the SDK iterator behind an interface so tests can fake it.
func (c gcpSecretManagerSDKClient) ListSecrets(ctx context.Context, req *secretmanagerpb.ListSecretsRequest) gcpSecretIterator {
	return c.client.ListSecrets(ctx, req)
}

// GCPSecretManagerClient manages secrets from GCP Secret Manager.
type GCPSecretManagerClient struct {
	*cachedSecretBackend

	client  gcpSecretManagerClient
	project string
	// prefix is prepended to connection names to form secret IDs, e.g. "bruin-prod-".
	prefix string
	// version is the secret version read for every connection: "latest", a number or an alias.
	version         string
	contextProvider func() context.Context
}

// NewGCPSecretManagerClientFromEnv creates a new GCP Secret Manager client from environment variables.
//
// Credentials are resolved in this order:
//  1. BRUIN_GCP_CREDENTIALS_JSON or BRUIN_GCP_CREDENTIALS_FILE, if set. Any credential type
//     Application Default Credentials accepts works, including external_account configs for
//     workload identity federation.
//  2. Otherwise Application Default Credentials: the file GOOGLE_APPLICATION_CREDENTIALS points
//     at, the gcloud application-default credentials file, then the metadata server (GKE
//     Workload Identity, GCE, Cloud Run).
//
// If BRUIN_GCP_IMPERSONATE_SERVICE_ACCOUNT is set, that service account is then impersonated
// using the credentials from step 1 or 2.
func NewGCPSecretManagerClientFromEnv(ctx context.Context, logger logger.Logger) (*GCPSecretManagerClient, error) {
	prefix := os.Getenv("BRUIN_GCP_SECRET_PREFIX")
	if prefix != "" && !gcpNameChars.MatchString(prefix) {
		return nil, errors.Errorf("BRUIN_GCP_SECRET_PREFIX '%s' is not valid: GCP Secret Manager secret IDs may only contain letters, digits, underscores and hyphens", prefix)
	}

	version := strings.TrimSpace(os.Getenv("BRUIN_GCP_SECRET_VERSION"))
	if version == "" {
		version = defaultGCPSecretVersion
	}
	if !gcpNameChars.MatchString(version) {
		return nil, errors.Errorf("BRUIN_GCP_SECRET_VERSION '%s' is not valid: use 'latest', a version number or a version alias", version)
	}

	creds, project, err := loadGCPCredentialsFromEnv(ctx, logger)
	if err != nil {
		return nil, err
	}

	sdkClient, err := secretmanager.NewClient(ctx, option.WithAuthCredentials(creds))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create GCP Secret Manager client")
	}

	client := newGCPSecretManagerClient(logger, gcpSecretManagerSDKClient{client: sdkClient}, project, prefix, version)
	client.contextProvider = func() context.Context { return ctx }
	logger.Debugf("GCP Secret Manager: reading version '%s' of secrets in project '%s'", version, project)
	if prefix != "" {
		logger.Debugf("GCP Secret Manager: reading connections from secrets prefixed with '%s'", prefix)
	}

	return client, nil
}

// loadGCPCredentialsFromEnv resolves the credentials and the project to read secrets from.
func loadGCPCredentialsFromEnv(ctx context.Context, logger logger.Logger) (*auth.Credentials, string, error) {
	creds, err := gcpBaseCredentialsFromEnv(logger)
	if err != nil {
		return nil, "", err
	}

	// Resolved from the base credentials: impersonated credentials don't carry a project.
	project, err := gcpProjectFromEnv(ctx, creds)
	if err != nil {
		return nil, "", err
	}

	if target := strings.TrimSpace(os.Getenv("BRUIN_GCP_IMPERSONATE_SERVICE_ACCOUNT")); target != "" {
		logger.Debugf("GCP Secret Manager: impersonating service account %s", target)
		creds, err = impersonate.NewCredentials(&impersonate.CredentialsOptions{
			TargetPrincipal: target,
			Scopes:          []string{gcpCloudPlatformScope},
			Credentials:     creds,
		})
		if err != nil {
			return nil, "", errors.Wrapf(err, "failed to impersonate GCP service account '%s'", target)
		}
	}

	return creds, project, nil
}

func gcpBaseCredentialsFromEnv(logger logger.Logger) (*auth.Credentials, error) {
	credentialsJSON := os.Getenv("BRUIN_GCP_CREDENTIALS_JSON")
	credentialsFile := os.Getenv("BRUIN_GCP_CREDENTIALS_FILE")
	opts := &credentials.DetectOptions{Scopes: []string{gcpCloudPlatformScope}}

	var source string
	var payload []byte
	switch {
	case credentialsJSON != "" && credentialsFile != "":
		return nil, errors.New("set only one of BRUIN_GCP_CREDENTIALS_JSON and BRUIN_GCP_CREDENTIALS_FILE")
	case credentialsJSON != "":
		source = "BRUIN_GCP_CREDENTIALS_JSON"
		payload = []byte(credentialsJSON)
	case credentialsFile != "":
		source = "BRUIN_GCP_CREDENTIALS_FILE"
		var err error
		payload, err = os.ReadFile(credentialsFile) //nolint:gosec // G703: the operator chooses this path, like GOOGLE_APPLICATION_CREDENTIALS.
		if err != nil {
			return nil, errors.Wrap(err, "failed to read BRUIN_GCP_CREDENTIALS_FILE")
		}
	}

	if payload != nil {
		credType, err := gcpCredentialType(payload)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid GCP credentials in %s", source)
		}
		creds, err := credentials.NewCredentialsFromJSON(credType, payload, opts)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to load GCP credentials from %s", source)
		}
		logger.Debugf("GCP Secret Manager: using %s credentials from %s", credType, source)
		return creds, nil
	}

	creds, err := credentials.DetectDefault(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find GCP credentials: set BRUIN_GCP_CREDENTIALS_FILE or GOOGLE_APPLICATION_CREDENTIALS, "+
			"run 'gcloud auth application-default login', or run on GKE, GCE or Cloud Run with a service account attached")
	}
	logger.Debugf("GCP Secret Manager: using %s", describeGCPDefaultCredentials(creds))

	return creds, nil
}

// gcpCredentialType reads only the "type" field of a credential file, to pick the matching loader.
func gcpCredentialType(payload []byte) (credentials.CredType, error) {
	var file struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &file); err != nil {
		return "", errors.New("credentials are not valid JSON")
	}

	for _, credType := range gcpCredentialTypes {
		if file.Type == string(credType) {
			return credType, nil
		}
	}

	names := make([]string, 0, len(gcpCredentialTypes))
	for _, credType := range gcpCredentialTypes {
		names = append(names, string(credType))
	}
	return "", errors.Errorf("unsupported credential type '%s' (supported: %s)", file.Type, strings.Join(names, ", "))
}

// describeGCPDefaultCredentials says where Application Default Credentials came from, without any values.
func describeGCPDefaultCredentials(creds *auth.Credentials) string {
	payload := creds.JSON()
	if payload == nil {
		return "credentials from the metadata server (GKE Workload Identity, GCE or Cloud Run)"
	}

	credType := "unknown"
	if t, err := gcpCredentialType(payload); err == nil {
		credType = string(t)
	}
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		return credType + " credentials from GOOGLE_APPLICATION_CREDENTIALS"
	}

	return credType + " credentials from the gcloud application default credentials file"
}

// gcpProjectFromEnv picks the project holding the secrets: BRUIN_GCP_PROJECT, then
// GOOGLE_CLOUD_PROJECT, then the project the credentials report (a service account key's
// project_id, or the metadata server's project), then the credentials' quota project.
func gcpProjectFromEnv(ctx context.Context, creds *auth.Credentials) (string, error) {
	for _, key := range []string{"BRUIN_GCP_PROJECT", "GOOGLE_CLOUD_PROJECT"} {
		if project := strings.TrimSpace(os.Getenv(key)); project != "" {
			return project, nil
		}
	}

	ctx, cancel := context.WithTimeout(ctx, defaultGCPProjectTimeout)
	defer cancel()

	project, err := creds.ProjectID(ctx)
	if err != nil {
		return "", errors.Wrap(err, "failed to read the GCP project from the credentials; set BRUIN_GCP_PROJECT")
	}
	if project == "" {
		project, err = creds.QuotaProjectID(ctx)
		if err != nil {
			return "", errors.Wrap(err, "failed to read the GCP quota project from the credentials; set BRUIN_GCP_PROJECT")
		}
	}
	if project == "" {
		return "", errors.New("no GCP project configured for GCP Secret Manager: set BRUIN_GCP_PROJECT, or use credentials that " +
			"name a project (a service account key's project_id or a quota_project_id)")
	}

	return project, nil
}

func newGCPSecretManagerClient(logger logger.Logger, client gcpSecretManagerClient, project, prefix, version string) *GCPSecretManagerClient {
	if version == "" {
		version = defaultGCPSecretVersion
	}
	c := &GCPSecretManagerClient{
		client:  client,
		project: project,
		prefix:  prefix,
		version: version,
	}
	c.cachedSecretBackend = newCachedSecretBackend(c, logger)

	return c
}

// baseContext is the context requests derive from, so cancelling the command cancels in-flight reads.
func (c *GCPSecretManagerClient) baseContext() context.Context {
	if c.contextProvider != nil {
		if ctx := c.contextProvider(); ctx != nil {
			return ctx
		}
	}

	return context.Background()
}

func (c *GCPSecretManagerClient) readConnections(name string) (*config.Connections, error) {
	secretID := c.prefix + name
	if !gcpSecretID.MatchString(secretID) {
		return nil, errors.Errorf(
			"connection '%s' can't be read from GCP Secret Manager: secret '%s' is not a valid secret ID, which may only contain "+
				"letters, digits, underscores and hyphens (a-z, A-Z, 0-9, _, -) and be at most 255 characters; rename the connection to match",
			name, secretID,
		)
	}

	ctx, cancel := context.WithTimeout(c.baseContext(), defaultGCPRequestTimeout)
	defer cancel()

	versionName := "projects/" + c.project + "/secrets/" + secretID + "/versions/" + c.version
	result, err := c.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: versionName})
	if err != nil {
		return nil, c.accessError(secretID, err)
	}

	payload := result.GetPayload()
	if payload == nil {
		return readConnectionSecret(name, nil)
	}
	if payload.DataCrc32C != nil && int64(crc32.Checksum(payload.GetData(), crc32.MakeTable(crc32.Castagnoli))) != payload.GetDataCrc32C() {
		return nil, &config.SecretsBackendUnavailableError{Err: errors.Errorf("secret '%s' read from GCP Secret Manager failed its checksum; retry the read", secretID)}
	}
	value := string(payload.GetData())

	return readConnectionSecret(name, &value)
}

func (c *GCPSecretManagerClient) accessError(secretID string, err error) error {
	where := "secret '" + secretID + "' (version '" + c.version + "') in GCP project '" + c.project + "'"
	if disabledErr := c.serviceDisabledError(err); disabledErr != nil {
		return &config.SecretsBackendUnavailableError{Err: disabledErr}
	}
	switch status.Code(err) {
	case codes.NotFound:
		return newSecretNotFoundError("%s not found in GCP Secret Manager", where)
	case codes.PermissionDenied:
		return &config.SecretsBackendUnavailableError{Err: errors.Wrapf(err,
			"permission denied reading %s: the identity needs the secretmanager.versions.access permission on the secret, "+
				"e.g. from roles/secretmanager.secretAccessor (if the role is granted on single secrets, a secret that doesn't exist is reported this way too)",
			where)}
	case codes.FailedPrecondition:
		// The version exists but is disabled or destroyed.
		return &invalidConnectionSecretError{errors.Wrapf(err, "%s can't be read from GCP Secret Manager", where)}
	default:
		return &config.SecretsBackendUnavailableError{Err: errors.Wrapf(err, "failed to read %s from GCP Secret Manager", where)}
	}
}

// serviceDisabledError returns a clear error when err says the Secret Manager API is disabled, and nil
// otherwise. GCP reports a disabled API as PermissionDenied, which would otherwise read as a missing
// IAM role. The project to enable it in is the request's consumer project, which for user credentials
// is the quota project rather than the project holding the secrets.
func (c *GCPSecretManagerClient) serviceDisabledError(err error) error {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.PermissionDenied {
		return nil
	}
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetReason() != "SERVICE_DISABLED" {
			continue
		}
		project := strings.TrimPrefix(info.GetMetadata()["consumer"], "projects/")
		if project == "" {
			project = c.project
		}
		return errors.Errorf("the Secret Manager API is not enabled in GCP project '%s'; enable it with "+
			"`gcloud services enable secretmanager.googleapis.com --project %s` and retry after a few minutes", project, project)
	}
	return nil
}

// listConnectionNames lists the secrets under the configured prefix and returns their IDs with the prefix removed.
func (c *GCPSecretManagerClient) listConnectionNames() ([]string, error) {
	ctx, cancel := context.WithTimeout(c.baseContext(), defaultGCPListTimeout)
	defer cancel()

	// The prefix is matched here rather than with a ListSecrets filter: the filter's name match
	// isn't a prefix match, so it would only narrow the listing anyway.
	var names []string
	it := c.client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: "projects/" + c.project})
	for {
		secret, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			if disabledErr := c.serviceDisabledError(err); disabledErr != nil {
				return nil, disabledErr
			}
			if status.Code(err) == codes.PermissionDenied {
				return nil, errors.Wrapf(err, "permission denied listing secrets in GCP project '%s': `connections list` needs the "+
					"secretmanager.secrets.list permission on the project, e.g. from roles/secretmanager.viewer", c.project)
			}
			return nil, errors.Wrapf(err, "failed to list secrets in GCP project '%s'", c.project)
		}

		// Names are projects/<project number>/secrets/<secret ID>.
		secretID := secret.GetName()[strings.LastIndex(secret.GetName(), "/")+1:]
		if !strings.HasPrefix(secretID, c.prefix) || secretID == c.prefix {
			continue
		}
		names = append(names, strings.TrimPrefix(secretID, c.prefix))
	}

	return names, nil
}
