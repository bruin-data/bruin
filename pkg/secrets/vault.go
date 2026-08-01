package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
	"github.com/pkg/errors"
	"golang.org/x/sync/singleflight"
)

const (
	defaultVaultRequestTimeout = 30 * time.Second
	defaultVaultRetryWaitMin   = 200 * time.Millisecond
	defaultVaultRetryWaitMax   = 2 * time.Second
	defaultVaultRetryMax       = 3
)

type vaultClientConfig struct {
	requestTimeout time.Duration
	retryWaitMin   time.Duration
	retryWaitMax   time.Duration
	retryMax       int
}

func defaultVaultClientConfig() vaultClientConfig {
	return vaultClientConfig{
		requestTimeout: defaultVaultRequestTimeout,
		retryWaitMin:   defaultVaultRetryWaitMin,
		retryWaitMax:   defaultVaultRetryWaitMax,
		retryMax:       defaultVaultRetryMax,
	}
}

func NewVaultClientFromEnv(logger logger.Logger) (*Client, error) {
	return NewVaultClientFromEnvContext(context.Background(), logger)
}

func NewVaultClientFromEnvContext(ctx context.Context, logger logger.Logger) (*Client, error) {
	host := os.Getenv("BRUIN_VAULT_HOST")
	if host == "" {
		return nil, errors.New("BRUIN_VAULT_HOST env variable not set")
	}
	token := os.Getenv("BRUIN_VAULT_TOKEN")
	role := os.Getenv("BRUIN_VAULT_ROLE")
	if token == "" && role == "" {
		return nil, errors.New("BRUIN_VAULT_TOKEN or BRUIN_VAULT_ROLE env variable not set")
	}
	path := os.Getenv("BRUIN_VAULT_PATH")
	if path == "" {
		return nil, errors.New("BRUIN_VAULT_PATH env variable not set")
	}
	mountPath := os.Getenv("BRUIN_VAULT_MOUNT_PATH")
	if mountPath == "" {
		return nil, errors.New("BRUIN_VAULT_MOUNT_PATH env variable not set")
	}
	kubernetesAuthMountPath := os.Getenv("BRUIN_VAULT_K8S_AUTH_MOUNT")
	if kubernetesAuthMountPath == "" {
		kubernetesAuthMountPath = "kubernetes"
	}

	clientConfig, err := vaultClientConfigFromEnv()
	if err != nil {
		return nil, err
	}

	return newVaultClient(ctx, logger, host, token, role, path, mountPath, kubernetesAuthMountPath, clientConfig)
}

func NewVaultClient(logger logger.Logger, host, token, role, path string, mountPath string, kubernetesAuthMountPath string) (*Client, error) {
	return newVaultClient(context.Background(), logger, host, token, role, path, mountPath, kubernetesAuthMountPath, defaultVaultClientConfig())
}

func newVaultClient(ctx context.Context, logger logger.Logger, host, token, role, path string, mountPath string, kubernetesAuthMountPath string, clientConfig vaultClientConfig) (*Client, error) {
	if ctx == nil {
		return nil, errors.New("nil context provided")
	}
	if host == "" {
		return nil, errors.New("empty vault host provided")
	}
	if err := validateVaultAddress(host); err != nil {
		return nil, err
	}
	if path == "" {
		return nil, errors.New("empty vault path provided")
	}
	if mountPath == "" {
		return nil, errors.New("empty vault mountpath provided")
	}
	if token != "" {
		return newVaultClientWithToken(ctx, host, token, mountPath, logger, path, clientConfig)
	}
	if role != "" {
		return newVaultClientWithKubernetesAuth(ctx, host, role, mountPath, kubernetesAuthMountPath, logger, path, clientConfig)
	}

	return nil, errors.New("no vault credentials provided")
}

func vaultClientConfigFromEnv() (vaultClientConfig, error) {
	clientConfig := defaultVaultClientConfig()

	var err error
	clientConfig.requestTimeout, err = durationFromEnv("BRUIN_VAULT_TIMEOUT", clientConfig.requestTimeout)
	if err != nil {
		return vaultClientConfig{}, err
	}
	clientConfig.retryWaitMin, err = durationFromEnv("BRUIN_VAULT_RETRY_WAIT_MIN", clientConfig.retryWaitMin)
	if err != nil {
		return vaultClientConfig{}, err
	}
	clientConfig.retryWaitMax, err = durationFromEnv("BRUIN_VAULT_RETRY_WAIT_MAX", clientConfig.retryWaitMax)
	if err != nil {
		return vaultClientConfig{}, err
	}

	if value := os.Getenv("BRUIN_VAULT_MAX_RETRIES"); value != "" {
		clientConfig.retryMax, err = strconv.Atoi(value)
		if err != nil || clientConfig.retryMax < 0 {
			return vaultClientConfig{}, errors.New("BRUIN_VAULT_MAX_RETRIES must be a non-negative integer")
		}
	}

	if clientConfig.retryWaitMax < clientConfig.retryWaitMin {
		return vaultClientConfig{}, errors.New("BRUIN_VAULT_RETRY_WAIT_MAX must be greater than or equal to BRUIN_VAULT_RETRY_WAIT_MIN")
	}

	return clientConfig, nil
}

func durationFromEnv(name string, defaultValue time.Duration) (time.Duration, error) {
	value := os.Getenv(name)
	if value == "" {
		return defaultValue, nil
	}

	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		return 0, errors.Errorf("%s must be a positive duration", name)
	}

	return duration, nil
}

func validateVaultAddress(address string) error {
	parsed, err := url.Parse(address)
	if err != nil {
		return errors.Wrap(err, "invalid Vault host")
	}
	if parsed.User != nil {
		return errors.New("invalid Vault host: URL credentials are not allowed")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("invalid Vault host: query parameters and fragments are not allowed")
	}

	switch parsed.Scheme {
	case "http", "https":
		if parsed.Host == "" {
			return errors.New("invalid Vault host: HTTP(S) URL must include a host")
		}
	case "unix":
		if strings.TrimPrefix(address, "unix://") == "" {
			return errors.New("invalid Vault host: unix URL must include a socket path")
		}
	default:
		return errors.New("invalid Vault host: URL scheme must be http, https, or unix")
	}

	return nil
}

type kvV2Reader interface {
	KvV2Read(ctx context.Context, path string, options ...vault.RequestOption) (*vault.Response[schema.KvV2ReadResponse], error)
}

type Client struct {
	client                  kvV2Reader
	mountPath               string
	path                    string
	logger                  logger.Logger
	ctx                     context.Context
	requestTimeout          time.Duration
	cacheMu                 sync.RWMutex
	managerRequests         singleflight.Group
	cacheManagers           map[string]config.ConnectionAndDetailsGetter
	cacheConnections        map[string]any
	cacheConnectionsDetails map[string]any
}

func newVaultAPIClient(host string, clientConfig vaultClientConfig) (*vault.Client, error) {
	client, err := vault.New(
		vault.WithAddress(host),
		vault.WithRequestTimeout(clientConfig.requestTimeout),
		vault.WithRetryConfiguration(vault.RetryConfiguration{
			RetryWaitMin: clientConfig.retryWaitMin,
			RetryWaitMax: clientConfig.retryWaitMax,
			RetryMax:     clientConfig.retryMax,
			Backoff:      vaultRetryBackoff,
		}),
	)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func vaultRetryBackoff(minimum, maximum time.Duration, attempt int, response *http.Response) time.Duration {
	delay := retryablehttp.DefaultBackoff(minimum, maximum, attempt, response)
	if delay <= 1 || responseHasRetryAfter(response) {
		return delay
	}

	// Equal jitter prevents clients from retrying in lockstep while retaining a
	// useful minimum delay. The request context still caps the total wait.
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(delay-half)+1))
}

func responseHasRetryAfter(response *http.Response) bool {
	if response == nil || (response.StatusCode != http.StatusTooManyRequests && response.StatusCode != http.StatusServiceUnavailable) {
		return false
	}

	return response.Header.Get("Retry-After") != ""
}

func newVaultClientWithToken(ctx context.Context, host, token, mountPath string, logger logger.Logger, path string, clientConfig vaultClientConfig) (*Client, error) {
	client, err := newVaultAPIClient(host, clientConfig)
	if err != nil {
		return nil, err
	}

	if err := client.SetToken(token); err != nil {
		return nil, errors.Wrap(err, "failed to set token on Vault client")
	}

	return newClient(ctx, &client.Secrets, mountPath, logger, path, clientConfig.requestTimeout), nil
}

func newVaultClientWithKubernetesAuth(ctx context.Context, host, role, mountPath, kubernetesAuthMountPath string, logger logger.Logger, path string, clientConfig vaultClientConfig) (*Client, error) {
	client, err := newVaultAPIClient(host, clientConfig)
	if err != nil {
		return nil, err
	}

	const serviceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	token, err := os.ReadFile(serviceAccountPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read service account token")
	}
	serviceAccountToken := strings.TrimSpace(string(token))
	if serviceAccountToken == "" {
		return nil, errors.New("service account token is empty")
	}

	authContext, cancel := context.WithTimeout(ctx, clientConfig.requestTimeout)
	defer cancel()

	clientToken, err := loginToVaultWithKubernetes(authContext, client, serviceAccountToken, role, kubernetesAuthMountPath)
	if err != nil {
		return nil, err
	}

	if err := client.SetToken(clientToken); err != nil {
		return nil, errors.Wrap(err, "failed to set token on secrets client")
	}

	return newClient(ctx, &client.Secrets, mountPath, logger, path, clientConfig.requestTimeout), nil
}

func loginToVaultWithKubernetes(ctx context.Context, client *vault.Client, serviceAccountToken, role, kubernetesAuthMountPath string) (string, error) {
	resp, err := client.Auth.KubernetesLogin(ctx, schema.KubernetesLoginRequest{Jwt: serviceAccountToken, Role: role}, vault.WithMountPath(kubernetesAuthMountPath))
	if err != nil {
		return "", sanitizedVaultError(err, "failed to login to the secrets backend")
	}
	if resp == nil || resp.Auth == nil || strings.TrimSpace(resp.Auth.ClientToken) == "" {
		return "", errors.New("failed to login to the secrets backend: Vault returned no client token")
	}

	return resp.Auth.ClientToken, nil
}

func newClient(ctx context.Context, client kvV2Reader, mountPath string, logger logger.Logger, path string, requestTimeout time.Duration) *Client {
	return &Client{
		client:                  client,
		mountPath:               mountPath,
		path:                    path,
		logger:                  logger,
		ctx:                     ctx,
		requestTimeout:          requestTimeout,
		cacheManagers:           make(map[string]config.ConnectionAndDetailsGetter),
		cacheConnections:        make(map[string]any),
		cacheConnectionsDetails: make(map[string]any),
	}
}

func (c *Client) GetConnection(name string) any {
	conn, err := c.ResolveConnection(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	return conn
}

// ResolveConnection implements config.ConnectionResolver.
func (c *Client) ResolveConnection(name string) (any, error) {
	c.cacheMu.RLock()
	if conn, ok := c.cacheConnections[name]; ok {
		c.cacheMu.RUnlock()
		return conn, nil
	}
	c.cacheMu.RUnlock()

	manager, err := c.getVaultManager(name)
	if err != nil {
		return nil, err
	}

	conn := manager.GetConnection(name)
	c.cacheMu.Lock()
	if c.cacheConnections == nil {
		c.cacheConnections = make(map[string]any)
	}
	c.cacheConnections[name] = conn
	c.cacheMu.Unlock()

	return conn, nil
}

func (c *Client) GetConnectionDetails(name string) any {
	c.cacheMu.RLock()
	if deets, ok := c.cacheConnectionsDetails[name]; ok {
		c.cacheMu.RUnlock()
		return deets
	}
	c.cacheMu.RUnlock()

	manager, err := c.getVaultManager(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	deets := manager.GetConnectionDetails(name)
	c.cacheMu.Lock()
	if c.cacheConnectionsDetails == nil {
		c.cacheConnectionsDetails = make(map[string]any)
	}
	c.cacheConnectionsDetails[name] = deets
	c.cacheMu.Unlock()

	return deets
}

func (c *Client) GetConnectionType(name string) string {
	manager, err := c.getVaultManager(name)
	if err != nil {
		return ""
	}
	return manager.GetConnectionType(name)
}

func (c *Client) getVaultManager(name string) (config.ConnectionAndDetailsGetter, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("cannot read a Vault secret with an empty name")
	}

	c.cacheMu.RLock()
	manager, ok := c.cacheManagers[name]
	c.cacheMu.RUnlock()
	if ok {
		return manager, nil
	}

	result, err, _ := c.managerRequests.Do(name, func() (any, error) {
		c.cacheMu.RLock()
		manager, ok := c.cacheManagers[name]
		c.cacheMu.RUnlock()
		if ok {
			return manager, nil
		}

		manager, err := c.fetchVaultManager(name)
		if err != nil {
			return nil, err
		}

		c.cacheMu.Lock()
		if c.cacheManagers == nil {
			c.cacheManagers = make(map[string]config.ConnectionAndDetailsGetter)
		}
		c.cacheManagers[name] = manager
		c.cacheMu.Unlock()

		return manager, nil
	})
	if err != nil {
		return nil, err
	}

	manager, ok = result.(config.ConnectionAndDetailsGetter)
	if !ok {
		return nil, errors.New("failed to process Vault secret manager")
	}

	return manager, nil
}

func (c *Client) fetchVaultManager(name string) (config.ConnectionAndDetailsGetter, error) {
	requestTimeout := c.requestTimeout
	if requestTimeout <= 0 {
		requestTimeout = defaultVaultRequestTimeout
	}
	baseContext := c.ctx
	if baseContext == nil {
		baseContext = context.Background()
	}
	ctx, cancelFunc := context.WithTimeout(baseContext, requestTimeout)
	defer cancelFunc()

	secretPath := fmt.Sprintf("%s/%s", c.path, name)
	res, err := c.client.KvV2Read(ctx, secretPath, vault.WithMountPath(c.mountPath))
	if err != nil {
		var respErr *vault.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return nil, errors.Errorf("secret '%s' not found in Vault", name)
		}
		return nil, sanitizedVaultError(err, fmt.Sprintf("failed to read secret '%s' from Vault", name))
	}
	if res == nil {
		return nil, errors.Errorf("failed to read secret '%s' from Vault: Vault returned an empty response", name)
	}

	detailsRaw, okDetails := res.Data.Data["details"]
	secretType, okType := res.Data.Data["type"].(string)
	details, ok := detailsRaw.(map[string]any)
	if !okDetails || !ok || !okType || strings.TrimSpace(secretType) == "" {
		return nil, errors.Errorf("secret '%s' must contain both 'type' (non-empty string) and 'details' (object)", name)
	}

	details["name"] = name

	// This is a hacky way to use the already existing logic in connections manager that processes connections config to create the right
	// platform/db client
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

	config := config.Config{
		Environments: map[string]config.Environment{
			"default": environment,
		},
		SelectedEnvironmentName: "default",
		SelectedEnvironment:     &environment,
		DefaultEnvironmentName:  "default",
	}

	manager, errs := connection.NewManagerFromConfig(&config)
	if len(errs) > 0 {
		return nil, errors.Wrapf(errs[0], "failed to configure connection '%s'", name)
	}

	return manager, nil
}

func sanitizedVaultError(err error, message string) error {
	var responseError *vault.ResponseError
	if errors.As(err, &responseError) {
		statusText := http.StatusText(responseError.StatusCode)
		if statusText == "" {
			return errors.Errorf("%s: Vault returned HTTP status %d", message, responseError.StatusCode)
		}
		return errors.Errorf("%s: Vault returned HTTP status %d (%s)", message, responseError.StatusCode, statusText)
	}

	return errors.Wrap(err, message)
}
