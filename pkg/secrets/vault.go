package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
	"github.com/pkg/errors"
)

func NewVaultClientFromEnv(logger logger.Logger) (*Client, error) {
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

	return NewVaultClient(logger, host, token, role, path, mountPath)
}

func NewVaultClient(logger logger.Logger, host, token, role, path string, mountPath string) (*Client, error) {
	if host == "" {
		return nil, errors.New("empty vault host provided")
	}
	if path == "" {
		return nil, errors.New("empty vault path provided")
	}
	if mountPath == "" {
		return nil, errors.New("empty vault mountpath provided")
	}
	if token != "" {
		return newVaultClientWithToken(host, token, mountPath, logger, path)
	}
	if role != "" {
		return newVaultClientWithKubernetesAuth(host, role, mountPath, logger, path)
	}

	return nil, errors.New("no vault credentials provided")
}

type kvV2Reader interface {
	KvV2Read(ctx context.Context, path string, options ...vault.RequestOption) (*vault.Response[schema.KvV2ReadResponse], error)
}

type Client struct {
	client                  kvV2Reader
	mountPath               string
	path                    string
	logger                  logger.Logger
	cacheConnections        map[string]any
	cacheConnectionsDetails map[string]any
}

func newVaultClientWithToken(host, token, mountPath string, logger logger.Logger, path string) (*Client, error) {
	client, err := vault.New(
		vault.WithAddress(host),
		vault.WithRequestTimeout(30*time.Second),
	)
	if err != nil {
		return nil, err
	}

	if err := client.SetToken(token); err != nil {
		return nil, errors.Wrap(err, "failed to set token on Vault client")
	}

	return &Client{
		client:                  &client.Secrets,
		mountPath:               mountPath,
		path:                    path,
		logger:                  logger,
		cacheConnections:        make(map[string]any),
		cacheConnectionsDetails: make(map[string]any),
	}, nil
}

func newVaultClientWithKubernetesAuth(host, role, mountPath string, logger logger.Logger, path string) (*Client, error) {
	client, err := vault.New(
		vault.WithAddress(host),
		vault.WithRequestTimeout(30*time.Second),
	)
	if err != nil {
		return nil, err
	}

	const serviceAccountPath = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	token, err := os.ReadFile(serviceAccountPath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read service account token")
	}

	resp, err := client.Auth.KubernetesLogin(context.Background(), schema.KubernetesLoginRequest{Jwt: string(token), Role: role})
	if err != nil {
		return nil, errors.Wrap(err, "failed to login to the secrets backend")
	}

	if err := client.SetToken(resp.Auth.ClientToken); err != nil {
		return nil, errors.Wrap(err, "failed to set token on secrets client")
	}

	return &Client{
		client:                  &client.Secrets,
		mountPath:               mountPath,
		path:                    path,
		logger:                  logger,
		cacheConnections:        make(map[string]any),
		cacheConnectionsDetails: make(map[string]any),
	}, nil
}

func (c *Client) GetConnection(name string) any {
	if conn, ok := c.cacheConnections[name]; ok {
		return conn
	}

	manager, err := c.getVaultManager(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	conn := manager.GetConnection(name)
	c.cacheConnections[name] = conn

	return conn
}

func (c *Client) GetConnectionDetails(name string) any {
	if deets, ok := c.cacheConnectionsDetails[name]; ok {
		return deets
	}

	manager, err := c.getVaultManager(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	deets := manager.GetConnectionDetails(name)
	c.cacheConnectionsDetails[name] = deets

	return deets
}

func (c *Client) getVaultManager(name string) (config.ConnectionAndDetailsGetter, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()

	secretPath := fmt.Sprintf("%s/%s", c.path, name)
	res, err := c.client.KvV2Read(ctx, secretPath, vault.WithMountPath(c.mountPath))
	if err != nil {
		var respErr *vault.ResponseError
		if errors.As(err, &respErr) && respErr.StatusCode == http.StatusNotFound {
			return nil, errors.Errorf("secret '%s' not found in Vault", name)
		}
		return nil, errors.Wrapf(err, "failed to read secret '%s' from Vault", name)
	}

	detailsRaw, okDetails := res.Data.Data["details"]
	secretType, okType := res.Data.Data["type"].(string)
	if !okDetails && !okType {
		return nil, errors.Errorf("secret '%s' is missing required 'details' or 'type' fields", name)
	}

	details, ok := detailsRaw.(map[string]any)
	if !ok {
		return nil, errors.Errorf("secret '%s' has invalid 'details' field: expected a map", name)
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
