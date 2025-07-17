package secrets

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/hashicorp/vault-client-go"
	"github.com/hashicorp/vault-client-go/schema"
	"github.com/pkg/errors"
)

func NewVaultClient(logger logger.Logger, host, token, role string) (*Client, error) {
	if host == "" {
		return nil, nil
	}
	if token != "" {
		return newVaultClientWithToken(host, token, "bruin", logger)
	}

	if role != "" {
		return newVaultClientWithKubernetesAuth(host, role, "bruin", logger)
	}

	return nil, errors.New("no vault credentials provided")
}

type Client struct {
	client    *vault.Client
	mountPath string
	logger    logger.Logger
}

func newVaultClientWithToken(host, token, mountPath string, logger logger.Logger) (*Client, error) {
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
		client:    client,
		mountPath: mountPath,
		logger:    logger,
	}, nil
}

func newVaultClientWithKubernetesAuth(host, role, mountPath string, logger logger.Logger) (*Client, error) {
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
		client:    client,
		mountPath: mountPath,
		logger:    logger,
	}, nil
}

func (c *Client) GetConnection(name string) any {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()

	res, err := c.client.Secrets.KvV2Read(ctx, name, vault.WithMountPath(c.mountPath))
	if err != nil {
		c.logger.Error("failed to read secret from Vault", "error", err)
		return nil
	}

	detailsRaw, okDetails := res.Data.Data["details"]
	secretType, okType := res.Data.Data["type"].(string)
	if !okDetails && !okType {
		c.logger.Error("failed to read secret from Vault", "error", "no details or type found")
		return nil
	}

	details, ok := detailsRaw.(map[string]string)
	if !ok {
		c.logger.Error("failed to read secret from Vault", "error", "details is not a map")
		return nil
	}

	connectionsMap := map[string][]map[string]string{
		secretType: {
			details,
		},
	}

	serialized, err := json.Marshal(connectionsMap)
	if err != nil {
		c.logger.Error("failed to marshal connections map", "error", err)
		return nil
	}

	var connections config.Connections
	if err := json.Unmarshal(serialized, &connections); err != nil {
		c.logger.Error("failed to unmarshal connections map", "error", err)
		return nil
	}

	return connections.GetByName(name)
}
