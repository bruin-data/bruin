package secrets

import (
	"context"
	"encoding/json"
	"os"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/logger"
	infisical "github.com/infisical/go-sdk"
	"github.com/pkg/errors"
)

// InfisicalClient manages secrets from Infisical.
type InfisicalClient struct {
	client                  infisical.InfisicalClientInterface
	projectID               string
	environment             string
	secretPath              string
	logger                  logger.Logger
	cacheConnections        map[string]any
	cacheConnectionsDetails map[string]any
}

// NewInfisicalClientFromEnv creates a new Infisical client from environment variables.
func NewInfisicalClientFromEnv(logger logger.Logger) (*InfisicalClient, error) {
	host := os.Getenv("BRUIN_INFISICAL_HOST")
	if host == "" {
		host = "https://app.infisical.com" // Default Infisical cloud
	}

	clientID := os.Getenv("BRUIN_INFISICAL_CLIENT_ID")
	if clientID == "" {
		return nil, errors.New("BRUIN_INFISICAL_CLIENT_ID env variable not set")
	}

	clientSecret := os.Getenv("BRUIN_INFISICAL_CLIENT_SECRET")
	if clientSecret == "" {
		return nil, errors.New("BRUIN_INFISICAL_CLIENT_SECRET env variable not set")
	}

	projectID := os.Getenv("BRUIN_INFISICAL_PROJECT_ID")
	if projectID == "" {
		return nil, errors.New("BRUIN_INFISICAL_PROJECT_ID env variable not set")
	}

	environment := os.Getenv("BRUIN_INFISICAL_ENVIRONMENT")
	if environment == "" {
		return nil, errors.New("BRUIN_INFISICAL_ENVIRONMENT env variable not set")
	}

	secretPath := os.Getenv("BRUIN_INFISICAL_SECRET_PATH")
	if secretPath == "" {
		secretPath = "/" // Default path
	}

	return NewInfisicalClient(logger, host, clientID, clientSecret, projectID, environment, secretPath)
}

// NewInfisicalClient creates a new Infisical secrets client.
func NewInfisicalClient(logger logger.Logger, host, clientID, clientSecret, projectID, environment, secretPath string) (*InfisicalClient, error) {
	if host == "" {
		return nil, errors.New("empty infisical host provided")
	}
	if clientID == "" {
		return nil, errors.New("empty infisical client ID provided")
	}
	if clientSecret == "" {
		return nil, errors.New("empty infisical client secret provided")
	}
	if projectID == "" {
		return nil, errors.New("empty infisical project ID provided")
	}
	if environment == "" {
		return nil, errors.New("empty infisical environment provided")
	}
	if secretPath == "" {
		secretPath = "/"
	}

	// Create Infisical client with config
	client := infisical.NewInfisicalClient(context.Background(), infisical.Config{
		SiteUrl:              host,
		AutoTokenRefresh:     true,
		CacheExpiryInSeconds: 0, // Disable SDK caching, use app-level caching
	})

	// Authenticate using Universal Auth
	_, err := client.Auth().UniversalAuthLogin(clientID, clientSecret)
	if err != nil {
		return nil, errors.Wrap(err, "failed to authenticate with Infisical")
	}

	return &InfisicalClient{
		client:                  client,
		projectID:               projectID,
		environment:             environment,
		secretPath:              secretPath,
		logger:                  logger,
		cacheConnections:        make(map[string]any),
		cacheConnectionsDetails: make(map[string]any),
	}, nil
}

// GetConnection retrieves a connection by name from Infisical.
func (c *InfisicalClient) GetConnection(name string) any {
	if conn, ok := c.cacheConnections[name]; ok {
		return conn
	}

	manager, err := c.getInfisicalManager(name)
	if err != nil {
		return nil
	}

	conn := manager.GetConnection(name)
	c.cacheConnections[name] = conn

	return conn
}

// GetConnectionDetails retrieves connection details by name from Infisical.
func (c *InfisicalClient) GetConnectionDetails(name string) any {
	if deets, ok := c.cacheConnectionsDetails[name]; ok {
		return deets
	}

	manager, err := c.getInfisicalManager(name)
	if err != nil {
		return nil
	}

	deets := manager.GetConnectionDetails(name)
	c.cacheConnectionsDetails[name] = deets

	return deets
}

func (c *InfisicalClient) getInfisicalManager(name string) (config.ConnectionAndDetailsGetter, error) {
	// Retrieve secret from Infisical
	secret, err := c.client.Secrets().Retrieve(infisical.RetrieveSecretOptions{
		SecretKey:   name,
		Environment: c.environment,
		ProjectID:   c.projectID,
		SecretPath:  c.secretPath,
	})
	if err != nil {
		c.logger.Error("failed to read secret from Infisical", "error", err)
		return nil, err
	}

	// Parse the secret value as JSON
	var secretData map[string]any
	if err := json.Unmarshal([]byte(secret.SecretValue), &secretData); err != nil {
		c.logger.Error("failed to parse secret as JSON", "error", err, "secret", name)
		return nil, errors.Wrap(err, "failed to parse secret as JSON")
	}

	detailsRaw, okDetails := secretData["details"]
	secretType, okType := secretData["type"].(string)
	if !okDetails && !okType {
		c.logger.Error("failed to read secret from Infisical", "error", "no details or type found")
		return nil, errors.New("no details or type found")
	}

	details, ok := detailsRaw.(map[string]any)
	if !ok {
		c.logger.Error("failed to read secret from Infisical", "error", "details is not a map", "details:", detailsRaw)
		return nil, errors.New("details is not a map")
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
		c.logger.Error("failed to marshal connections map", "error", err)
		return nil, err
	}

	var connections config.Connections

	if err := json.Unmarshal(serialized, &connections); err != nil {
		c.logger.Error("failed to unmarshal connections map", "error", err)
		return nil, err
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
		c.logger.Error("failed to create manager from config", "error", errs)
		return nil, errors.Wrap(errs[0], "failed to create manager from config")
	}

	return manager, nil
}
