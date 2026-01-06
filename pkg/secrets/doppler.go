package secrets

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
)

const dopplerAPIBaseURL = "https://api.doppler.com/v3"

type dopplerHTTPClient interface {
	GetSecret(ctx context.Context, secretName string) (map[string]any, error)
}

type httpDopplerClient struct {
	token   string
	project string
	config  string
	client  *http.Client
}

func (h *httpDopplerClient) GetSecret(ctx context.Context, secretName string) (map[string]any, error) {
	url := fmt.Sprintf("%s/configs/config/secrets/download?project=%s&config=%s&format=json", dopplerAPIBaseURL, h.project, h.config)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create request")
	}

	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Accept", "application/json")

	resp, err := h.client.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch secrets from Doppler")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, errors.Errorf("doppler API returned status %d: %s", resp.StatusCode, string(body))
	}

	var allSecrets map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&allSecrets); err != nil {
		return nil, errors.Wrap(err, "failed to decode Doppler response")
	}

	secretValue, ok := allSecrets[secretName]
	if !ok {
		return nil, errors.Errorf("secret '%s' not found in Doppler", secretName)
	}

	secretStr, ok := secretValue.(string)
	if !ok {
		return nil, errors.Errorf("secret '%s' is not a string", secretName)
	}

	var secretData map[string]any
	if err := json.Unmarshal([]byte(secretStr), &secretData); err != nil {
		return nil, errors.Wrap(err, "failed to parse secret as JSON")
	}

	return secretData, nil
}

// DopplerClient manages secrets from Doppler.
type DopplerClient struct {
	client                  dopplerHTTPClient
	logger                  logger.Logger
	cacheConnections        map[string]any
	cacheConnectionsDetails map[string]any
}

// NewDopplerClientFromEnv creates a new Doppler client from environment variables.
func NewDopplerClientFromEnv(logger logger.Logger) (*DopplerClient, error) {
	token := os.Getenv("BRUIN_DOPPLER_TOKEN")
	if token == "" {
		return nil, errors.New("BRUIN_DOPPLER_TOKEN env variable not set")
	}
	project := os.Getenv("BRUIN_DOPPLER_PROJECT")
	if project == "" {
		return nil, errors.New("BRUIN_DOPPLER_PROJECT env variable not set")
	}
	config := os.Getenv("BRUIN_DOPPLER_CONFIG")
	if config == "" {
		return nil, errors.New("BRUIN_DOPPLER_CONFIG env variable not set")
	}

	return NewDopplerClient(logger, token, project, config)
}

// NewDopplerClient creates a new Doppler secrets client.
func NewDopplerClient(logger logger.Logger, token, project, config string) (*DopplerClient, error) {
	if token == "" {
		return nil, errors.New("empty doppler token provided")
	}
	if project == "" {
		return nil, errors.New("empty doppler project provided")
	}
	if config == "" {
		return nil, errors.New("empty doppler config provided")
	}

	httpClient := &httpDopplerClient{
		token:   token,
		project: project,
		config:  config,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}

	return &DopplerClient{
		client:                  httpClient,
		logger:                  logger,
		cacheConnections:        make(map[string]any),
		cacheConnectionsDetails: make(map[string]any),
	}, nil
}

// GetConnection retrieves a connection by name from Doppler.
func (c *DopplerClient) GetConnection(name string) any {
	if conn, ok := c.cacheConnections[name]; ok {
		return conn
	}

	manager, err := c.getDopplerManager(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	conn := manager.GetConnection(name)
	c.cacheConnections[name] = conn

	return conn
}

// GetConnectionDetails retrieves connection details by name from Doppler.
func (c *DopplerClient) GetConnectionDetails(name string) any {
	if deets, ok := c.cacheConnectionsDetails[name]; ok {
		return deets
	}

	manager, err := c.getDopplerManager(name)
	if err != nil {
		c.logger.Errorf("%v", err)
		return nil
	}

	deets := manager.GetConnectionDetails(name)
	c.cacheConnectionsDetails[name] = deets

	return deets
}

func (c *DopplerClient) getDopplerManager(name string) (config.ConnectionAndDetailsGetter, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelFunc()

	secretData, err := c.client.GetSecret(ctx, name)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read secret '%s' from Doppler", name)
	}

	detailsRaw, okDetails := secretData["details"]
	secretType, okType := secretData["type"].(string)
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
