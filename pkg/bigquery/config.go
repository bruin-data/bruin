package bigquery

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"golang.org/x/oauth2/google"
)

type Config struct {
	ProjectID           string `envconfig:"BIGQUERY_PROJECT"`
	CredentialsFilePath string `envconfig:"BIGQUERY_CREDENTIALS_FILE"`
	CredentialsJSON     string
	AccessToken         string
	Credentials         *google.Credentials
	Location            string `envconfig:"BIGQUERY_LOCATION"`
	// Add support for Application Default Credentials
	UseApplicationDefaultCredentials bool `envconfig:"BIGQUERY_USE_ADC"`
	MaxBillableBytes                 *int64
	MaxQueryCost                     *float64
	MaxBillableBytesSoft             *int64
	MaxQueryCostSoft                 *float64
	EnvMaxBillableBytes              *int64
	EnvMaxQueryCostSoft              *float64
	ReadOnly                         bool
}

// MaxBillableBytesByConnectionEnv lets Bruin Cloud cap an agent run's BigQuery connections: a JSON object of connection name to bytes.
const MaxBillableBytesByConnectionEnv = "BRUIN_BIGQUERY_MAX_BILLABLE_BYTES_BY_CONNECTION"

// MaxQueryCostSoftByConnectionEnv sets soft limits the same way: a JSON object of connection name to USD.
const MaxQueryCostSoftByConnectionEnv = "BRUIN_BIGQUERY_MAX_QUERY_COST_SOFT_BY_CONNECTION"

func MaxBillableBytesFromEnv(connectionName string) (*int64, error) {
	return connectionLimitFromEnv[int64](MaxBillableBytesByConnectionEnv, connectionName, "bytes")
}

func MaxQueryCostSoftFromEnv(connectionName string) (*float64, error) {
	return connectionLimitFromEnv[float64](MaxQueryCostSoftByConnectionEnv, connectionName, "USD")
}

func connectionLimitFromEnv[T int64 | float64](envName, connectionName, unit string) (*T, error) {
	raw := strings.TrimSpace(os.Getenv(envName))
	if raw == "" {
		return nil, nil //nolint:nilnil // unset means no cap
	}

	var limits map[string]T
	if err := json.Unmarshal([]byte(raw), &limits); err != nil || limits == nil {
		return nil, fmt.Errorf("%s must be a JSON object of connection name to %s, got %q", envName, unit, raw)
	}

	limit, ok := limits[connectionName]
	if !ok {
		return nil, nil //nolint:nilnil // connection has no cap
	}
	if limit <= 0 {
		return nil, fmt.Errorf("%s: the limit for %q must be positive, got %v", envName, connectionName, limit)
	}

	return &limit, nil
}

func (c Config) maxQueryCostSoft() *float64 {
	if c.EnvMaxQueryCostSoft == nil {
		return c.MaxQueryCostSoft
	}
	if c.MaxQueryCostSoft != nil && *c.MaxQueryCostSoft < *c.EnvMaxQueryCostSoft {
		return c.MaxQueryCostSoft
	}

	return c.EnvMaxQueryCostSoft
}

// The env cap is only sent to BigQuery, never dry-run first like max_billable_bytes, so scripts a dry run can't plan still run.
func (c Config) bytesBilledLimit() *int64 {
	if c.EnvMaxBillableBytes == nil {
		return c.MaxBillableBytes
	}
	if c.MaxBillableBytes != nil && *c.MaxBillableBytes < *c.EnvMaxBillableBytes {
		return c.MaxBillableBytes
	}

	return c.EnvMaxBillableBytes
}

func (c Config) IsValid() bool {
	// Valid if we have a project ID and at least one credential method
	return c.ProjectID != "" && (c.CredentialsFilePath != "" ||
		c.CredentialsJSON != "" ||
		c.AccessToken != "" ||
		c.Credentials != nil ||
		c.UseApplicationDefaultCredentials)
}

func (c Config) GetConnectionURI() (string, error) {
	if c.ReadOnly {
		return "", errors.New("read_only connections cannot be used with ingestr")
	}
	// If using Application Default Credentials, return simple URI
	// ingestr will use ADC automatically when no credentials are in the URI
	if c.UseApplicationDefaultCredentials {
		URI := "bigquery://" + c.ProjectID
		if c.Location != "" {
			URI += "?location=" + c.Location
		}
		return URI, nil
	}

	var creds []byte
	switch {
	case c.AccessToken != "":
		return "", errors.New("BigQuery connections authenticated with an OAuth access token cannot be used for ingestr assets, use a service account instead")
	case c.CredentialsJSON != "":
		creds = []byte(c.CredentialsJSON)
	case c.CredentialsFilePath != "":
		var err error
		creds, err = os.ReadFile(c.CredentialsFilePath)
		if err != nil {
			return "", err
		}
	case c.Credentials != nil:
		return "", errors.New("only `service_account_json` or `service_account_file` supported")
	default:
		return "", errors.New("could not find google credentials")
	}

	URI := fmt.Sprintf("bigquery://%s?credentials_base64=%s", c.ProjectID, base64.StdEncoding.EncodeToString(creds))

	if c.Location != "" {
		URI += "&location=" + c.Location
	}

	return URI, nil
}

func (c Config) GetIngestrURI() (string, error) {
	return c.GetConnectionURI()
}
