package bigquery

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
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
	ReadOnly                         bool
}

// Bruin Cloud caps an agent run's BigQuery connections with these: per connection name, or one cap for all.
const (
	MaxBillableBytesEnv             = "BRUIN_BIGQUERY_MAX_BILLABLE_BYTES"
	MaxBillableBytesByConnectionEnv = "BRUIN_BIGQUERY_MAX_BILLABLE_BYTES_BY_CONNECTION"
)

func MaxBillableBytesFromEnv(connectionName string) (*int64, error) {
	if raw := strings.TrimSpace(os.Getenv(MaxBillableBytesByConnectionEnv)); raw != "" {
		var limits map[string]int64
		if err := json.Unmarshal([]byte(raw), &limits); err != nil {
			return nil, fmt.Errorf("%s must be a JSON object of connection name to bytes: %w", MaxBillableBytesByConnectionEnv, err)
		}
		if limit, ok := limits[connectionName]; ok {
			if limit <= 0 {
				return nil, fmt.Errorf("%s: the limit for %q must be a positive integer, got %d", MaxBillableBytesByConnectionEnv, connectionName, limit)
			}
			return &limit, nil
		}
	}

	raw := strings.TrimSpace(os.Getenv(MaxBillableBytesEnv))
	if raw == "" {
		return nil, nil //nolint:nilnil // unset means no cap
	}

	limit, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || limit <= 0 {
		return nil, fmt.Errorf("%s must be a positive integer, got %q", MaxBillableBytesEnv, raw)
	}

	return &limit, nil
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
