package spark

import (
	"fmt"
	"net/url"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

const (
	optionCatalog           = "adbc.connection.catalog"
	optionIngestLocation    = "spark.ingest.location"
	optionIngestStagingArea = "spark.ingest.staging_area_uri"
)

// Config contains the options passed to the ADBC Spark driver. URI uses the
// driver's spark:// URI format; Options exposes driver-specific settings that
// are not modeled explicitly by Bruin.
type Config struct {
	URI               string
	Catalog           string
	IngestLocation    string
	IngestStagingArea string
	Options           map[string]string
}

func (c Config) validate() error {
	if strings.TrimSpace(c.URI) == "" {
		return errors.New("Spark connection URI is required")
	}
	if strings.ContainsRune(c.URI, ';') {
		return errors.New("Spark connection URI cannot contain ';' (the DSN option delimiter)")
	}
	if strings.ContainsRune(c.Catalog, ';') {
		return errors.New("Spark connection catalog cannot contain ';' (the DSN option delimiter)")
	}
	if strings.ContainsRune(c.IngestLocation, ';') {
		return errors.New("Spark connection ingest_location cannot contain ';' (the DSN option delimiter)")
	}
	if strings.ContainsRune(c.IngestStagingArea, ';') {
		return errors.New("Spark connection ingest_staging_area cannot contain ';' (the DSN option delimiter)")
	}

	parsed, err := url.Parse(c.URI)
	if err != nil {
		return errors.Wrap(err, "invalid Spark connection URI")
	}
	if parsed.Scheme != "spark" {
		return fmt.Errorf("spark connection URI must use the spark:// scheme, got %q", parsed.Scheme)
	}
	if parsed.Hostname() == "" || parsed.Port() == "" {
		return errors.New("Spark connection URI must include a host and port")
	}

	for key, value := range c.Options {
		if strings.TrimSpace(key) == "" {
			return errors.New("Spark connection option names cannot be empty")
		}
		if strings.ContainsRune(key, ';') || strings.ContainsRune(value, ';') {
			return fmt.Errorf("spark connection option %q cannot contain ';' (the DSN option delimiter)", key)
		}
		switch key {
		case "driver", "uri":
			return fmt.Errorf("spark connection option %q is managed by Bruin and cannot be overridden", key)
		case optionCatalog:
			if c.Catalog != "" {
				return fmt.Errorf("spark connection option %q cannot be set when catalog is configured", key)
			}
		case optionIngestLocation, optionIngestStagingArea:
			return fmt.Errorf("spark connection option %q must be configured using its dedicated connection field", key)
		}
	}

	return nil
}

// ToOptions builds the native option map consumed by the ADBC driver manager.
func (c Config) ToOptions() (map[string]string, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}

	options := make(map[string]string, len(c.Options)+4)
	options["driver"] = dbcDriverName
	options["uri"] = c.URI
	if c.Catalog != "" {
		options[optionCatalog] = c.Catalog
	}
	for key, value := range c.Options {
		options[key] = value
	}

	return options, nil
}

// ToDSN builds the semicolon-delimited DSN used by ADBC's database/sql bridge.
func (c Config) ToDSN() (string, error) {
	options, err := c.ToOptions()
	if err != nil {
		return "", err
	}

	keys := make([]string, 0, len(options))
	for key := range options {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+options[key])
	}
	return strings.Join(parts, ";"), nil
}

// IngestOptions returns statement-level options for ADBC bulk ingestion.
// The Spark driver does not accept these options when opening a database.
func (c Config) IngestOptions() map[string]string {
	options := make(map[string]string, 2)
	if c.IngestStagingArea != "" {
		options[optionIngestStagingArea] = c.IngestStagingArea
	}
	if c.IngestLocation != "" {
		options[optionIngestLocation] = c.IngestLocation
	}
	return options
}

func (c Config) GetDatabase() string {
	return c.Catalog
}
