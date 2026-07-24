package spark

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfigToOptions(t *testing.T) {
	t.Parallel()

	config := Config{
		URI:               "spark://:token@example.com:443?api=connect&auth_type=token&tls=true",
		Catalog:           "analytics",
		IngestLocation:    "s3://bucket/tables/contacts",
		IngestStagingArea: "s3://bucket/staging",
		Options: map[string]string{
			"spark.connect.timeout": "30",
		},
	}

	options, err := config.ToOptions()
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"driver":                  "spark",
		"uri":                     config.URI,
		"adbc.connection.catalog": "analytics",
		"spark.connect.timeout":   "30",
	}, options)

	dsn, err := config.ToDSN()
	require.NoError(t, err)
	require.Equal(
		t,
		"adbc.connection.catalog=analytics;driver=spark;spark.connect.timeout=30;uri="+config.URI,
		dsn,
	)
	require.Equal(t, map[string]string{
		"spark.ingest.location":         "s3://bucket/tables/contacts",
		"spark.ingest.staging_area_uri": "s3://bucket/staging",
	}, config.IngestOptions())
}

func TestConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config Config
		error  string
	}{
		{name: "missing URI", config: Config{}, error: "URI is required"},
		{name: "wrong scheme", config: Config{URI: "https://localhost:443"}, error: "spark://"},
		{name: "missing host", config: Config{URI: "spark:///path"}, error: "host and port"},
		{name: "DSN delimiter", config: Config{URI: "spark://localhost:10000;token=x"}, error: "cannot contain ';'"},
		{
			name:   "catalog DSN delimiter",
			config: Config{URI: "spark://localhost:10000", Catalog: "main;other"},
			error:  "catalog cannot contain ';'",
		},
		{
			name:   "managed URI option",
			config: Config{URI: "spark://localhost:10000", Options: map[string]string{"uri": "spark://elsewhere:10000"}},
			error:  "managed by Bruin",
		},
		{
			name:   "duplicate catalog option",
			config: Config{URI: "spark://localhost:10000", Catalog: "main", Options: map[string]string{optionCatalog: "other"}},
			error:  "catalog is configured",
		},
		{
			name:   "statement option in options",
			config: Config{URI: "spark://localhost:10000", Options: map[string]string{optionIngestStagingArea: "s3://bucket/staging"}},
			error:  "dedicated connection field",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := test.config.ToOptions()
			require.ErrorContains(t, err, test.error)
		})
	}
}
