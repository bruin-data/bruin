package iceberg

import (
	"net/url"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAWSRegion = "us-east-1"

func parseQuery(t *testing.T, raw string) (string, url.Values) {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u.Scheme, u.Query()
}

func TestConfig_GetIngestrURI_GlueS3(t *testing.T) {
	t.Parallel()

	c := Config{
		CatalogName: "analytics",
		Catalog: config.IcebergCatalog{
			Type:      config.IcebergCatalogGlue,
			CatalogID: "123456789012",
			Region:    testAWSRegion,
			Auth:      config.IcebergAuth{AccessKey: "AKID", SecretKey: "SECRET"},
		},
		Storage: config.IcebergStorage{
			Type: config.IcebergStorageS3,
			Path: "s3://company-lake/warehouse",
		},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)

	scheme, q := parseQuery(t, got)
	assert.Equal(t, "iceberg+glue", scheme)
	assert.Equal(t, "s3", q.Get("storage"))
	assert.Equal(t, "s3://company-lake/warehouse", q.Get("warehouse"))
	assert.Equal(t, testAWSRegion, q.Get("region"))
	assert.Equal(t, "AKID", q.Get("access_key_id"))
	assert.Equal(t, "SECRET", q.Get("secret_access_key"))
	assert.Equal(t, "123456789012", q.Get("glue.id"))
	assert.Equal(t, "analytics", q.Get("catalog_name"))
}

func TestConfig_GetIngestrURI_SQLiteS3MinIO(t *testing.T) {
	t.Parallel()

	useSSL := false
	c := Config{
		Catalog: config.IcebergCatalog{
			Type: config.IcebergCatalogSQLite,
			Path: "/state/catalog.db",
		},
		Storage: config.IcebergStorage{
			Type:     config.IcebergStorageS3,
			Path:     "s3://ingestr-iceberg/warehouse",
			Endpoint: "localhost:9000",
			UseSSL:   &useSSL,
			Region:   testAWSRegion,
			Auth:     config.IcebergAuth{AccessKey: "minioadmin", SecretKey: "minioadmin"},
		},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+sqlite:///state/catalog.db?"), "got: %s", got)

	scheme, q := parseQuery(t, got)
	assert.Equal(t, "iceberg+sqlite", scheme)
	assert.Equal(t, "s3", q.Get("storage"))
	assert.Equal(t, "localhost:9000", q.Get("endpoint"))
	assert.Equal(t, "false", q.Get("use_ssl"))
	assert.Equal(t, "minioadmin", q.Get("access_key_id"))
}

func TestConfig_GetIngestrURI_BucketPrefixStorage(t *testing.T) {
	t.Parallel()

	// bucket (+ prefix) is an alternative to a full s3:// path.
	c := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
		Storage: config.IcebergStorage{
			Type:   config.IcebergStorageS3,
			Bucket: "company-lake",
			Prefix: "warehouse",
		},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "s3", q.Get("storage"))
	assert.Equal(t, "company-lake", q.Get("bucket"))
	assert.Equal(t, "warehouse", q.Get("prefix"))
	assert.Empty(t, q.Get("warehouse"), "warehouse must not be set when bucket is used")
}

func TestConfig_GetIngestrURI_PostgresS3(t *testing.T) {
	t.Parallel()

	c := Config{
		Catalog: config.IcebergCatalog{
			Type:     config.IcebergCatalogPostgres,
			Host:     "metadata-db.internal",
			Port:     5432,
			Database: "iceberg_catalog",
			Auth:     config.IcebergAuth{Username: "iceberg_user", Password: "secret"},
		},
		Storage: config.IcebergStorage{
			Type:   config.IcebergStorageS3,
			Path:   "s3://company-lake/warehouse",
			Region: "eu-west-1",
			Auth:   config.IcebergAuth{AccessKey: "AKID", SecretKey: "SECRET"},
		},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+postgres://iceberg_user:secret@metadata-db.internal:5432/iceberg_catalog?"), "got: %s", got)

	_, q := parseQuery(t, got)
	assert.Equal(t, "s3", q.Get("storage"))
	assert.Equal(t, "eu-west-1", q.Get("region"))
}

func TestConfig_GetIngestrURI_RESTCatalog(t *testing.T) {
	t.Parallel()

	c := Config{
		Catalog: config.IcebergCatalog{
			Type: config.IcebergCatalogREST,
			Host: "catalog.internal",
			Port: 8181,
		},
		Storage: config.IcebergStorage{
			Type:   config.IcebergStorageS3,
			Path:   "s3://warehouse/prod",
			Region: testAWSRegion,
		},
		// REST-catalog auth / advanced options via the passthrough.
		Properties: map[string]string{
			"credential":        "client-id:secret",
			"oauth2-server-uri": "https://auth.internal/token",
		},
	}

	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+rest://catalog.internal:8181?"), "got: %s", got)

	scheme, q := parseQuery(t, got)
	assert.Equal(t, "iceberg+rest", scheme)
	assert.Equal(t, "s3", q.Get("storage"))
	assert.Equal(t, "client-id:secret", q.Get("credential"))
	assert.Equal(t, "https://auth.internal/token", q.Get("oauth2-server-uri"))
}

func TestConfig_GetIngestrURI_HadoopAndHive(t *testing.T) {
	t.Parallel()

	hadoop := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogHadoop, Path: "/tmp/warehouse"},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: "s3://b/p"},
	}
	got, err := hadoop.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+hadoop:///tmp/warehouse?"), "got: %s", got)

	hive := Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogHive, Host: "metastore", Port: 9083},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: "s3://b/p"},
	}
	got, err = hive.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+hive://metastore:9083?"), "got: %s", got)
}

func TestConfig_GetIngestrURI_SQLCatalogViaProperties(t *testing.T) {
	t.Parallel()

	c := Config{
		Catalog:    config.IcebergCatalog{Type: config.IcebergCatalogSQL},
		Storage:    config.IcebergStorage{Type: config.IcebergStorageS3, Path: "s3://b/p"},
		Properties: map[string]string{"uri": "postgresql://u:p@h:5432/db"},
	}
	got, err := c.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(got, "iceberg+sql://?"), "got: %s", got)
	_, q := parseQuery(t, got)
	assert.Equal(t, "postgresql://u:p@h:5432/db", q.Get("uri"))
}

func TestConfig_GetIngestrURI_TableAndNamespaceParams(t *testing.T) {
	t.Parallel()

	createNS := false
	c := Config{
		Catalog:         config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
		Storage:         config.IcebergStorage{Type: config.IcebergStorageS3, Path: "s3://lake/wh"},
		CreateNamespace: &createNS,
		TableLocation:   "s3://lake/wh/{namespace}/{table}",
		TablePath:       "{namespace}/{table}",
		TableProperties: map[string]string{
			"write.format.default": "parquet",
			"format-version":       "2",
		},
	}

	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "false", q.Get("create_namespace"))
	assert.Equal(t, "s3://lake/wh/{namespace}/{table}", q.Get("table_location"))
	assert.Equal(t, "{namespace}/{table}", q.Get("table_path"))
	assert.Equal(t, "parquet", q.Get("table.write.format.default"))
	assert.Equal(t, "2", q.Get("table.format-version"))
}

func TestConfig_GetIngestrURI_PropertiesOverride(t *testing.T) {
	t.Parallel()

	// A passthrough property wins over a structured value on conflict.
	c := Config{
		Catalog:    config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
		Storage:    config.IcebergStorage{Type: config.IcebergStorageS3, Path: "s3://lake/wh"},
		Properties: map[string]string{"region": "eu-west-1"},
	}
	_, q := parseQuery(t, mustURI(t, c))
	assert.Equal(t, "eu-west-1", q.Get("region"))
}

func mustURI(t *testing.T, c Config) string {
	t.Helper()
	uri, err := c.GetIngestrURI()
	require.NoError(t, err)
	return uri
}

func TestConfig_GetIngestrURI_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{
			name:    "missing catalog type",
			config:  Config{Storage: config.IcebergStorage{Type: config.IcebergStorageS3}},
			wantErr: "catalog.type must be provided",
		},
		{
			name: "unsupported catalog type",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogType("duckdb")},
				Storage: config.IcebergStorage{Type: config.IcebergStorageS3},
			},
			wantErr: `unsupported catalog type "duckdb"`,
		},
		{
			name: "sqlite requires path",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogSQLite},
				Storage: config.IcebergStorage{Type: config.IcebergStorageS3},
			},
			wantErr: "sqlite catalog requires",
		},
		{
			name: "missing storage type",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue},
			},
			wantErr: "storage.type must be provided",
		},
		{
			name: "gcs not yet supported",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue},
				Storage: config.IcebergStorage{Type: config.IcebergStorageType("gcs"), Path: "gs://b/p"},
			},
			wantErr: `unsupported storage type "gcs"`,
		},
		{
			name: "path and bucket are mutually exclusive",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue},
				Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: "s3://b/p", Bucket: "b"},
			},
			wantErr: `set either "path"`,
		},
		{
			name: "prefix requires bucket",
			config: Config{
				Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue},
				Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Prefix: "warehouse"},
			},
			wantErr: `"prefix" requires "bucket"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := tt.config.GetIngestrURI()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{
		Catalog: config.IcebergCatalog{Type: config.IcebergCatalogGlue, Region: testAWSRegion},
		Storage: config.IcebergStorage{Type: config.IcebergStorageS3, Path: "s3://lake/wh"},
	})
	require.NoError(t, err)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(uri, "iceberg+glue://?"), "got: %s", uri)
}
