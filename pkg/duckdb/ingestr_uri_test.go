package duck

import (
	"net/url"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/assert"
)

func parseQuery(t *testing.T, raw string) (string, url.Values) {
	t.Helper()
	u, err := url.Parse(raw)
	assert.NoError(t, err)
	return u.Scheme, u.Query()
}

func TestBuildIngestrLakehouseURI_NilOrZero(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "", BuildIngestrLakehouseURI(nil))
	assert.Equal(t, "", BuildIngestrLakehouseURI(&config.LakehouseConfig{}))
}

func TestBuildIngestrLakehouseURI_NonDuckLakeReturnsEmpty(t *testing.T) {
	t.Parallel()

	lh := &config.LakehouseConfig{Format: config.LakehouseFormatIceberg}
	assert.Equal(t, "", BuildIngestrLakehouseURI(lh))
}

func TestBuildIngestrLakehouseURI_DuckDBCatalogS3Storage_MinIO(t *testing.T) {
	t.Parallel()

	useSSL := false
	lh := &config.LakehouseConfig{
		Format: config.LakehouseFormatDuckLake,
		Catalog: config.CatalogConfig{
			Type: config.CatalogTypeDuckDB,
			Path: "/tmp/metadata.duckdb",
		},
		Storage: config.StorageConfig{
			Type:     config.StorageTypeS3,
			Path:     "s3://ducklake/warehouse",
			Endpoint: "minio.local:9000",
			URLStyle: "path",
			UseSSL:   &useSSL,
			Auth: config.StorageAuth{
				AccessKey: "AKID",
				SecretKey: "SECRET",
			},
		},
	}

	got := BuildIngestrLakehouseURI(lh)
	scheme, q := parseQuery(t, got)

	assert.Equal(t, "ducklake", scheme)
	assert.Empty(t, q.Get("alias"), "alias must not appear on the URI")
	assert.Equal(t, "duckdb", q.Get("catalog_type"))
	assert.Equal(t, "/tmp/metadata.duckdb", q.Get("catalog_path"))
	assert.Equal(t, "s3", q.Get("storage_type"))
	assert.Equal(t, "s3://ducklake/warehouse", q.Get("storage_path"))
	assert.Equal(t, "minio.local:9000", q.Get("storage_endpoint"))
	assert.Equal(t, "path", q.Get("storage_url_style"))
	assert.Equal(t, "false", q.Get("storage_use_ssl"))
	assert.Equal(t, "AKID", q.Get("storage_access_key"))
	assert.Equal(t, "SECRET", q.Get("storage_secret_key"))
}

func TestBuildIngestrLakehouseURI_PostgresCatalog(t *testing.T) {
	t.Parallel()

	lh := &config.LakehouseConfig{
		Format: config.LakehouseFormatDuckLake,
		Catalog: config.CatalogConfig{
			Type:     config.CatalogTypePostgres,
			Host:     "metastore.internal",
			Port:     5432,
			Database: "ducklake_meta",
			Auth: config.CatalogAuth{
				Username: "lake_user",
				Password: "lake_pass",
			},
		},
		Storage: config.StorageConfig{
			Type: config.StorageTypeS3,
			Path: "s3://bucket/lake",
			Auth: config.StorageAuth{AccessKey: "AKID", SecretKey: "SECRET"},
		},
	}

	got := BuildIngestrLakehouseURI(lh)
	_, q := parseQuery(t, got)

	assert.Equal(t, "postgres", q.Get("catalog_type"))
	assert.Equal(t, "metastore.internal", q.Get("catalog_host"))
	assert.Equal(t, "5432", q.Get("catalog_port"))
	assert.Equal(t, "ducklake_meta", q.Get("catalog_database"))
	assert.Equal(t, "lake_user", q.Get("catalog_username"))
	assert.Equal(t, "lake_pass", q.Get("catalog_password"))
}

func TestBuildIngestrLakehouseURI_UseSSLUnsetOmitted(t *testing.T) {
	t.Parallel()

	lh := &config.LakehouseConfig{
		Format: config.LakehouseFormatDuckLake,
		Catalog: config.CatalogConfig{
			Type: config.CatalogTypeDuckDB,
			Path: "/tmp/m.duckdb",
		},
		Storage: config.StorageConfig{
			Type: config.StorageTypeS3,
			Path: "s3://b/p",
			Auth: config.StorageAuth{AccessKey: "a", SecretKey: "b"},
		},
	}

	_, q := parseQuery(t, BuildIngestrLakehouseURI(lh))
	_, present := q["storage_use_ssl"]
	assert.False(t, present, "storage_use_ssl should be omitted when UseSSL is nil")
}

func TestConfig_GetIngestrURI_WithLakehouse(t *testing.T) {
	t.Parallel()

	useSSL := false
	c := Config{
		Path: "/scratch/duck.db",
		Lakehouse: &config.LakehouseConfig{
			Format: config.LakehouseFormatDuckLake,
			Catalog: config.CatalogConfig{
				Type: config.CatalogTypeDuckDB,
				Path: "/tmp/metadata.duckdb",
			},
			Storage: config.StorageConfig{
				Type:     config.StorageTypeS3,
				Path:     "s3://ducklake/warehouse",
				Endpoint: "minio.local:9000",
				URLStyle: "path",
				UseSSL:   &useSSL,
				Auth: config.StorageAuth{
					AccessKey: "AKID",
					SecretKey: "SECRET",
				},
			},
		},
	}

	scheme, q := parseQuery(t, c.GetIngestrURI())

	assert.Equal(t, "ducklake", scheme)
	assert.Empty(t, q.Get("alias"), "alias must not appear on the URI")
	assert.Equal(t, "duckdb", q.Get("catalog_type"))
	assert.Equal(t, "/tmp/metadata.duckdb", q.Get("catalog_path"))
	assert.Equal(t, "minio.local:9000", q.Get("storage_endpoint"))
}

func TestConfig_GetIngestrURI_NoLakehouse_UnchangedBehavior(t *testing.T) {
	t.Parallel()

	c := Config{Path: "/some/path/db.duckdb"}
	assert.Equal(t, "duckdb:////some/path/db.duckdb", c.GetIngestrURI())
}

