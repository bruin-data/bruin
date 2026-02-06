package duck

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateLakehouseConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		lh      *config.LakehouseConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil config is valid",
			lh:      nil,
			wantErr: false,
		},
		{
			name:    "empty format fails generic validation",
			lh:      &config.LakehouseConfig{},
			wantErr: true,
			errMsg:  "lakehouse format is required",
		},
		// Iceberg validation
		{
			name: "iceberg without catalog fails",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
			},
			wantErr: true,
			errMsg:  "DuckDB iceberg requires catalog configuration",
		},
		{
			name: "iceberg with empty catalog type fails",
			lh: &config.LakehouseConfig{
				Format:  config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{},
			},
			wantErr: true,
			errMsg:  "DuckDB iceberg requires catalog type",
		},
		{
			name: "iceberg with glue but no catalog_id fails",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{
					Type: config.CatalogTypeGlue,
				},
			},
			wantErr: true,
			errMsg:  "DuckDB iceberg with glue catalog requires catalog_id",
		},
		{
			name: "iceberg with glue and catalog_id passes",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{
					Type:      config.CatalogTypeGlue,
					CatalogID: "123456789012",
				},
			},
			wantErr: false,
		},
		{
			name: "iceberg with unsupported catalog type fails",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{
					Type: config.CatalogType("rest"),
				},
			},
			wantErr: true,
			errMsg:  "unsupported catalog type",
		},
		// DuckLake validation
		{
			name: "ducklake without catalog fails",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatDuckLake,
			},
			wantErr: true,
			errMsg:  "DuckDB ducklake requires catalog configuration",
		},
		{
			name: "ducklake without storage fails",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatDuckLake,
				Catalog: &config.CatalogConfig{
					Type:     config.CatalogTypePostgres,
					Host:     "localhost",
					Database: "ducklake_catalog",
					Auth: &config.CatalogAuth{
						Username: "ducklake_user",
						Password: "ducklake_password",
					},
				},
			},
			wantErr: true,
			errMsg:  "DuckDB ducklake requires storage configuration",
		},
		{
			name: "ducklake with postgres missing credentials fails",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatDuckLake,
				Catalog: &config.CatalogConfig{
					Type:     config.CatalogTypePostgres,
					Host:     "localhost",
					Database: "ducklake_catalog",
				},
				Storage: &config.StorageConfig{
					Type: config.StorageTypeS3,
					Path: "s3://ducklake/warehouse",
				},
			},
			wantErr: true,
			errMsg:  "DuckDB ducklake with postgres catalog requires username and password",
		},
		{
			name: "ducklake with s3 storage missing path fails",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatDuckLake,
				Catalog: &config.CatalogConfig{
					Type:     config.CatalogTypePostgres,
					Host:     "localhost",
					Database: "ducklake_catalog",
					Auth: &config.CatalogAuth{
						Username: "ducklake_user",
						Password: "ducklake_password",
					},
				},
				Storage: &config.StorageConfig{
					Type: config.StorageTypeS3,
				},
			},
			wantErr: true,
			errMsg:  "DuckDB ducklake with s3 storage requires path",
		},
		{
			name: "ducklake with postgres and s3 storage passes",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatDuckLake,
				Catalog: &config.CatalogConfig{
					Type:     config.CatalogTypePostgres,
					Host:     "localhost",
					Port:     5432,
					Database: "ducklake_catalog",
					Auth: &config.CatalogAuth{
						Username: "ducklake_user",
						Password: "ducklake_password",
					},
				},
				Storage: &config.StorageConfig{
					Type: config.StorageTypeS3,
					Path: "s3://ducklake/warehouse",
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateLakehouseConfig(tt.lh)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLakehouseAttacher_GetRequiredExtensions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		lh       *config.LakehouseConfig
		wantExts []string
	}{
		{
			name: "iceberg with glue and s3",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{
					Type: config.CatalogTypeGlue,
				},
				Storage: &config.StorageConfig{
					Type: config.StorageTypeS3,
				},
			},
			wantExts: []string{"iceberg", "aws", "httpfs"},
		},
		{
			name: "iceberg with glue only (no storage)",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{
					Type: config.CatalogTypeGlue,
				},
			},
			wantExts: []string{"iceberg", "aws"},
		},
		{
			name: "ducklake with postgres and s3",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatDuckLake,
				Catalog: &config.CatalogConfig{
					Type: config.CatalogTypePostgres,
				},
				Storage: &config.StorageConfig{
					Type: config.StorageTypeS3,
				},
			},
			wantExts: []string{"ducklake", "postgres", "aws", "httpfs"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			attacher := NewLakehouseAttacher()
			extensions := attacher.getRequiredExtensions(tt.lh)

			for _, ext := range tt.wantExts {
				assert.Contains(t, extensions, ext)
			}
		})
	}
}

func TestLakehouseAttacher_GenerateS3Secret(t *testing.T) {
	t.Parallel()
	attacher := NewLakehouseAttacher()

	tests := []struct {
		name    string
		storage *config.StorageConfig
		want    string
	}{
		{
			name: "s3 with access key and secret key",
			storage: &config.StorageConfig{
				Type:   config.StorageTypeS3,
				Region: "us-east-1",
				Auth: &config.StorageAuth{
					AccessKey: "AKIAIOSFODNN7EXAMPLE",
					SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				},
			},
			want: `CREATE OR REPLACE SECRET test_secret (
    TYPE s3
,   PROVIDER config
,   KEY_ID 'AKIAIOSFODNN7EXAMPLE'
,   SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
,   REGION 'us-east-1'
,   SCOPE 's3://'
)`,
		},
		{
			name: "s3 with session token",
			storage: &config.StorageConfig{
				Type:   config.StorageTypeS3,
				Region: "us-west-2",
				Auth: &config.StorageAuth{
					AccessKey:    "AKIAIOSFODNN7EXAMPLE",
					SecretKey:    "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					SessionToken: "FwoGZXIvYXdzEBYaDPe...",
				},
			},
			want: `CREATE OR REPLACE SECRET test_secret (
    TYPE s3
,   PROVIDER config
,   KEY_ID 'AKIAIOSFODNN7EXAMPLE'
,   SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
,   SESSION_TOKEN 'FwoGZXIvYXdzEBYaDPe...'
,   REGION 'us-west-2'
,   SCOPE 's3://'
)`,
		},
		{
			name: "s3 with scope path",
			storage: &config.StorageConfig{
				Type: config.StorageTypeS3,
				Path: "s3://ducklake/warehouse",
				Auth: &config.StorageAuth{
					AccessKey: "AKIAIOSFODNN7EXAMPLE",
					SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
				},
			},
			want: `CREATE OR REPLACE SECRET test_secret (
    TYPE s3
,   PROVIDER config
,   KEY_ID 'AKIAIOSFODNN7EXAMPLE'
,   SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
,   SCOPE 's3://ducklake/warehouse'
)`,
		},
		{
			name: "s3 without credentials returns empty",
			storage: &config.StorageConfig{
				Type: config.StorageTypeS3,
				Auth: &config.StorageAuth{},
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := attacher.generateS3Secret("test_secret", tt.storage)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestLakehouseAttacher_GeneratePostgresSecret(t *testing.T) {
	t.Parallel()
	attacher := NewLakehouseAttacher()

	catalog := &config.CatalogConfig{
		Type:     config.CatalogTypePostgres,
		Host:     "localhost",
		Port:     5432,
		Database: "ducklake_catalog",
		Auth: &config.CatalogAuth{
			Username: "ducklake_user",
			Password: "ducklake_password",
		},
	}

	want := `CREATE OR REPLACE SECRET test_secret (
    TYPE postgres
,   HOST 'localhost'
,   PORT 5432
,   DATABASE 'ducklake_catalog'
,   USER 'ducklake_user'
,   PASSWORD 'ducklake_password'
)`

	result := attacher.generateCatalogSecret("test_secret", catalog)
	assert.Equal(t, want, result)
}

func TestLakehouseAttacher_GenerateGlueSecret(t *testing.T) {
	t.Parallel()
	attacher := NewLakehouseAttacher()

	catalog := &config.CatalogConfig{
		Type:   config.CatalogTypeGlue,
		Region: "us-east-1",
		Auth: &config.CatalogAuth{
			AccessKey: "AKIAIOSFODNN7EXAMPLE",
			SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		},
	}

	want := `CREATE OR REPLACE SECRET test_secret (
    TYPE s3
,   PROVIDER config
,   KEY_ID 'AKIAIOSFODNN7EXAMPLE'
,   SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
,   REGION 'us-east-1'
)`

	result := attacher.generateGlueSecret("test_secret", catalog)
	assert.Equal(t, want, result)
}

func TestLakehouseAttacher_GenerateIcebergAttach(t *testing.T) {
	t.Parallel()
	attacher := NewLakehouseAttacher()

	tests := []struct {
		name    string
		lh      *config.LakehouseConfig
		alias   string
		want    string
		wantErr bool
	}{
		{
			name: "iceberg with glue catalog",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{
					Type:      config.CatalogTypeGlue,
					CatalogID: "123456789012",
					Region:    "us-east-1",
				},
			},
			alias: "my_iceberg",
			want:  "ATTACH '123456789012' AS my_iceberg (TYPE 'iceberg', ENDPOINT_TYPE 'glue')",
		},
		{
			name: "iceberg with glue catalog no region",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{
					Type:      config.CatalogTypeGlue,
					CatalogID: "123456789012",
				},
			},
			alias: "glue_catalog",
			want:  "ATTACH '123456789012' AS glue_catalog (TYPE 'iceberg', ENDPOINT_TYPE 'glue')",
		},
		{
			name: "iceberg without catalog returns error",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
			},
			alias:   "no_catalog",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := attacher.generateIcebergAttach(tt.lh, tt.alias)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestLakehouseAttacher_GenerateDuckLakeAttach(t *testing.T) {
	t.Parallel()
	attacher := NewLakehouseAttacher()

	lh := &config.LakehouseConfig{
		Format: config.LakehouseFormatDuckLake,
		Catalog: &config.CatalogConfig{
			Type:     config.CatalogTypePostgres,
			Host:     "localhost",
			Database: "ducklake_catalog",
			Auth: &config.CatalogAuth{
				Username: "ducklake_user",
				Password: "ducklake_password",
			},
		},
		Storage: &config.StorageConfig{
			Type: config.StorageTypeS3,
			Path: "s3://ducklake/warehouse",
		},
	}

	result, err := attacher.generateDuckLakeAttach(lh, "ducklake_catalog")
	require.NoError(t, err)
	assert.Equal(t, "ATTACH 'ducklake:postgres:' AS ducklake_catalog (DATA_PATH 's3://ducklake/warehouse', META_SECRET 'bruin_ducklake_catalog_catalog', OVERRIDE_DATA_PATH true)", result)
}

func TestLakehouseAttacher_GenerateAttachStatements(t *testing.T) {
	t.Parallel()
	attacher := NewLakehouseAttacher()

	tests := []struct {
		name         string
		lh           *config.LakehouseConfig
		alias        string
		wantContains []string
		wantMinLen   int
		wantNil      bool
		wantErr      bool
	}{
		{
			name:    "nil config returns nil statements",
			lh:      nil,
			alias:   "test_alias",
			wantNil: true,
		},
		{
			name: "iceberg with glue, s3 storage and credentials",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{
					Type:      config.CatalogTypeGlue,
					CatalogID: "123456789012",
					Region:    "us-east-1",
				},
				Storage: &config.StorageConfig{
					Type:   config.StorageTypeS3,
					Region: "us-east-1",
					Auth: &config.StorageAuth{
						AccessKey: "AKIAEXAMPLE",
						SecretKey: "secretkey",
					},
				},
			},
			alias: "iceberg_catalog",
			wantContains: []string{
				"INSTALL iceberg",
				"LOAD iceberg",
				"INSTALL aws",
				"LOAD aws",
				"CREATE OR REPLACE SECRET",
				"PROVIDER config",
				"ATTACH '123456789012' AS iceberg_catalog",
				"CREATE SCHEMA IF NOT EXISTS iceberg_catalog.main",
				"USE iceberg_catalog",
				"TYPE 'iceberg'",
				"ENDPOINT_TYPE 'glue'",
			},
			wantMinLen: 7, // includes schema + use
		},
		{
			name: "iceberg with glue only (no storage auth)",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatIceberg,
				Catalog: &config.CatalogConfig{
					Type:      config.CatalogTypeGlue,
					CatalogID: "123456789012",
					Region:    "us-east-1",
				},
			},
			alias: "glue_only",
			wantContains: []string{
				"INSTALL iceberg",
				"LOAD iceberg",
				"INSTALL aws",
				"LOAD aws",
				"ATTACH '123456789012' AS glue_only",
				"CREATE SCHEMA IF NOT EXISTS glue_only.main",
				"USE glue_only",
			},
			wantMinLen: 7,
		},
		{
			name: "ducklake with postgres and s3 storage",
			lh: &config.LakehouseConfig{
				Format: config.LakehouseFormatDuckLake,
				Catalog: &config.CatalogConfig{
					Type:     config.CatalogTypePostgres,
					Host:     "localhost",
					Database: "ducklake_catalog",
					Auth: &config.CatalogAuth{
						Username: "ducklake_user",
						Password: "ducklake_password",
					},
				},
				Storage: &config.StorageConfig{
					Type:   config.StorageTypeS3,
					Path:   "s3://ducklake/warehouse",
					Region: "us-east-1",
					Auth: &config.StorageAuth{
						AccessKey: "AKIAEXAMPLE",
						SecretKey: "secretkey",
					},
				},
			},
			alias: "ducklake_catalog",
			wantContains: []string{
				"INSTALL ducklake",
				"LOAD ducklake",
				"INSTALL postgres",
				"LOAD postgres",
				"INSTALL aws",
				"LOAD aws",
				"CREATE OR REPLACE SECRET",
				"TYPE postgres",
				"TYPE s3",
				"ATTACH 'ducklake:postgres:' AS ducklake_catalog",
				"DATA_PATH 's3://ducklake/warehouse'",
				"META_SECRET 'bruin_ducklake_catalog_catalog'",
				"OVERRIDE_DATA_PATH true",
				"CREATE SCHEMA IF NOT EXISTS ducklake_catalog.main",
				"USE ducklake_catalog",
			},
			wantMinLen: 9,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			statements, err := attacher.GenerateAttachStatements(tt.lh, tt.alias)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			if tt.wantNil {
				assert.Nil(t, statements)
				return
			}

			assert.GreaterOrEqual(t, len(statements), tt.wantMinLen)

			// Join all statements to check for expected content
			allStatements := ""
			for _, s := range statements {
				allStatements += s + "\n"
			}

			for _, want := range tt.wantContains {
				assert.Contains(t, allStatements, want, "Expected statement to contain: %s", want)
			}
		})
	}
}

func TestEscapeSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"hello", "hello"},
		{"it's", "it''s"},
		{"test''double", "test''''double"},
		{"path/to/file", "path/to/file"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, escapeSQL(tt.input))
		})
	}
}

func TestDollarQuote(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"hello", "'hello'"},
		{"it's", "'it''s'"},
		{"token?with?questions", "'token?with?questions'"},
		{"FwoGZXIvYXdzEBYaDPe?abc", "'FwoGZXIvYXdzEBYaDPe?abc'"},
		{"contains delimiter", "'contains delimiter'"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, dollarQuote(tt.input))
		})
	}
}

func TestConfig_HasLakehouse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want bool
	}{
		{
			name: "with lakehouse",
			cfg: Config{
				Path: "/path/to/db",
				Lakehouse: &config.LakehouseConfig{
					Format: config.LakehouseFormatIceberg,
				},
			},
			want: true,
		},
		{
			name: "without lakehouse",
			cfg: Config{
				Path: "/path/to/db",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.cfg.HasLakehouse())
		})
	}
}

func TestConfig_GetLakehouseAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "iceberg format",
			cfg: Config{
				Lakehouse: &config.LakehouseConfig{
					Format: config.LakehouseFormatIceberg,
				},
			},
			want: "iceberg_catalog",
		},
		{
			name: "ducklake format",
			cfg: Config{
				Lakehouse: &config.LakehouseConfig{
					Format: config.LakehouseFormatDuckLake,
				},
			},
			want: "ducklake_catalog",
		},
		{
			name: "nil lakehouse",
			cfg:  Config{},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.cfg.GetLakehouseAlias())
		})
	}
}
