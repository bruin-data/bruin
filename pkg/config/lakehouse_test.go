package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLakehouseConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		lh      *LakehouseConfig
		wantErr bool
		errMsg  string
	}{
		{
			name:    "empty format returns error",
			lh:      &LakehouseConfig{},
			wantErr: true,
			errMsg:  "lakehouse format is required",
		},
		{
			name: "unsupported format returns error",
			lh: &LakehouseConfig{
				Format: LakehouseFormat("unsupported"),
			},
			wantErr: true,
			errMsg:  "unsupported lakehouse format",
		},
		{
			name: "iceberg format is valid",
			lh: &LakehouseConfig{
				Format: LakehouseFormatIceberg,
			},
			wantErr: false,
		},
		{
			name: "ducklake format is valid",
			lh: &LakehouseConfig{
				Format: LakehouseFormatDuckLake,
			},
			wantErr: false,
		},
		{
			name: "valid catalog type passes",
			lh: &LakehouseConfig{
				Format: LakehouseFormatIceberg,
				Catalog: &CatalogConfig{
					Type: CatalogTypeGlue,
				},
			},
			wantErr: false,
		},
		{
			name: "valid postgres catalog type passes",
			lh: &LakehouseConfig{
				Format: LakehouseFormatDuckLake,
				Catalog: &CatalogConfig{
					Type: CatalogTypePostgres,
				},
			},
			wantErr: false,
		},
		{
			name: "valid duckdb catalog type passes",
			lh: &LakehouseConfig{
				Format: LakehouseFormatDuckLake,
				Catalog: &CatalogConfig{
					Type: CatalogTypeDuckDB,
				},
			},
			wantErr: false,
		},
		{
			name: "valid sqlite catalog type passes",
			lh: &LakehouseConfig{
				Format: LakehouseFormatDuckLake,
				Catalog: &CatalogConfig{
					Type: CatalogTypeSQLite,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid catalog type fails",
			lh: &LakehouseConfig{
				Format: LakehouseFormatIceberg,
				Catalog: &CatalogConfig{
					Type: CatalogType("invalid"),
				},
			},
			wantErr: true,
			errMsg:  "unsupported catalog type",
		},
		{
			name: "valid storage type passes",
			lh: &LakehouseConfig{
				Format: LakehouseFormatIceberg,
				Storage: &StorageConfig{
					Type: StorageTypeS3,
				},
			},
			wantErr: false,
		},
		{
			name: "invalid storage type fails",
			lh: &LakehouseConfig{
				Format: LakehouseFormatIceberg,
				Storage: &StorageConfig{
					Type: StorageType("invalid"),
				},
			},
			wantErr: true,
			errMsg:  "unsupported storage type",
		},
		{
			name: "empty catalog type is valid (engine-specific validation)",
			lh: &LakehouseConfig{
				Format:  LakehouseFormatIceberg,
				Catalog: &CatalogConfig{},
			},
			wantErr: false,
		},
		{
			name: "full config is valid",
			lh: &LakehouseConfig{
				Format: LakehouseFormatIceberg,
				Catalog: &CatalogConfig{
					Type:      CatalogTypeGlue,
					CatalogID: "123456789012",
					Region:    "us-east-1",
				},
				Storage: &StorageConfig{
					Type: StorageTypeS3,
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := tt.lh.Validate()

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestTypeConstants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		got  any
		want any
	}{
		{"lakehouse format", LakehouseFormat("iceberg"), LakehouseFormatIceberg},
		{"lakehouse format ducklake", LakehouseFormat("ducklake"), LakehouseFormatDuckLake},
		{"catalog type", CatalogType("glue"), CatalogTypeGlue},
		{"catalog type postgres", CatalogType("postgres"), CatalogTypePostgres},
		{"catalog type duckdb", CatalogType("duckdb"), CatalogTypeDuckDB},
		{"catalog type sqlite", CatalogType("sqlite"), CatalogTypeSQLite},
		{"storage type", StorageType("s3"), StorageTypeS3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestCatalogAuth_IsAWS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth *CatalogAuth
		want bool
	}{
		{"nil auth", nil, false},
		{"empty auth", &CatalogAuth{}, false},
		{"with access and secret key", &CatalogAuth{AccessKey: "AKIA...", SecretKey: "secret"}, true},
		{"only access key", &CatalogAuth{AccessKey: "AKIA..."}, false},
		{"only secret key", &CatalogAuth{SecretKey: "secret"}, false},
		{"postgres creds do not count as aws", &CatalogAuth{Username: "user", Password: "pass"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.auth.IsAWS())
		})
	}
}

func TestCatalogAuth_IsPostgres(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth *CatalogAuth
		want bool
	}{
		{"nil auth", nil, false},
		{"empty auth", &CatalogAuth{}, false},
		{"with username and password", &CatalogAuth{Username: "user", Password: "pass"}, true},
		{"only username", &CatalogAuth{Username: "user"}, false},
		{"only password", &CatalogAuth{Password: "pass"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.auth.IsPostgres())
		})
	}
}

func TestStorageAuth_IsS3(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		auth *StorageAuth
		want bool
	}{
		{"nil auth", nil, false},
		{"empty auth", &StorageAuth{}, false},
		{"with access and secret key", &StorageAuth{AccessKey: "AKIA...", SecretKey: "secret"}, true},
		{"only access key", &StorageAuth{AccessKey: "AKIA..."}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.auth.IsS3())
		})
	}
}
