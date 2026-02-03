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
					Type:     StorageTypeS3,
					Location: "s3://my-bucket/warehouse",
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

func TestLakehouseFormat_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, LakehouseFormatIceberg, LakehouseFormat("iceberg"))
}

func TestCatalogType_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, CatalogTypeGlue, CatalogType("glue"))
}

func TestStorageType_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, StorageTypeS3, StorageType("s3"))
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.auth.IsAWS())
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

func TestCatalogAuth_Fields(t *testing.T) {
	t.Parallel()

	auth := CatalogAuth{
		AccessKey:    "access",
		SecretKey:    "secret",
		SessionToken: "session",
	}

	assert.Equal(t, "access", auth.AccessKey)
	assert.Equal(t, "secret", auth.SecretKey)
	assert.Equal(t, "session", auth.SessionToken)
}

func TestStorageAuth_Fields(t *testing.T) {
	t.Parallel()

	auth := StorageAuth{
		AccessKey:    "access",
		SecretKey:    "secret",
		SessionToken: "session",
	}

	assert.Equal(t, "access", auth.AccessKey)
	assert.Equal(t, "secret", auth.SecretKey)
	assert.Equal(t, "session", auth.SessionToken)
}

func TestCatalogConfig_Fields(t *testing.T) {
	t.Parallel()

	catalog := CatalogConfig{
		Type:      CatalogTypeGlue,
		CatalogID: "123456789012",
		Region:    "us-east-1",
		Auth: &CatalogAuth{
			AccessKey: "AKIA...",
			SecretKey: "secret",
		},
	}

	assert.Equal(t, CatalogTypeGlue, catalog.Type)
	assert.Equal(t, "123456789012", catalog.CatalogID)
	assert.Equal(t, "us-east-1", catalog.Region)
	assert.NotNil(t, catalog.Auth)
	assert.Equal(t, "AKIA...", catalog.Auth.AccessKey)
}

func TestStorageConfig_Fields(t *testing.T) {
	t.Parallel()

	storage := StorageConfig{
		Type:     StorageTypeS3,
		Location: "s3://my-bucket/warehouse",
		Region:   "us-west-2",
		Auth: &StorageAuth{
			AccessKey: "AKIAEXAMPLE",
			SecretKey: "secretkey",
		},
	}

	assert.Equal(t, StorageTypeS3, storage.Type)
	assert.Equal(t, "s3://my-bucket/warehouse", storage.Location)
	assert.Equal(t, "us-west-2", storage.Region)
	assert.NotNil(t, storage.Auth)
	assert.Equal(t, "AKIAEXAMPLE", storage.Auth.AccessKey)
}

func TestLakehouseConfig_Fields(t *testing.T) {
	t.Parallel()

	lh := LakehouseConfig{
		Format: LakehouseFormatIceberg,
		Catalog: &CatalogConfig{
			Type:      CatalogTypeGlue,
			CatalogID: "123456789012",
		},
		Storage: &StorageConfig{
			Type:     StorageTypeS3,
			Location: "s3://my-bucket/warehouse",
		},
	}

	assert.Equal(t, LakehouseFormatIceberg, lh.Format)
	assert.NotNil(t, lh.Catalog)
	assert.Equal(t, CatalogTypeGlue, lh.Catalog.Type)
	assert.NotNil(t, lh.Storage)
	assert.Equal(t, StorageTypeS3, lh.Storage.Type)
}
