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
		{"catalog type", CatalogType("glue"), CatalogTypeGlue},
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

func TestStructFieldAssignments(t *testing.T) {
	t.Parallel()

	catalogAuth := CatalogAuth{
		AccessKey:    "access",
		SecretKey:    "secret",
		SessionToken: "session",
	}
	storageAuth := StorageAuth{
		AccessKey:    "access",
		SecretKey:    "secret",
		SessionToken: "session",
	}
	catalog := CatalogConfig{
		Type:      CatalogTypeGlue,
		CatalogID: "123456789012",
		Region:    "us-east-1",
		Auth: &CatalogAuth{
			AccessKey: "AKIA...",
			SecretKey: "secret",
		},
	}
	storage := StorageConfig{
		Type:   StorageTypeS3,
		Region: "us-west-2",
		Auth: &StorageAuth{
			AccessKey: "AKIAEXAMPLE",
			SecretKey: "secretkey",
		},
	}
	lh := LakehouseConfig{
		Format: LakehouseFormatIceberg,
		Catalog: &CatalogConfig{
			Type:      CatalogTypeGlue,
			CatalogID: "123456789012",
		},
		Storage: &StorageConfig{
			Type: StorageTypeS3,
		},
	}

	tests := []struct {
		name string
		got  any
		want any
	}{
		{"catalog auth access key", catalogAuth.AccessKey, "access"},
		{"catalog auth secret key", catalogAuth.SecretKey, "secret"},
		{"catalog auth session token", catalogAuth.SessionToken, "session"},
		{"storage auth access key", storageAuth.AccessKey, "access"},
		{"storage auth secret key", storageAuth.SecretKey, "secret"},
		{"storage auth session token", storageAuth.SessionToken, "session"},
		{"catalog type", catalog.Type, CatalogTypeGlue},
		{"catalog id", catalog.CatalogID, "123456789012"},
		{"catalog region", catalog.Region, "us-east-1"},
		{"catalog auth present", catalog.Auth != nil, true},
		{"catalog auth access key", catalog.Auth.AccessKey, "AKIA..."},
		{"storage type", storage.Type, StorageTypeS3},
		{"storage region", storage.Region, "us-west-2"},
		{"storage auth present", storage.Auth != nil, true},
		{"storage auth access key", storage.Auth.AccessKey, "AKIAEXAMPLE"},
		{"lakehouse format", lh.Format, LakehouseFormatIceberg},
		{"lakehouse catalog present", lh.Catalog != nil, true},
		{"lakehouse catalog type", lh.Catalog.Type, CatalogTypeGlue},
		{"lakehouse storage present", lh.Storage != nil, true},
		{"lakehouse storage type", lh.Storage.Type, StorageTypeS3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.got)
		})
	}
}
