package config

import (
	"errors"
	"fmt"
	"slices"
)

type LakehouseFormat string

const (
	LakehouseFormatIceberg  LakehouseFormat = "iceberg"
	LakehouseFormatDuckLake LakehouseFormat = "ducklake"
	// Future: LakehouseFormatDelta.
)

type CatalogType string

const (
	CatalogTypeGlue CatalogType = "glue"

	// DuckLake Specific.
	CatalogTypePostgres CatalogType = "postgres"
	CatalogTypeDuckDB   CatalogType = "duckdb"
	CatalogTypeSQLite   CatalogType = "sqlite"
	// Future: CatalogTypeRest.
)

type CatalogAuth struct {
	// AWS credentials (for Glue)
	AccessKey    string `yaml:"access_key,omitempty" json:"access_key,omitempty" mapstructure:"access_key"`
	SecretKey    string `yaml:"secret_key,omitempty" json:"secret_key,omitempty" mapstructure:"secret_key"`
	SessionToken string `yaml:"session_token,omitempty" json:"session_token,omitempty" mapstructure:"session_token"`

	// Postgres credentials (for DuckLake)
	Username string `yaml:"username,omitempty" json:"username,omitempty" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password,omitempty" mapstructure:"password"`
}

func (auth *CatalogAuth) IsAWS() bool {
	if auth == nil {
		return false
	}
	return auth.AccessKey != "" && auth.SecretKey != ""
}

func (auth *CatalogAuth) IsPostgres() bool {
	if auth == nil {
		return false
	}
	return auth.Username != "" && auth.Password != ""
}

type CatalogConfig struct {
	Type CatalogType `yaml:"type" json:"type" mapstructure:"type"`

	// Glue-specific
	CatalogID string `yaml:"catalog_id,omitempty" json:"catalog_id,omitempty" mapstructure:"catalog_id"`
	Region    string `yaml:"region,omitempty" json:"region,omitempty" mapstructure:"region"`

	// DuckDB and SQLite-specific
	Path string `yaml:"path,omitempty" json:"path,omitempty" mapstructure:"path"`

	// Postgres-specific
	Host     string `yaml:"host,omitempty" json:"host,omitempty" mapstructure:"host"`
	Port     int    `yaml:"port,omitempty" json:"port,omitempty" mapstructure:"port"`
	Database string `yaml:"database,omitempty" json:"database,omitempty" mapstructure:"database"`

	// Authentication
	Auth *CatalogAuth `yaml:"auth,omitempty" json:"auth,omitempty" mapstructure:"auth"`
}

type StorageType string

const (
	StorageTypeS3 StorageType = "s3"
	// Future: StorageTypeGCS, StorageTypeLocal.
)

var (
	supportedCatalogTypes = []CatalogType{
		CatalogTypeGlue,
		CatalogTypePostgres,
		CatalogTypeDuckDB,
		CatalogTypeSQLite,
	}
	supportedStorageTypes = []StorageType{
		StorageTypeS3,
	}
)

type StorageAuth struct {
	// AWS S3 credentials
	AccessKey    string `yaml:"access_key,omitempty" json:"access_key,omitempty" mapstructure:"access_key"`
	SecretKey    string `yaml:"secret_key,omitempty" json:"secret_key,omitempty" mapstructure:"secret_key"`
	SessionToken string `yaml:"session_token,omitempty" json:"session_token,omitempty" mapstructure:"session_token"`
}

func (a *StorageAuth) IsS3() bool {
	if a == nil {
		return false
	}
	return a.AccessKey != "" && a.SecretKey != ""
}

type StorageConfig struct {
	Type   StorageType  `yaml:"type" json:"type" mapstructure:"type"`
	Path   string       `yaml:"path,omitempty" json:"path,omitempty" mapstructure:"path"`
	Region string       `yaml:"region,omitempty" json:"region,omitempty" mapstructure:"region"`
	Auth   *StorageAuth `yaml:"auth,omitempty" json:"auth,omitempty" mapstructure:"auth"`
}

type LakehouseConfig struct {
	Format  LakehouseFormat `yaml:"format" json:"format" mapstructure:"format"`
	Catalog *CatalogConfig  `yaml:"catalog,omitempty" json:"catalog,omitempty" mapstructure:"catalog"`
	Storage *StorageConfig  `yaml:"storage,omitempty" json:"storage,omitempty" mapstructure:"storage"`
}

// Validate performs basic structural validation of the LakehouseConfig (engine-agnostic).
func (lh *LakehouseConfig) Validate() error {
	if lh.Format == "" {
		return errors.New("lakehouse format is required")
	}

	switch lh.Format {
	case LakehouseFormatIceberg:
		// valid format
	case LakehouseFormatDuckLake:
		// valid format
	default:
		return fmt.Errorf("unsupported lakehouse format: %s (supported: iceberg, ducklake)", lh.Format)
	}

	// Validate catalog type if specified
	if lh.Catalog != nil && lh.Catalog.Type != "" && !slices.Contains(supportedCatalogTypes, lh.Catalog.Type) {
		return fmt.Errorf("unsupported catalog type: %s (supported: glue, postgres, duckdb, sqlite)", lh.Catalog.Type)
	}

	// Validate storage type if specified
	if lh.Storage != nil && lh.Storage.Type != "" && !slices.Contains(supportedStorageTypes, lh.Storage.Type) {
		return fmt.Errorf("unsupported storage type: %s (supported: s3)", lh.Storage.Type)
	}

	return nil
}
