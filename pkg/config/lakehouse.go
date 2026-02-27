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

func (auth CatalogAuth) IsAWS() bool {
	return auth.AccessKey != "" && auth.SecretKey != ""
}

func (auth CatalogAuth) IsPostgres() bool {
	return auth.Username != "" && auth.Password != ""
}

func (auth CatalogAuth) IsZero() bool {
	return auth == (CatalogAuth{})
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
	Auth CatalogAuth `yaml:"auth,omitempty" json:"auth,omitempty" mapstructure:"auth"`
}

func (c CatalogConfig) IsZero() bool {
	return c.Type == "" &&
		c.CatalogID == "" &&
		c.Region == "" &&
		c.Path == "" &&
		c.Host == "" &&
		c.Port == 0 &&
		c.Database == "" &&
		c.Auth.IsZero()
}

type StorageType string

const (
	StorageTypeS3  StorageType = "s3"
	StorageTypeGCS StorageType = "gcs"
	// Future: StorageTypeLocal.
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
		StorageTypeGCS,
	}
)

type StorageAuth struct {
	// S3/GCS HMAC-style credentials
	AccessKey    string `yaml:"access_key,omitempty" json:"access_key,omitempty" mapstructure:"access_key"`
	SecretKey    string `yaml:"secret_key,omitempty" json:"secret_key,omitempty" mapstructure:"secret_key"`
	SessionToken string `yaml:"session_token,omitempty" json:"session_token,omitempty" mapstructure:"session_token"`
}

func (a StorageAuth) IsS3() bool {
	return a.AccessKey != "" && a.SecretKey != ""
}

func (a StorageAuth) IsGCS() bool {
	return a.AccessKey != "" && a.SecretKey != ""
}

func (a StorageAuth) IsZero() bool {
	return a == (StorageAuth{})
}

type StorageConfig struct {
	Type   StorageType `yaml:"type" json:"type" mapstructure:"type"`
	Path   string      `yaml:"path,omitempty" json:"path,omitempty" mapstructure:"path"`
	Region string      `yaml:"region,omitempty" json:"region,omitempty" mapstructure:"region"`
	Auth   StorageAuth `yaml:"auth,omitempty" json:"auth,omitempty" mapstructure:"auth"`
}

func (s StorageConfig) IsZero() bool {
	return s.Type == "" &&
		s.Path == "" &&
		s.Region == "" &&
		s.Auth.IsZero()
}

type LakehouseConfig struct {
	Format  LakehouseFormat `yaml:"format" json:"format" mapstructure:"format"`
	Catalog CatalogConfig   `yaml:"catalog,omitempty" json:"catalog,omitempty" mapstructure:"catalog"`
	Storage StorageConfig   `yaml:"storage,omitempty" json:"storage,omitempty" mapstructure:"storage"`
}

func (lh *LakehouseConfig) IsZero() bool {
	if lh == nil {
		return true
	}

	return lh.Format == "" && lh.Catalog.IsZero() && lh.Storage.IsZero()
}

// Validate performs basic structural validation of the LakehouseConfig (engine-agnostic).
func (lh *LakehouseConfig) Validate() error {
	if lh == nil {
		return nil
	}

	if lh.Format == "" {
		return errors.New("lakehouse format is required")
	}

	if !slices.Contains([]LakehouseFormat{LakehouseFormatIceberg, LakehouseFormatDuckLake}, lh.Format) {
		return fmt.Errorf("unsupported lakehouse format: %s (supported: iceberg, ducklake)", lh.Format)
	}

	// Validate catalog type if specified
	if lh.Catalog.Type == "" || !slices.Contains(supportedCatalogTypes, lh.Catalog.Type) {
		return errors.New("empty or unsupported catalog type: (supported: glue, postgres, duckdb, sqlite)")
	}

	// Validate storage type if specified
	if lh.Storage.Type == "" || !slices.Contains(supportedStorageTypes, lh.Storage.Type) {
		return errors.New("empty or unsupported storage type: (supported: s3, gcs)")
	}

	return nil
}
