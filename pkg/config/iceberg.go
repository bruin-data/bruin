package config

// This file defines the Iceberg ingestr-destination config, kept separate from
// the DuckDB lakehouse types (lakehouse.go) so the two evolve independently.

type IcebergCatalogType string

const (
	IcebergCatalogGlue     IcebergCatalogType = "glue"
	IcebergCatalogSQLite   IcebergCatalogType = "sqlite"
	IcebergCatalogPostgres IcebergCatalogType = "postgres"
	IcebergCatalogREST     IcebergCatalogType = "rest"
	IcebergCatalogHive     IcebergCatalogType = "hive"
	IcebergCatalogHadoop   IcebergCatalogType = "hadoop"
	IcebergCatalogSQL      IcebergCatalogType = "sql"
)

type IcebergStorageType string

const (
	IcebergStorageS3    IcebergStorageType = "s3"
	IcebergStorageGCS   IcebergStorageType = "gcs"
	IcebergStorageLocal IcebergStorageType = "local"
)

// IcebergAuth holds credentials for the catalog and/or storage backend.
type IcebergAuth struct {
	// S3 / Glue credentials.
	AccessKey    string `yaml:"access_key,omitempty" json:"access_key,omitempty" mapstructure:"access_key" sensitive:"true"`
	SecretKey    string `yaml:"secret_key,omitempty" json:"secret_key,omitempty" mapstructure:"secret_key" sensitive:"true"`
	SessionToken string `yaml:"session_token,omitempty" json:"session_token,omitempty" mapstructure:"session_token" sensitive:"true"`

	// Postgres-catalog credentials.
	Username string `yaml:"username,omitempty" json:"username,omitempty" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password,omitempty" mapstructure:"password" sensitive:"true"`
}

// IcebergCatalog describes the Iceberg catalog backend. Which fields apply depends
// on Type (glue: catalog_id/region; sqlite/hadoop: path; postgres/rest/hive: host).
type IcebergCatalog struct {
	Type      IcebergCatalogType `yaml:"type" json:"type" mapstructure:"type"`
	CatalogID string             `yaml:"catalog_id,omitempty" json:"catalog_id,omitempty" mapstructure:"catalog_id"`
	Region    string             `yaml:"region,omitempty" json:"region,omitempty" mapstructure:"region"`
	Path      string             `yaml:"path,omitempty" json:"path,omitempty" mapstructure:"path"`
	Host      string             `yaml:"host,omitempty" json:"host,omitempty" mapstructure:"host"`
	Port      int                `yaml:"port,omitempty" json:"port,omitempty" mapstructure:"port"`
	Database  string             `yaml:"database,omitempty" json:"database,omitempty" mapstructure:"database"`
	Auth      IcebergAuth        `yaml:"auth,omitempty" json:"auth,omitempty" mapstructure:"auth"`

	// RestUseSSL selects HTTPS for the REST catalog (ingestr defaults to HTTP);
	// set true for managed catalogs like Polaris, Unity, or Tabular.
	RestUseSSL *bool `yaml:"rest_use_ssl,omitempty" json:"rest_use_ssl,omitempty" mapstructure:"rest_use_ssl"`

	// Driver and Dialect configure the generic sql catalog (both required, e.g.
	// driver=pgx, dialect=postgres); the sqlite/postgres types set them for you.
	Driver  string `yaml:"driver,omitempty" json:"driver,omitempty" mapstructure:"driver"`
	Dialect string `yaml:"dialect,omitempty" json:"dialect,omitempty" mapstructure:"dialect"`

	// Catalog credentials. These are dedicated fields (rather than free-form
	// Properties entries) so the credential masker redacts them from run logs.
	// Credential/Token are REST-catalog auth; URI is the SQL-catalog connection
	// string (may embed a password).
	Credential string `yaml:"credential,omitempty" json:"credential,omitempty" mapstructure:"credential" sensitive:"true"`
	Token      string `yaml:"token,omitempty" json:"token,omitempty" mapstructure:"token" sensitive:"true"`
	URI        string `yaml:"uri,omitempty" json:"uri,omitempty" mapstructure:"uri" sensitive:"true"`
}

// IcebergStorage describes where data files live. Type is "s3" (or S3-compatible),
// "gcs", or "local"; leave the block empty to inherit the catalog's warehouse.
type IcebergStorage struct {
	// Type is optional: when empty it's inferred from the warehouse scheme
	// (s3://→s3, gs://→gcs, file://→local); set it to disambiguate the bucket form.
	Type IcebergStorageType `yaml:"type,omitempty" json:"type,omitempty" mapstructure:"type"`
	// Path is the full warehouse location: s3://<bucket>/<prefix>,
	// gs://<bucket>/<prefix>, or (for local storage) a filesystem path/file:// URI.
	Path string `yaml:"path,omitempty" json:"path,omitempty" mapstructure:"path"`
	// Bucket/Prefix are an alternative to Path: the bucket name and an optional
	// key prefix, from which the s3://… or gs://… warehouse is built.
	Bucket   string      `yaml:"bucket,omitempty" json:"bucket,omitempty" mapstructure:"bucket"`
	Prefix   string      `yaml:"prefix,omitempty" json:"prefix,omitempty" mapstructure:"prefix"`
	Region   string      `yaml:"region,omitempty" json:"region,omitempty" mapstructure:"region"`
	Endpoint string      `yaml:"endpoint,omitempty" json:"endpoint,omitempty" mapstructure:"endpoint"`
	UseSSL   *bool       `yaml:"use_ssl,omitempty" json:"use_ssl,omitempty" mapstructure:"use_ssl"`
	Auth     IcebergAuth `yaml:"auth,omitempty" json:"auth,omitempty" mapstructure:"auth"`

	// GCS service-account credentials (type "gcs"): KeyFile is a key-file path,
	// KeyJSON the inline JSON. Empty uses Application Default Credentials.
	KeyFile string `yaml:"key_file,omitempty" json:"key_file,omitempty" mapstructure:"key_file"`
	KeyJSON string `yaml:"key_json,omitempty" json:"key_json,omitempty" mapstructure:"key_json" sensitive:"true"`
}
