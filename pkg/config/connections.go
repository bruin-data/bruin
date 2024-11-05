package config

import (
	"encoding/json"
	"os"

	"golang.org/x/oauth2/google"
)

type AwsConnection struct {
	Name      string `yaml:"name" json:"name" mapstructure:"name"`
	AccessKey string `yaml:"access_key" json:"access_key" mapstructure:"access_key"`
	SecretKey string `yaml:"secret_key" json:"secret_key" mapstructure:"secret_key"`
}

func (c AwsConnection) GetName() string {
	return c.Name
}

type GoogleCloudPlatformConnection struct {
	Name               string `yaml:"name" json:"name" mapstructure:"name"`
	ServiceAccountJSON string `yaml:"service_account_json" json:"service_account_json,omitempty" mapstructure:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file" json:"service_account_file,omitempty" mapstructure:"service_account_file"`
	ProjectID          string `yaml:"project_id" json:"project_id" mapstructure:"project_id"`
	Location           string `yaml:"location" json:"location,omitempty" mapstructure:"location"`
	rawCredentials     *google.Credentials
}

func (c GoogleCloudPlatformConnection) GetName() string {
	return c.Name
}

func (c GoogleCloudPlatformConnection) MarshalYAML() (interface{}, error) {
	m := make(map[string]interface{})

	if c.Name != "" {
		m["name"] = c.Name
	}
	if c.ProjectID != "" {
		m["project_id"] = c.ProjectID
	}
	if c.Location != "" {
		m["location"] = c.Location
	}

	// Include only one of ServiceAccountJSON or ServiceAccountFile, whichever is not empty
	if c.ServiceAccountFile != "" {
		m["service_account_file"] = c.ServiceAccountFile
	} else if c.ServiceAccountJSON != "" {
		m["service_account_json"] = c.ServiceAccountJSON
	}
	return m, nil
}

func (c *GoogleCloudPlatformConnection) SetCredentials(cred *google.Credentials) {
	c.rawCredentials = cred
}

func (c *GoogleCloudPlatformConnection) GetCredentials() *google.Credentials {
	return c.rawCredentials
}

func (c GoogleCloudPlatformConnection) MarshalJSON() ([]byte, error) {
	if c.ServiceAccountJSON == "" && c.ServiceAccountFile != "" {
		contents, err := os.ReadFile(c.ServiceAccountFile)
		if err != nil {
			return []byte{}, err
		}

		c.ServiceAccountJSON = string(contents)
	}

	return json.Marshal(map[string]string{
		"name":                 c.Name,
		"service_account_json": c.ServiceAccountJSON,
		"service_account_file": c.ServiceAccountFile,
		"project_id":           c.ProjectID,
	})
}

type AthenaConnection struct {
	Name             string `yaml:"name" json:"name" mapstructure:"name"`
	AccessKey        string `yaml:"access_key" json:"access_key" mapstructure:"access_key"`
	SecretKey        string `yaml:"secret_key" json:"secret_key" mapstructure:"secret_key"`
	QueryResultsPath string `yaml:"query_results_path" json:"query_results_path" mapstructure:"query_results_path"`
	Region           string `yaml:"region" json:"region" mapstructure:"region"`
	Database         string `yaml:"database" json:"database,omitempty" mapstructure:"database"`
}

func (c AthenaConnection) GetName() string {
	return c.Name
}

type SynapseConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Host     string `yaml:"host"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port"     json:"port" mapstructure:"port" jsonschema:"default=1433"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`
}

func (c SynapseConnection) GetName() string {
	return c.Name
}

type DatabricksConnection struct {
	Name    string `yaml:"name"  json:"name" mapstructure:"name"`
	Token   string `yaml:"token" json:"token" mapstructure:"token"`
	Path    string `yaml:"path"  json:"path" mapstructure:"path"`
	Host    string `yaml:"host"  json:"host" mapstructure:"host"`
	Port    int    `yaml:"port"  json:"port" mapstructure:"port" jsonschema:"default=443"`
	Catalog string `yaml:"catalog"  json:"catalog" mapstructure:"catalog"`
	Schema  string `yaml:"schema"  json:"schema" mapstructure:"schema"`
}

func (c DatabricksConnection) GetName() string {
	return c.Name
}

type MongoConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Host     string `yaml:"host"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port"     json:"port" mapstructure:"port" jsonschema:"default=27017"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`
}

func (c MongoConnection) GetName() string {
	return c.Name
}

type MsSQLConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Host     string `yaml:"host"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port"     json:"port" mapstructure:"port" jsonschema:"default=1433"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`
}

func (c MsSQLConnection) GetName() string {
	return c.Name
}

type MySQLConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Host     string `yaml:"host"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port"     json:"port" mapstructure:"port" jsonschema:"default=3306"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`
	Driver   string `yaml:"driver" json:"driver,omitempty" mapstructure:"driver"`
}

func (c MySQLConnection) GetName() string {
	return c.Name
}

type PostgresConnection struct {
	Name         string `yaml:"name" json:"name" mapstructure:"name"`
	Username     string `yaml:"username" json:"username" mapstructure:"username"`
	Password     string `yaml:"password" json:"password" mapstructure:"password"`
	Host         string `yaml:"host" json:"host" mapstructure:"host"`
	Port         int    `yaml:"port" json:"port" mapstructure:"port" jsonschema:"default=5432"`
	Database     string `yaml:"database" json:"database" mapstructure:"database"`
	Schema       string `yaml:"schema" json:"schema" mapstructure:"schema"`
	PoolMaxConns int    `yaml:"pool_max_conns" json:"pool_max_conns" mapstructure:"pool_max_conns" default:"10"`
	SslMode      string `yaml:"ssl_mode" json:"ssl_mode" mapstructure:"ssl_mode" default:"disable"`
}

func (c PostgresConnection) GetName() string {
	return c.Name
}

type RedshiftConnection struct {
	Name         string `yaml:"name" json:"name" mapstructure:"name"`
	Username     string `yaml:"username" json:"username" mapstructure:"username"`
	Password     string `yaml:"password" json:"password" mapstructure:"password"`
	Host         string `yaml:"host" json:"host" mapstructure:"host"`
	Port         int    `yaml:"port" json:"port" mapstructure:"port" jsonschema:"default=5439"`
	Database     string `yaml:"database" json:"database" mapstructure:"database"`
	Schema       string `yaml:"schema" json:"schema" mapstructure:"schema"`
	PoolMaxConns int    `yaml:"pool_max_conns" json:"pool_max_conns" mapstructure:"pool_max_conns" default:"10"`
	SslMode      string `yaml:"ssl_mode" json:"ssl_mode" mapstructure:"ssl_mode" default:"disable"`
}

func (c RedshiftConnection) GetName() string {
	return c.Name
}

type SnowflakeConnection struct {
	Name      string `yaml:"name" json:"name" mapstructure:"name"`
	Account   string `yaml:"account" json:"account" mapstructure:"account"`
	Username  string `yaml:"username" json:"username" mapstructure:"username"`
	Password  string `yaml:"password" json:"password" mapstructure:"password"`
	Region    string `yaml:"region" json:"region" mapstructure:"region"`
	Role      string `yaml:"role" json:"role" mapstructure:"role"`
	Database  string `yaml:"database" json:"database" mapstructure:"database"`
	Schema    string `yaml:"schema" json:"schema" mapstructure:"schema"`
	Warehouse string `yaml:"warehouse" json:"warehouse" mapstructure:"warehouse"`
}

func (c SnowflakeConnection) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{
		"name":      c.Name,
		"account":   c.Account,
		"username":  c.Username,
		"password":  c.Password,
		"region":    c.Region,
		"role":      c.Role,
		"database":  c.Database,
		"schema":    c.Schema,
		"warehouse": c.Warehouse,
	})
}

func (c SnowflakeConnection) GetName() string {
	return c.Name
}

type HANAConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Host     string `yaml:"host"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port"     json:"port" mapstructure:"port"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`
}

func (c HANAConnection) GetName() string {
	return c.Name
}

type GorgiasConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	Domain string `yaml:"domain" json:"domain" mapstructure:"domain"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
	Email  string `yaml:"email" json:"email" mapstructure:"email"`
}

func (c GorgiasConnection) GetName() string {
	return c.Name
}

type KlaviyoConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c KlaviyoConnection) GetName() string {
	return c.Name
}

type AdjustConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c AdjustConnection) GetName() string {
	return c.Name
}

type FacebookAdsConnection struct {
	Name        string `yaml:"name" json:"name" mapstructure:"name"`
	AccessToken string `yaml:"access_token" json:"access_token" mapstructure:"access_token"`
	AccountID   string `yaml:"account_id" json:"account_id" mapstructure:"account_id"`
}

func (c FacebookAdsConnection) GetName() string {
	return c.Name
}

type StripeConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c StripeConnection) GetName() string {
	return c.Name
}

type NotionConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c NotionConnection) GetName() string {
	return c.Name
}

type ShopifyConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	URL    string `yaml:"url" json:"url" mapstructure:"url"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c ShopifyConnection) GetName() string {
	return c.Name
}

type KafkaConnection struct {
	Name             string `yaml:"name" json:"name" mapstructure:"name"`
	BootstrapServers string `yaml:"bootstrap_servers" json:"bootstrap_servers" mapstructure:"bootstrap_servers"`
	GroupID          string `yaml:"group_id" json:"group_id" mapstructure:"group_id"`
	SecurityProtocol string `yaml:"security_protocol" json:"security_protocol,omitempty" mapstructure:"security_protocol"`
	SaslMechanisms   string `yaml:"sasl_mechanisms" json:"sasl_mechanisms,omitempty" mapstructure:"sasl_mechanisms"`
	SaslUsername     string `yaml:"sasl_username" json:"sasl_username,omitempty" mapstructure:"sasl_username"`
	SaslPassword     string `yaml:"sasl_password" json:"sasl_password,omitempty" mapstructure:"sasl_password"`
	BatchSize        string `yaml:"batch_size" json:"batch_size,omitempty" mapstructure:"batch_size"`
	BatchTimeout     string `yaml:"batch_timeout" json:"batch_timeout,omitempty" mapstructure:"batch_timeout"`
}

func (c KafkaConnection) GetName() string {
	return c.Name
}

type AirtableConnection struct {
	Name        string `yaml:"name" json:"name" mapstructure:"name"`
	BaseID      string `yaml:"base_id" json:"base_id" mapstructure:"base_id"`
	AccessToken string `yaml:"access_token" json:"access_token" mapstructure:"access_token"`
}

func (c AirtableConnection) GetName() string {
	return c.Name
}

type DuckDBConnection struct {
	Name string `yaml:"name" json:"name" mapstructure:"name"`
	Path string `yaml:"path" json:"path" mapstructure:"path"`
}

func (d DuckDBConnection) GetName() string {
	return d.Name
}

type AppsflyerConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c AppsflyerConnection) GetName() string {
	return c.Name
}

type HubspotConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c HubspotConnection) GetName() string {
	return c.Name
}

type GoogleSheetsConnection struct {
	Name            string `yaml:"name" json:"name" mapstructure:"name"`
	CredentialsPath string `yaml:"credentials_path" json:"credentials_path" mapstructure:"credentials_path"`
}

func (c GoogleSheetsConnection) GetName() string {
	return c.Name
}

type ChessConnection struct {
	Name    string   `yaml:"name" json:"name" mapstructure:"name"`
	Players []string `yaml:"players" json:"players" mapstructure:"players"`
}

func (c ChessConnection) GetName() string {
	return c.Name
}

type GenericConnection struct {
	Name  string `yaml:"name" json:"name" mapstructure:"name"`
	Value string `yaml:"value" json:"value" mapstructure:"value"`
}

func (c GenericConnection) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{
		"name":  c.Name,
		"value": c.Value,
	})
}

func (c GenericConnection) GetName() string {
	return c.Name
}

type S3Connection struct {
	Name            string `yaml:"name" json:"name" mapstructure:"name"`
	BucketName      string `yaml:"bucket_name" json:"bucket_name" mapstructure:"bucket_name"`
	PathToFile      string `yaml:"path_to_file" json:"path_to_file" mapstructure:"path_to_file"`
	AccessKeyID     string `yaml:"access_key_id" json:"access_key_id" mapstructure:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key" json:"secret_access_key" mapstructure:"secret_access_key"`
}

func (c S3Connection) GetName() string {
	return c.Name
}

type SlackConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c SlackConnection) GetName() string {
	return c.Name
}
