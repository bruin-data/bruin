package config

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/config"
	"golang.org/x/oauth2/google"
)

type AwsConnection struct {
	Name      string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessKey string `yaml:"access_key,omitempty" json:"access_key" mapstructure:"access_key"`
	SecretKey string `yaml:"secret_key,omitempty" json:"secret_key" mapstructure:"secret_key"`
	Region    string `yaml:"region,omitempty" json:"region" mapstructure:"region"`
}

func (c AwsConnection) GetName() string {
	return c.Name
}

type GoogleCloudPlatformConnection struct { //nolint:recvcheck
	Name                             string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ServiceAccountJSON               string `yaml:"service_account_json,omitempty" json:"service_account_json,omitempty" mapstructure:"service_account_json"`
	ServiceAccountFile               string `yaml:"service_account_file,omitempty" json:"service_account_file,omitempty" mapstructure:"service_account_file"`
	ProjectID                        string `yaml:"project_id,omitempty" json:"project_id" mapstructure:"project_id"`
	Location                         string `yaml:"location,omitempty" json:"location,omitempty" mapstructure:"location"`
	UseApplicationDefaultCredentials bool   `yaml:"use_application_default_credentials,omitempty" json:"use_application_default_credentials,omitempty" mapstructure:"use_application_default_credentials"`
	rawCredentials                   *google.Credentials
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

	if c.UseApplicationDefaultCredentials {
		m["use_application_default_credentials"] = c.UseApplicationDefaultCredentials
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
		"name":                                c.Name,
		"service_account_json":                c.ServiceAccountJSON,
		"service_account_file":                c.ServiceAccountFile,
		"project_id":                          c.ProjectID,
		"location":                            c.Location,
		"use_application_default_credentials": strconv.FormatBool(c.UseApplicationDefaultCredentials),
	})
}

type AthenaConnection struct { //nolint:recvcheck
	Name             string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessKey        string `yaml:"access_key_id,omitempty" json:"access_key_id" mapstructure:"access_key_id"`
	SecretKey        string `yaml:"secret_access_key,omitempty" json:"secret_access_key" mapstructure:"secret_access_key"`
	SessionToken     string `yaml:"session_token,omitempty" json:"session_token" mapstructure:"session_token"`
	QueryResultsPath string `yaml:"query_results_path,omitempty" json:"query_results_path" mapstructure:"query_results_path"`
	Region           string `yaml:"region,omitempty" json:"region" mapstructure:"region"`
	Database         string `yaml:"database,omitempty" json:"database,omitempty" mapstructure:"database"`
	Profile          string `yaml:"profile,omitempty" json:"profile,omitempty" mapstructure:"profile"`

	regionSetFromProfile bool
}

func (c AthenaConnection) GetName() string {
	return c.Name
}

func (c *AthenaConnection) LoadCredentialsFromProfile(ctx context.Context) error {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(c.Profile),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS credentials from profile %s: %w", c.Profile, err)
	}
	creds, err := cfg.Credentials.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve AWS credentials: %w", err)
	}

	c.AccessKey = creds.AccessKeyID
	c.SecretKey = creds.SecretAccessKey
	c.SessionToken = creds.SessionToken

	if c.Region == "" {
		c.Region = cfg.Region
		c.regionSetFromProfile = true
	}

	return nil
}

func (c AthenaConnection) MarshalYAML() (interface{}, error) {
	m := make(map[string]interface{})

	if c.Name != "" {
		m["name"] = c.Name
	}

	if c.QueryResultsPath != "" {
		m["query_results_path"] = c.QueryResultsPath
	}

	if c.Database != "" {
		m["database"] = c.Database
	}

	if c.Region != "" && !c.regionSetFromProfile {
		m["region"] = c.Region
	}

	if c.Profile != "" {
		m["profile"] = c.Profile
		return m, nil
	}

	if c.AccessKey != "" {
		m["access_key_id"] = c.AccessKey
	}

	if c.SecretKey != "" {
		m["secret_access_key"] = c.SecretKey
	}

	return m, nil
}

type SynapseConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port,omitempty"     json:"port" mapstructure:"port" jsonschema:"default=1433"`
	Database string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
	Options  string `yaml:"options,omitempty"  json:"options,omitempty" mapstructure:"options"`
}

func (c SynapseConnection) GetName() string {
	return c.Name
}

type FabricWarehouseConnection struct {
	Name                      string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Host                      string `yaml:"host,omitempty"     json:"host" mapstructure:"host"`
	Port                      int    `yaml:"port,omitempty"     json:"port" mapstructure:"port" jsonschema:"default=1433"`
	Database                  string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
	Username                  string `yaml:"username,omitempty" json:"username,omitempty" mapstructure:"username"`
	Password                  string `yaml:"password,omitempty" json:"password,omitempty" mapstructure:"password"`
	Options                   string `yaml:"options,omitempty"  json:"options,omitempty" mapstructure:"options"`
	UseAzureDefaultCredential bool   `yaml:"use_azure_default_credential,omitempty" json:"use_azure_default_credential,omitempty" mapstructure:"use_azure_default_credential"`
	ClientID                  string `yaml:"client_id,omitempty" json:"client_id,omitempty" mapstructure:"client_id"`
	ClientSecret              string `yaml:"client_secret,omitempty" json:"client_secret,omitempty" mapstructure:"client_secret"`
	TenantID                  string `yaml:"tenant_id,omitempty" json:"tenant_id,omitempty" mapstructure:"tenant_id"`
}

func (c FabricWarehouseConnection) GetName() string {
	return c.Name
}

type DatabricksConnection struct {
	Name         string `yaml:"name,omitempty"  json:"name" mapstructure:"name"`
	Token        string `yaml:"token,omitempty" json:"token,omitempty" mapstructure:"token" jsonschema:"oneof_required=token"`
	Path         string `yaml:"path,omitempty"  json:"path" mapstructure:"path"`
	Host         string `yaml:"host,omitempty"  json:"host" mapstructure:"host"`
	Port         int    `yaml:"port,omitempty"  json:"port" mapstructure:"port" jsonschema:"default=443"`
	Catalog      string `yaml:"catalog,omitempty"  json:"catalog" mapstructure:"catalog"`
	Schema       string `yaml:"schema,omitempty"  json:"schema" mapstructure:"schema"`
	ClientID     string `yaml:"client_id,omitempty" json:"client_id,omitempty" mapstructure:"client_id" jsonschema:"oneof_required=oauth"`
	ClientSecret string `yaml:"client_secret,omitempty" json:"client_secret,omitempty" mapstructure:"client_secret" jsonschema:"oneof_required=oauth"`
}

func (c DatabricksConnection) GetName() string {
	return c.Name
}

type MongoConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port,omitempty"     json:"port" mapstructure:"port" jsonschema:"default=27017"`
	Database string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
}

func (c MongoConnection) GetName() string {
	return c.Name
}

type CouchbaseConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Bucket   string `yaml:"bucket,omitempty" json:"bucket,omitempty" mapstructure:"bucket"`
	SSL      bool   `yaml:"ssl,omitempty" json:"ssl,omitempty" mapstructure:"ssl"`
}

func (c CouchbaseConnection) GetName() string {
	return c.Name
}

type CursorConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c CursorConnection) GetName() string {
	return c.Name
}

type MongoAtlasConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Database string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
}

func (c MongoAtlasConnection) GetName() string {
	return c.Name
}

type MsSQLConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port,omitempty"     json:"port" mapstructure:"port" jsonschema:"default=1433"`
	Database string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
	Options  string `yaml:"options,omitempty"  json:"options,omitempty" mapstructure:"options"`
}

func (c MsSQLConnection) GetName() string {
	return c.Name
}

type MySQLConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username    string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password    string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host        string `yaml:"host,omitempty"     json:"host" mapstructure:"host"`
	Port        int    `yaml:"port,omitempty"     json:"port" mapstructure:"port" jsonschema:"default=3306"`
	Database    string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
	Driver      string `yaml:"driver,omitempty" json:"driver,omitempty" mapstructure:"driver"`
	SslCaPath   string `yaml:"ssl_ca_path,omitempty" json:"ssl_ca_path,omitempty" mapstructure:"ssl_ca_path"`
	SslCertPath string `yaml:"ssl_cert_path,omitempty" json:"ssl_cert_path,omitempty" mapstructure:"ssl_cert_path"`
	SslKeyPath  string `yaml:"ssl_key_path,omitempty" json:"ssl_key_path,omitempty" mapstructure:"ssl_key_path"`
}

func (c MySQLConnection) GetName() string {
	return c.Name
}

type PostgresConnection struct {
	Name         string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username     string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password     string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host         string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Port         int    `yaml:"port,omitempty" json:"port" mapstructure:"port" jsonschema:"default=5432"`
	Database     string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
	Schema       string `yaml:"schema,omitempty" json:"schema,omitempty" mapstructure:"schema"`
	PoolMaxConns int    `yaml:"pool_max_conns,omitempty" json:"pool_max_conns,omitempty" mapstructure:"pool_max_conns" default:"10"`
	SslMode      string `yaml:"ssl_mode,omitempty" json:"ssl_mode,omitempty" mapstructure:"ssl_mode" default:"allow"`
}

func (c PostgresConnection) GetName() string {
	return c.Name
}

type RedshiftConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Port     int    `yaml:"port,omitempty" json:"port" mapstructure:"port" jsonschema:"default=5439"`
	Database string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
	Schema   string `yaml:"schema,omitempty" json:"schema" mapstructure:"schema"`
	SslMode  string `yaml:"ssl_mode,omitempty" json:"ssl_mode,omitempty" mapstructure:"ssl_mode" default:"allow"`
}

func (c RedshiftConnection) GetName() string {
	return c.Name
}

type SnowflakeConnection struct {
	Name           string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Account        string `yaml:"account,omitempty" json:"account" mapstructure:"account"`
	Username       string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password       string `yaml:"password,omitempty" json:"password,omitempty" jsonschema:"oneof_required=password" mapstructure:"password"`
	Region         string `yaml:"region,omitempty" json:"region,omitempty" mapstructure:"region"`
	Role           string `yaml:"role,omitempty" json:"role,omitempty" mapstructure:"role"`
	Database       string `yaml:"database,omitempty" json:"database,omitempty" mapstructure:"database"`
	Schema         string `yaml:"schema,omitempty" json:"schema,omitempty" mapstructure:"schema"`
	Warehouse      string `yaml:"warehouse,omitempty" json:"warehouse,omitempty" mapstructure:"warehouse"`
	PrivateKeyPath string `yaml:"private_key_path,omitempty" json:"private_key_path,omitempty" jsonschema:"oneof_required=private_key_path" mapstructure:"private_key_path"`
	PrivateKey     string `yaml:"private_key,omitempty" json:"private_key,omitempty" jsonschema:"oneof_required=private_key" mapstructure:"private_key"`
}

func (c SnowflakeConnection) MarshalJSON() ([]byte, error) {
	if c.PrivateKey != "" && c.PrivateKeyPath != "" {
		fmt.Printf("Warning: Both private_key and private_key_path are set for Snowflake connection '%s'. Using private_key content and ignoring private_key_path.\n", c.Name)
	}

	if c.PrivateKey == "" && c.PrivateKeyPath != "" {
		contents, err := os.ReadFile(c.PrivateKeyPath)
		if err != nil {
			return []byte{}, err
		}

		c.PrivateKey = string(contents)
	}

	return json.Marshal(map[string]string{
		"name":        c.Name,
		"account":     c.Account,
		"username":    c.Username,
		"password":    c.Password,
		"region":      c.Region,
		"role":        c.Role,
		"database":    c.Database,
		"schema":      c.Schema,
		"warehouse":   c.Warehouse,
		"private_key": c.PrivateKey,
	})
}

func (c SnowflakeConnection) GetName() string {
	return c.Name
}

type HANAConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port,omitempty"     json:"port" mapstructure:"port"`
	Database string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
}

func (c HANAConnection) GetName() string {
	return c.Name
}

type HostawayConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c HostawayConnection) GetName() string {
	return c.Name
}

type GorgiasConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Domain string `yaml:"domain,omitempty" json:"domain" mapstructure:"domain"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
	Email  string `yaml:"email,omitempty" json:"email" mapstructure:"email"`
}

func (c GorgiasConnection) GetName() string {
	return c.Name
}

type KlaviyoConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c KlaviyoConnection) GetName() string {
	return c.Name
}

type AdjustConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c AdjustConnection) GetName() string {
	return c.Name
}

type AnthropicConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c AnthropicConnection) GetName() string {
	return c.Name
}

type FacebookAdsConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessToken string `yaml:"access_token,omitempty" json:"access_token" mapstructure:"access_token"`
	AccountID   string `yaml:"account_id,omitempty" json:"account_id" mapstructure:"account_id"`
}

func (c FacebookAdsConnection) GetName() string {
	return c.Name
}

type StripeConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c StripeConnection) GetName() string {
	return c.Name
}

type NotionConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c NotionConnection) GetName() string {
	return c.Name
}

type AlliumConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c AlliumConnection) GetName() string {
	return c.Name
}

type ShopifyConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	URL    string `yaml:"url,omitempty" json:"url" mapstructure:"url"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c ShopifyConnection) GetName() string {
	return c.Name
}

type KafkaConnection struct {
	Name             string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	BootstrapServers string `yaml:"bootstrap_servers,omitempty" json:"bootstrap_servers" mapstructure:"bootstrap_servers"`
	GroupID          string `yaml:"group_id,omitempty" json:"group_id" mapstructure:"group_id"`
	SecurityProtocol string `yaml:"security_protocol,omitempty" json:"security_protocol,omitempty" mapstructure:"security_protocol"`
	SaslMechanisms   string `yaml:"sasl_mechanisms,omitempty" json:"sasl_mechanisms,omitempty" mapstructure:"sasl_mechanisms"`
	SaslUsername     string `yaml:"sasl_username,omitempty" json:"sasl_username,omitempty" mapstructure:"sasl_username"`
	SaslPassword     string `yaml:"sasl_password,omitempty" json:"sasl_password,omitempty" mapstructure:"sasl_password"`
	BatchSize        string `yaml:"batch_size,omitempty" json:"batch_size,omitempty" mapstructure:"batch_size"`
	BatchTimeout     string `yaml:"batch_timeout,omitempty" json:"batch_timeout,omitempty" mapstructure:"batch_timeout"`
}

func (c KafkaConnection) GetName() string {
	return c.Name
}

type AirtableConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	BaseID      string `yaml:"base_id,omitempty" json:"base_id" mapstructure:"base_id"`
	AccessToken string `yaml:"access_token,omitempty" json:"access_token" mapstructure:"access_token"`
}

func (c AirtableConnection) GetName() string {
	return c.Name
}

type DuckDBConnection struct {
	Name string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Path string `yaml:"path,omitempty" json:"path" mapstructure:"path"`
}

func (d DuckDBConnection) GetName() string {
	return d.Name
}

type MotherduckConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Token    string `yaml:"token,omitempty" json:"token" mapstructure:"token"`
	Database string `yaml:"database,omitempty" json:"database,omitempty" mapstructure:"database"`
}

func (m MotherduckConnection) GetName() string {
	return m.Name
}

type ClickHouseConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Host     string `yaml:"host"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port"     json:"port" mapstructure:"port"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`
	HTTPPort int    `yaml:"http_port" json:"http_port" mapstructure:"http_port"`
	Secure   *int   `yaml:"secure" json:"secure" mapstructure:"secure"`
}

func (c ClickHouseConnection) GetName() string {
	return c.Name
}

type AppsflyerConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c AppsflyerConnection) GetName() string {
	return c.Name
}

type HubspotConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c HubspotConnection) GetName() string {
	return c.Name
}

type IntercomConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessToken string `yaml:"access_token,omitempty" json:"access_token" mapstructure:"access_token"`
	Region      string `yaml:"region,omitempty" json:"region" mapstructure:"region"`
}

func (c IntercomConnection) GetName() string {
	return c.Name
}

type GoogleSheetsConnection struct {
	Name               string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ServiceAccountJSON string `yaml:"service_account_json,omitempty" json:"service_account_json,omitempty" mapstructure:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file,omitempty" json:"service_account_file,omitempty" mapstructure:"service_account_file"`
}

func (c GoogleSheetsConnection) GetName() string {
	return c.Name
}

type ChessConnection struct {
	Name    string   `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Players []string `yaml:"players,omitempty" json:"players" mapstructure:"players" jsonschema:"default=MagnusCarlsen,default=Hikaru"`
}

func (c ChessConnection) GetName() string {
	return c.Name
}

type ApplovinMaxConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c ApplovinMaxConnection) GetName() string {
	return c.Name
}

type GenericConnection struct {
	Name  string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Value string `yaml:"value,omitempty" json:"value" mapstructure:"value"`
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
	Name            string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	BucketName      string `yaml:"bucket_name,omitempty" json:"bucket_name" mapstructure:"bucket_name"`
	PathToFile      string `yaml:"path_to_file,omitempty" json:"path_to_file" mapstructure:"path_to_file"`
	AccessKeyID     string `yaml:"access_key_id,omitempty" json:"access_key_id" mapstructure:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key,omitempty" json:"secret_access_key" mapstructure:"secret_access_key"`
	EndpointURL     string `yaml:"endpoint_url,omitempty" json:"endpoint_url" mapstructure:"endpoint_url"`
	Layout          string `yaml:"layout,omitempty" json:"layout" mapstructure:"layout"`
}

func (c S3Connection) GetName() string {
	return c.Name
}

type ZendeskConnection struct {
	Name       string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIToken   string `yaml:"api_token,omitempty" json:"api_token" mapstructure:"api_token"`
	Email      string `yaml:"email,omitempty" json:"email" mapstructure:"email"`
	OAuthToken string `yaml:"oauth_token,omitempty" json:"oauth_token" mapstructure:"oauth_token"`
	Subdomain  string `yaml:"sub_domain,omitempty" json:"sub_domain" mapstructure:"sub_domain"`
}

func (c ZendeskConnection) GetName() string {
	return c.Name
}

type SlackConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c SlackConnection) GetName() string {
	return c.Name
}

type SocrataConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Domain   string `yaml:"domain,omitempty" json:"domain" mapstructure:"domain"`
	AppToken string `yaml:"app_token,omitempty" json:"app_token" mapstructure:"app_token"`
	Username string `yaml:"username,omitempty" json:"username,omitempty" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password,omitempty" mapstructure:"password"`
}

func (c SocrataConnection) GetName() string {
	return c.Name
}

type PersonioConnection struct {
	Name         string `yaml:"name" json:"name" mapstructure:"name"`
	ClientID     string `yaml:"client_id" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret" json:"client_secret" mapstructure:"client_secret"`
}

func (c PersonioConnection) GetName() string {
	return c.Name
}

type AsanaConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessToken string `yaml:"access_token,omitempty" json:"access_token" mapstructure:"access_token"`
	WorkspaceID string `yaml:"workspace,omitempty" json:"workspace" mapstructure:"workspace"`
}

func (c AsanaConnection) GetName() string {
	return c.Name
}

type DynamoDBConnection struct {
	Name            string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessKeyID     string `yaml:"access_key_id,omitempty" json:"access_key_id" mapstructure:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key,omitempty" json:"secret_access_key" mapstructure:"secret_access_key"`
	Region          string `yaml:"region,omitempty" json:"region" mapstructure:"region"`
}

func (c DynamoDBConnection) GetName() string {
	return c.Name
}

type DoceboConnection struct {
	Name         string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	BaseURL      string `yaml:"base_url,omitempty" json:"base_url" mapstructure:"base_url"`
	ClientID     string `yaml:"client_id,omitempty" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret,omitempty" json:"client_secret" mapstructure:"client_secret"`
	Username     string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password     string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
}

func (c DoceboConnection) GetName() string {
	return c.Name
}

type GoogleAdsConnection struct {
	Name               string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	CustomerID         string `yaml:"customer_id,omitempty" json:"customer_id" mapstructure:"customer_id"`
	ServiceAccountJSON string `yaml:"service_account_json,omitempty" json:"service_account_json,omitempty" mapstructure:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file,omitempty" json:"service_account_file,omitempty" mapstructure:"service_account_file"`
	DeveloperToken     string `yaml:"dev_token,omitempty" json:"dev_token" mapstructure:"dev_token"`
	LoginCustomerID    string `yaml:"login_customer_id,omitempty" json:"login_customer_id,omitempty" mapstructure:"login_customer_id"`
}

func (c GoogleAdsConnection) GetName() string {
	return c.Name
}

type TikTokAdsConnection struct {
	Name          string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessToken   string `yaml:"access_token,omitempty" json:"access_token" mapstructure:"access_token"`
	AdvertiserIDs string `yaml:"advertiser_ids,omitempty" json:"advertiser_ids" mapstructure:"advertiser_ids"`
	Timezone      string `yaml:"timezone,omitempty" json:"timezone,omitempty" mapstructure:"timezone"`
}

func (c TikTokAdsConnection) GetName() string {
	return c.Name
}

type SnapchatAdsConnection struct {
	Name           string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	RefreshToken   string `yaml:"refresh_token,omitempty" json:"refresh_token" mapstructure:"refresh_token"`
	ClientID       string `yaml:"client_id,omitempty" json:"client_id" mapstructure:"client_id"`
	ClientSecret   string `yaml:"client_secret,omitempty" json:"client_secret" mapstructure:"client_secret"`
	OrganizationID string `yaml:"organization_id,omitempty" json:"organization_id,omitempty" mapstructure:"organization_id"`
}

func (c SnapchatAdsConnection) GetName() string {
	return c.Name
}

// github://?access_token=<access_token>&owner=<owner>&repo=<repo>
type GitHubConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessToken string `yaml:"access_token,omitempty" json:"access_token,omitempty" mapstructure:"access_token"`
	Owner       string `yaml:"owner,omitempty" json:"owner" mapstructure:"owner"`
	Repo        string `yaml:"repo,omitempty" json:"repo" mapstructure:"repo"`
}

func (c GitHubConnection) GetName() string {
	return c.Name
}

type AppStoreConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	KeyID    string `yaml:"key_id,omitempty" json:"key_id" mapstructure:"key_id"`
	IssuerID string `yaml:"issuer_id,omitempty" json:"issuer_id" mapstructure:"issuer_id"`
	KeyPath  string `yaml:"key_path,omitempty" json:"key_path" mapstructure:"key_path"`
	Key      string `yaml:"key,omitempty" json:"key" mapstructure:"key"`
}

func (c AppStoreConnection) GetName() string {
	return c.Name
}

type LinkedInAdsConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessToken string `yaml:"access_token,omitempty" json:"access_token" mapstructure:"access_token"`
	AccountIds  string `yaml:"account_ids,omitempty" json:"account_ids" mapstructure:"account_ids"`
}

func (c LinkedInAdsConnection) GetName() string {
	return c.Name
}

type MailchimpConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
	Server string `yaml:"server,omitempty" json:"server" mapstructure:"server"`
}

func (c MailchimpConnection) GetName() string {
	return c.Name
}

type RevenueCatConnection struct {
	Name      string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey    string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
	ProjectID string `yaml:"project_id,omitempty" json:"project_id" mapstructure:"project_id"`
}

func (c RevenueCatConnection) GetName() string {
	return c.Name
}

type LinearConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c LinearConnection) GetName() string {
	return c.Name
}

type GCSConnection struct {
	Name               string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ServiceAccountFile string `yaml:"service_account_file,omitempty" json:"service_account_file,omitempty" jsonschema:"oneof_required=service_account_file" mapstructure:"service_account_file"`
	ServiceAccountJSON string `yaml:"service_account_json,omitempty" json:"service_account_json,omitempty" jsonschema:"oneof_required=service_account_json" mapstructure:"service_account_json"`
	BucketName         string `yaml:"bucket_name,omitempty" json:"bucket_name,omitempty" mapstructure:"bucket_name"`
	PathToFile         string `yaml:"path_to_file,omitempty" json:"path_to_file,omitempty" mapstructure:"path_to_file"`
	Layout             string `yaml:"layout,omitempty" json:"layout,omitempty" mapstructure:"layout"`
}

func (c GCSConnection) GetName() string {
	return c.Name
}

type KinesisConnection struct {
	Name            string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessKeyID     string `yaml:"access_key_id,omitempty" json:"access_key_id" mapstructure:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key,omitempty" json:"secret_access_key" mapstructure:"secret_access_key"`
	Region          string `yaml:"region,omitempty" json:"region" mapstructure:"region"`
}

func (c KinesisConnection) GetName() string {
	return c.Name
}

type PipedriveConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIToken string `yaml:"api_token,omitempty" json:"api_token" mapstructure:"api_token"`
}

func (c PipedriveConnection) GetName() string {
	return c.Name
}

type MixpanelConnection struct {
	Name      string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username  string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password  string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	ProjectID string `yaml:"project_id,omitempty" json:"project_id" mapstructure:"project_id"`
	Server    string `yaml:"server,omitempty" json:"server,omitempty" mapstructure:"server"`
}

func (c MixpanelConnection) GetName() string {
	return c.Name
}

type ClickupConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIToken string `yaml:"api_token,omitempty" json:"api_token" mapstructure:"api_token"`
}

func (c ClickupConnection) GetName() string {
	return c.Name
}

type PinterestConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessToken string `yaml:"access_token,omitempty" json:"access_token" mapstructure:"access_token"`
}

func (c PinterestConnection) GetName() string {
	return c.Name
}

type TrustpilotConnection struct {
	Name           string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	BusinessUnitID string `yaml:"business_unit_id,omitempty" json:"business_unit_id" mapstructure:"business_unit_id"`
	APIKey         string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c TrustpilotConnection) GetName() string {
	return c.Name
}

type QuickBooksConnection struct {
	Name         string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	CompanyID    string `yaml:"company_id,omitempty" json:"company_id" mapstructure:"company_id"`
	ClientID     string `yaml:"client_id,omitempty" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret,omitempty" json:"client_secret" mapstructure:"client_secret"`
	RefreshToken string `yaml:"refresh_token,omitempty" json:"refresh_token" mapstructure:"refresh_token"`
	Environment  string `yaml:"environment,omitempty" json:"environment,omitempty" mapstructure:"environment"`
	MinorVersion string `yaml:"minor_version,omitempty" json:"minor_version,omitempty" mapstructure:"minor_version"`
}

func (c QuickBooksConnection) GetName() string {
	return c.Name
}

type WiseConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c WiseConnection) GetName() string {
	return c.Name
}

type ZoomConnection struct {
	Name         string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ClientID     string `yaml:"client_id,omitempty" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret,omitempty" json:"client_secret" mapstructure:"client_secret"`
	AccountID    string `yaml:"account_id,omitempty" json:"account_id" mapstructure:"account_id"`
}

func (c ZoomConnection) GetName() string {
	return c.Name
}

type EMRServerlessConnection struct {
	Name          string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessKey     string `yaml:"access_key" json:"access_key" mapstructure:"access_key"`
	SecretKey     string `yaml:"secret_key" json:"secret_key" mapstructure:"secret_key"`
	ApplicationID string `yaml:"application_id" json:"application_id" mapstructure:"application_id"`
	ExecutionRole string `yaml:"execution_role" json:"execution_role" mapstructure:"execution_role"`
	Region        string `yaml:"region" json:"region" mapstructure:"region"`
	Workspace     string `yaml:"workspace" json:"workspace,omitempty" mapstructure:"workspace"`
}

func (c EMRServerlessConnection) GetName() string {
	return c.Name
}

type DataprocServerlessConnection struct {
	Name                             string   `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ServiceAccountJSON               string   `yaml:"service_account_json,omitempty" json:"service_account_json,omitempty" mapstructure:"service_account_json"`
	ServiceAccountFile               string   `yaml:"service_account_file,omitempty" json:"service_account_file,omitempty" mapstructure:"service_account_file"`
	UseApplicationDefaultCredentials bool     `yaml:"use_application_default_credentials,omitempty" json:"use_application_default_credentials,omitempty" mapstructure:"use_application_default_credentials"`
	ProjectID                        string   `yaml:"project_id,omitempty" json:"project_id" mapstructure:"project_id"`
	Region                           string   `yaml:"region" json:"region" mapstructure:"region"`
	Workspace                        string   `yaml:"workspace" json:"workspace" mapstructure:"workspace"`
	ExecutionRole                    string   `yaml:"execution_role" json:"execution_role" mapstructure:"execution_role"`
	SubnetworkURI                    string   `yaml:"subnetwork_uri,omitempty" json:"subnetwork_uri,omitempty" mapstructure:"subnetwork_uri"`
	NetworkTags                      []string `yaml:"network_tags,omitempty" json:"network_tags,omitempty" mapstructure:"network_tags"`
	KmsKey                           string   `yaml:"kms_key,omitempty" json:"kms_key,omitempty" mapstructure:"kms_key"`
	StagingBucket                    string   `yaml:"staging_bucket,omitempty" json:"staging_bucket,omitempty" mapstructure:"staging_bucket"`
	MetastoreService                 string   `yaml:"metastore_service,omitempty" json:"metastore_service,omitempty" mapstructure:"metastore_service"`
}

func (c DataprocServerlessConnection) GetName() string {
	return c.Name
}

type GoogleAnalyticsConnection struct {
	Name               string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ServiceAccountFile string `yaml:"service_account_file,omitempty" json:"service_account_file" mapstructure:"service_account_file"`
	PropertyID         string `yaml:"property_id,omitempty" json:"property_id" mapstructure:"property_id"`
}

func (c GoogleAnalyticsConnection) GetName() string {
	return c.Name
}

type AppLovinConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c AppLovinConnection) GetName() string {
	return c.Name
}

type FrankfurterConnection struct {
	Name string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
}

func (c FrankfurterConnection) GetName() string {
	return c.Name
}

type SalesforceConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Token    string `yaml:"token,omitempty" json:"token" mapstructure:"token"`
	Domain   string `yaml:"domain" json:"domain" mapstructure:"domain"`
}

func (c SalesforceConnection) GetName() string {
	return c.Name
}

type SQLiteConnection struct {
	Name string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Path string `yaml:"path,omitempty" json:"path" mapstructure:"path"`
}

func (c SQLiteConnection) GetName() string {
	return c.Name
}

type DB2Connection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Port     string `yaml:"port,omitempty" json:"port" mapstructure:"port"`
	Database string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
}

func (c DB2Connection) GetName() string {
	return c.Name
}

type OracleConnection struct {
	Name         string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username     string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password     string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host         string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Port         string `yaml:"port,omitempty" json:"port" mapstructure:"port"`
	ServiceName  string `yaml:"service_name,omitempty" json:"service_name" mapstructure:"service_name"`
	SID          string `yaml:"sid,omitempty" json:"sid" mapstructure:"sid"`
	Role         string `yaml:"role,omitempty" json:"role" mapstructure:"role"`
	SSL          bool   `yaml:"ssl,omitempty" json:"ssl" mapstructure:"ssl"`
	SSLVerify    bool   `yaml:"ssl_verify,omitempty" json:"ssl_verify" mapstructure:"ssl_verify"`
	PrefetchRows int    `yaml:"prefetch_rows,omitempty" json:"prefetch_rows" mapstructure:"prefetch_rows"`
	TraceFile    string `yaml:"trace_file,omitempty" json:"trace_file" mapstructure:"trace_file"`
	Wallet       string `yaml:"wallet,omitempty" json:"wallet" mapstructure:"wallet"`
}

func (c OracleConnection) GetName() string {
	return c.Name
}

type PhantombusterConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c PhantombusterConnection) GetName() string {
	return c.Name
}

type ElasticsearchConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username    string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password    string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host        string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Port        int    `yaml:"port,omitempty" json:"port" mapstructure:"port"`
	Secure      string `yaml:"secure,omitempty" json:"secure" mapstructure:"secure" default:"true"`
	VerifyCerts string `yaml:"verify_certs,omitempty" json:"verify_certs" mapstructure:"verify_certs" default:"true"`
}

func (c ElasticsearchConnection) GetName() string {
	return c.Name
}

type SpannerConnection struct {
	Name               string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ProjectID          string `yaml:"project_id,omitempty" json:"project_id" mapstructure:"project_id"`
	InstanceID         string `yaml:"instance_id,omitempty" json:"instance_id" mapstructure:"instance_id"`
	Database           string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
	ServiceAccountJSON string `yaml:"service_account_json,omitempty" json:"service_account_json,omitempty" mapstructure:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file,omitempty" json:"service_account_file,omitempty" mapstructure:"service_account_file"`
}

func (c SpannerConnection) GetName() string {
	return c.Name
}

type SolidgateConnection struct {
	Name      string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	PublicKey string `yaml:"public_key,omitempty" json:"public_key" mapstructure:"public_key"`
	SecretKey string `yaml:"secret_key,omitempty" json:"secret_key" mapstructure:"secret_key"`
}

func (c SolidgateConnection) GetName() string {
	return c.Name
}

type SmartsheetConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessToken string `yaml:"access_token,omitempty" json:"access_token" mapstructure:"access_token"`
}

func (c SmartsheetConnection) GetName() string {
	return c.Name
}

type AttioConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c AttioConnection) GetName() string {
	return c.Name
}

type SFTPConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Host     string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Port     int    `yaml:"port,omitempty" json:"port" mapstructure:"port"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
}

func (c SFTPConnection) GetName() string {
	return c.Name
}

type ISOCPulseConnection struct {
	Name  string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Token string `yaml:"token,omitempty" json:"token" mapstructure:"token"`
}

func (c ISOCPulseConnection) GetName() string {
	return c.Name
}

type InfluxDBConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Host   string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Port   int    `yaml:"port,omitempty" json:"port,omitempty" mapstructure:"port"`
	Token  string `yaml:"token,omitempty" json:"token" mapstructure:"token"`
	Org    string `yaml:"org,omitempty" json:"org" mapstructure:"org"`
	Bucket string `yaml:"bucket,omitempty" json:"bucket" mapstructure:"bucket"`
	Secure string `yaml:"secure,omitempty" json:"secure,omitempty" mapstructure:"secure" default:"true"`
}

func (c InfluxDBConnection) GetName() string {
	return c.Name
}

type TableauConnection struct {
	Name                      string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Host                      string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Username                  string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password                  string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	PersonalAccessTokenName   string `yaml:"personal_access_token_name,omitempty" json:"personal_access_token_name" mapstructure:"personal_access_token_name"`
	PersonalAccessTokenSecret string `yaml:"personal_access_token_secret,omitempty" json:"personal_access_token_secret" mapstructure:"personal_access_token_secret"`
	SiteID                    string `yaml:"site_id,omitempty" json:"site_id" mapstructure:"site_id"`
	APIVersion                string `yaml:"api_version,omitempty" json:"api_version" mapstructure:"api_version"`
}

type TrinoConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Host     string `yaml:"host" json:"host" mapstructure:"host"`
	Port     int    `yaml:"port" json:"port" mapstructure:"port"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password,omitempty" mapstructure:"password"`
	Catalog  string `yaml:"catalog" json:"catalog" mapstructure:"catalog"`
	Schema   string `yaml:"schema" json:"schema" mapstructure:"schema"`
}

func (c TrinoConnection) GetName() string {
	return c.Name
}

func (c TableauConnection) GetName() string {
	return c.Name
}

type FluxxConnection struct {
	Name         string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Instance     string `yaml:"instance,omitempty" json:"instance" mapstructure:"instance"`
	ClientID     string `yaml:"client_id,omitempty" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret,omitempty" json:"client_secret" mapstructure:"client_secret"`
}

func (c FluxxConnection) GetName() string {
	return c.Name
}

type FreshdeskConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Domain string `yaml:"domain,omitempty" json:"domain" mapstructure:"domain"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c FreshdeskConnection) GetName() string {
	return c.Name
}

type FundraiseUpConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c FundraiseUpConnection) GetName() string {
	return c.Name
}

type FirefliesConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c FirefliesConnection) GetName() string {
	return c.Name
}

type JiraConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Domain   string `yaml:"domain,omitempty" json:"domain" mapstructure:"domain"`
	Email    string `yaml:"email,omitempty" json:"email" mapstructure:"email"`
	APIToken string `yaml:"api_token,omitempty" json:"api_token" mapstructure:"api_token"`
}

func (c JiraConnection) GetName() string {
	return c.Name
}

type MondayConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIToken string `yaml:"api_token,omitempty" json:"api_token" mapstructure:"api_token"`
}

func (c MondayConnection) GetName() string {
	return c.Name
}

type PlusVibeAIConnection struct {
	Name        string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey      string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
	WorkspaceID string `yaml:"workspace_id,omitempty" json:"workspace_id" mapstructure:"workspace_id"`
}

func (c PlusVibeAIConnection) GetName() string {
	return c.Name
}

type BruinCloudConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIToken string `yaml:"api_token,omitempty" json:"api_token" mapstructure:"api_token"`
}

func (c BruinCloudConnection) GetName() string {
	return c.Name
}

type PrimerConnection struct {
	Name   string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key,omitempty" json:"api_key" mapstructure:"api_key"`
}

func (c PrimerConnection) GetName() string {
	return c.Name
}

type IndeedConnection struct {
	Name         string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ClientID     string `yaml:"client_id,omitempty" json:"client_id" mapstructure:"client_id"`
	ClientSecret string `yaml:"client_secret,omitempty" json:"client_secret" mapstructure:"client_secret"`
	EmployerID   string `yaml:"employer_id,omitempty" json:"employer_id" mapstructure:"employer_id"`
}

func (c IndeedConnection) GetName() string {
	return c.Name
}
