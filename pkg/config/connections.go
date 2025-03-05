package config

import (
	"encoding/json"
	"os"

	"golang.org/x/oauth2/google"
)

type AwsConnection struct {
	Name      string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessKey string `yaml:"access_key,omitempty" json:"access_key" mapstructure:"access_key"`
	SecretKey string `yaml:"secret_key,omitempty" json:"secret_key" mapstructure:"secret_key"`
}

func (c AwsConnection) GetName() string {
	return c.Name
}

type GoogleCloudPlatformConnection struct { //nolint:recvcheck
	Name               string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ServiceAccountJSON string `yaml:"service_account_json,omitempty" json:"service_account_json,omitempty" mapstructure:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file,omitempty" json:"service_account_file,omitempty" mapstructure:"service_account_file"`
	ProjectID          string `yaml:"project_id,omitempty" json:"project_id" mapstructure:"project_id"`
	Location           string `yaml:"location,omitempty" json:"location,omitempty" mapstructure:"location"`
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
	Name             string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	AccessKey        string `yaml:"access_key_id,omitempty" json:"access_key_id" mapstructure:"access_key_id"`
	SecretKey        string `yaml:"secret_access_key,omitempty" json:"secret_access_key" mapstructure:"secret_access_key"`
	QueryResultsPath string `yaml:"query_results_path,omitempty" json:"query_results_path" mapstructure:"query_results_path"`
	Region           string `yaml:"region,omitempty" json:"region" mapstructure:"region"`
	Database         string `yaml:"database,omitempty" json:"database,omitempty" mapstructure:"database"`
}

func (c AthenaConnection) GetName() string {
	return c.Name
}

type SynapseConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port,omitempty"     json:"port" mapstructure:"port" jsonschema:"default=1433"`
	Database string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
}

func (c SynapseConnection) GetName() string {
	return c.Name
}

type DatabricksConnection struct {
	Name    string `yaml:"name,omitempty"  json:"name" mapstructure:"name"`
	Token   string `yaml:"token,omitempty" json:"token" mapstructure:"token"`
	Path    string `yaml:"path,omitempty"  json:"path" mapstructure:"path"`
	Host    string `yaml:"host,omitempty"  json:"host" mapstructure:"host"`
	Port    int    `yaml:"port,omitempty"  json:"port" mapstructure:"port" jsonschema:"default=443"`
	Catalog string `yaml:"catalog,omitempty"  json:"catalog" mapstructure:"catalog"`
	Schema  string `yaml:"schema,omitempty"  json:"schema" mapstructure:"schema"`
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

type MsSQLConnection struct {
	Name     string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host     string `yaml:"host,omitempty"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port,omitempty"     json:"port" mapstructure:"port" jsonschema:"default=1433"`
	Database string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
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
	Schema       string `yaml:"schema,omitempty" json:"schema" mapstructure:"schema"`
	PoolMaxConns int    `yaml:"pool_max_conns,omitempty" json:"pool_max_conns" mapstructure:"pool_max_conns" default:"10"`
	SslMode      string `yaml:"ssl_mode,omitempty" json:"ssl_mode" mapstructure:"ssl_mode" default:"disable"`
}

func (c PostgresConnection) GetName() string {
	return c.Name
}

type RedshiftConnection struct {
	Name         string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Username     string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password     string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Host         string `yaml:"host,omitempty" json:"host" mapstructure:"host"`
	Port         int    `yaml:"port,omitempty" json:"port" mapstructure:"port" jsonschema:"default=5439"`
	Database     string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
	Schema       string `yaml:"schema,omitempty" json:"schema" mapstructure:"schema"`
	PoolMaxConns int    `yaml:"pool_max_conns,omitempty" json:"pool_max_conns" mapstructure:"pool_max_conns" default:"10"`
	SslMode      string `yaml:"ssl_mode,omitempty" json:"ssl_mode" mapstructure:"ssl_mode" default:"disable"`
}

func (c RedshiftConnection) GetName() string {
	return c.Name
}

type SnowflakeConnection struct {
	Name           string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	Account        string `yaml:"account,omitempty" json:"account" mapstructure:"account"`
	Username       string `yaml:"username,omitempty" json:"username" mapstructure:"username"`
	Password       string `yaml:"password,omitempty" json:"password" mapstructure:"password"`
	Region         string `yaml:"region,omitempty" json:"region" mapstructure:"region"`
	Role           string `yaml:"role,omitempty" json:"role" mapstructure:"role"`
	Database       string `yaml:"database,omitempty" json:"database" mapstructure:"database"`
	Schema         string `yaml:"schema,omitempty" json:"schema" mapstructure:"schema"`
	Warehouse      string `yaml:"warehouse,omitempty" json:"warehouse" mapstructure:"warehouse"`
	PrivateKeyPath string `yaml:"private_key_path,omitempty" json:"private_key_path" mapstructure:"private_key_path"`
}

func (c SnowflakeConnection) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{
		"name":             c.Name,
		"account":          c.Account,
		"username":         c.Username,
		"password":         c.Password,
		"region":           c.Region,
		"role":             c.Role,
		"database":         c.Database,
		"schema":           c.Schema,
		"warehouse":        c.Warehouse,
		"private_key_path": c.PrivateKeyPath,
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
	Players []string `yaml:"players,omitempty" json:"players" mapstructure:"players" jsonschema:"default=MagnusCarlsen,default=HikaruNakamura,default=ArjunErigaisi"`
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

type GoogleAdsConnection struct {
	Name               string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	CustomerID         string `yaml:"customer_id,omitempty" json:"customer_id" mapstructure:"customer_id"`
	ServiceAccountJSON string `yaml:"service_account_json,omitempty" json:"service_account_json,omitempty" mapstructure:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file,omitempty" json:"service_account_file,omitempty" mapstructure:"service_account_file"`
	DeveloperToken     string `yaml:"dev_token,omitempty" json:"dev_token" mapstructure:"dev_token"`
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

type GCSConnection struct {
	Name               string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ServiceAccountFile string `yaml:"service_account_file,omitempty" json:"service_account_file" mapstructure:"service_account_file"`
	ServiceAccountJSON string `yaml:"service_account_json,omitempty" json:"service_account_json" mapstructure:"service_account_json"`
}

func (c GCSConnection) GetName() string {
	return c.Name
}

type KinesisConnection struct {
	Name            string `yaml:"name,omitempty" json:"name" mapstructure:"name"`
	ACCESSKEYID     string `yaml:"access_key_id,omitempty" json:"access_key_id" mapstructure:"access_key_id"`
	SECRETACCESSKEY string `yaml:"secret_access_key,omitempty" json:"secret_access_key" mapstructure:"secret_access_key"`
	Region          string `yaml:"region,omitempty" json:"region" mapstructure:"region"`
}

func (c KinesisConnection) GetName() string {
	return c.Name
}
