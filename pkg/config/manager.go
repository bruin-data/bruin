package config

import (
	"encoding/json"
	errors "errors"
	"fmt"
	fs2 "io/fs"
	"os"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/git"
	path2 "github.com/bruin-data/bruin/pkg/path"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/afero"
	"golang.org/x/oauth2/google"
)

type PostgresConnection struct {
	Name         string `yaml:"name" json:"name" mapstructure:"name"`
	Username     string `yaml:"username" json:"username" mapstructure:"username"`
	Password     string `yaml:"password" json:"password" mapstructure:"password"`
	Host         string `yaml:"host" json:"host" mapstructure:"host"`
	Port         int    `yaml:"port" json:"port" mapstructure:"port"`
	Database     string `yaml:"database" json:"database" mapstructure:"database"`
	Schema       string `yaml:"schema" json:"schema" mapstructure:"schema"`
	PoolMaxConns int    `yaml:"pool_max_conns" json:"pool_max_conns" mapstructure:"pool_max_conns" default:"10"`
	SslMode      string `yaml:"ssl_mode" json:"ssl_mode" mapstructure:"ssl_mode" default:"disable"`
}

func (c PostgresConnection) GetName() string {
	return c.Name
}

type MsSQLConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Host     string `yaml:"host"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port"     json:"port" mapstructure:"port"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`
}

func (c MsSQLConnection) GetName() string {
	return c.Name
}

type DatabricksConnection struct {
	Name  string `yaml:"name"  json:"name" mapstructure:"name"`
	Token string `yaml:"token" json:"token" mapstructure:"token"`
	Path  string `yaml:"path"  json:"path" mapstructure:"path"`
	Host  string `yaml:"host"  json:"host" mapstructure:"host"`
	Port  int    `yaml:"port"  json:"port" mapstructure:"port"`
}

func (c DatabricksConnection) GetName() string {
	return c.Name
}

type GoogleCloudPlatformConnection struct {
	Name               string `yaml:"name" json:"name" mapstructure:"name"`
	ServiceAccountJSON string `yaml:"service_account_json" json:"service_account_json" mapstructure:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file" json:"service_account_file" mapstructure:"service_account_file"`
	ProjectID          string `yaml:"project_id" json:"project_id" mapstructure:"project_id"`
	Location           string `yaml:"location" json:"location" mapstructure:"location"`
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

type MongoConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Host     string `yaml:"host"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port"     json:"port" mapstructure:"port"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`
}

func (c MongoConnection) GetName() string {
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

type AwsConnection struct {
	Name      string `yaml:"name" json:"name" mapstructure:"name"`
	AccessKey string `yaml:"access_key" json:"access_key" mapstructure:"access_key"`
	SecretKey string `yaml:"secret_key" json:"secret_key" mapstructure:"secret_key"`
}

func (c AwsConnection) GetName() string {
	return c.Name
}

type AthenaConnection struct {
	Name             string `yaml:"name" json:"name" mapstructure:"name"`
	AccessKey        string `yaml:"access_key" json:"access_key" mapstructure:"access_key"`
	SecretKey        string `yaml:"secret_key" json:"secret_key" mapstructure:"secret_key"`
	QueryResultsPath string `yaml:"query_results_path" json:"query_results_path" mapstructure:"query_results_path"`
	Region           string `yaml:"region" json:"region" mapstructure:"region"`
	Database         string `yaml:"database" json:"database" mapstructure:"database"`
}

func (c AthenaConnection) GetName() string {
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

type MySQLConnection struct {
	Name     string `yaml:"name" json:"name" mapstructure:"name"`
	Username string `yaml:"username" json:"username" mapstructure:"username"`
	Password string `yaml:"password" json:"password" mapstructure:"password"`
	Host     string `yaml:"host"     json:"host" mapstructure:"host"`
	Port     int    `yaml:"port"     json:"port" mapstructure:"port"`
	Database string `yaml:"database" json:"database" mapstructure:"database"`
	Driver   string `yaml:"driver" json:"driver" mapstructure:"driver"`
}

func (c MySQLConnection) GetName() string {
	return c.Name
}

type NotionConnection struct {
	Name   string `yaml:"name" json:"name" mapstructure:"name"`
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c NotionConnection) GetName() string {
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

type Connections struct {
	AwsConnection       []AwsConnection                 `yaml:"aws,omitempty" json:"aws,omitempty" mapstructure:"aws"`
	AthenaConnection    []AthenaConnection              `yaml:"athena,omitempty" json:"athena,omitempty" mapstructure:"athena"`
	GoogleCloudPlatform []GoogleCloudPlatformConnection `yaml:"google_cloud_platform,omitempty" json:"google_cloud_platform,omitempty" mapstructure:"google_cloud_platform"`
	Snowflake           []SnowflakeConnection           `yaml:"snowflake,omitempty" json:"snowflake,omitempty" mapstructure:"snowflake"`
	Postgres            []PostgresConnection            `yaml:"postgres,omitempty" json:"postgres,omitempty" mapstructure:"postgres"`
	RedShift            []PostgresConnection            `yaml:"redshift,omitempty" json:"redshift,omitempty" mapstructure:"redshift"`
	MsSQL               []MsSQLConnection               `yaml:"mssql,omitempty" json:"mssql,omitempty" mapstructure:"mssql"`
	Databricks          []DatabricksConnection          `yaml:"databricks,omitempty" json:"databricks,omitempty" mapstructure:"databricks"`
	Synapse             []MsSQLConnection               `yaml:"synapse,omitempty" json:"synapse,omitempty" mapstructure:"synapse"`
	Mongo               []MongoConnection               `yaml:"mongo,omitempty" json:"mongo,omitempty" mapstructure:"mongo"`
	MySQL               []MySQLConnection               `yaml:"mysql,omitempty" json:"mysql,omitempty" mapstructure:"mysql"`
	Notion              []NotionConnection              `yaml:"notion,omitempty" json:"notion,omitempty" mapstructure:"notion"`
	HANA                []HANAConnection                `yaml:"hana,omitempty" json:"hana,omitempty" mapstructure:"hana"`
	Shopify             []ShopifyConnection             `yaml:"shopify,omitempty" json:"shopify,omitempty" mapstructure:"shopify"`
	Gorgias             []GorgiasConnection             `yaml:"gorgias,omitempty" json:"gorgias,omitempty" mapstructure:"gorgias"`
	Klaviyo             []KlaviyoConnection             `yaml:"klaviyo,omitempty" json:"klaviyo,omitempty" mapstructure:"klaviyo"`
	Adjust              []AdjustConnection              `yaml:"adjust,omitempty" json:"adjust,omitempty" mapstructure:"adjust"`
	Generic             []GenericConnection             `yaml:"generic,omitempty" json:"generic,omitempty" mapstructure:"generic"`
	FacebookAds         []FacebookAdsConnection         `yaml:"facebookads,omitempty" json:"facebookads,omitempty" mapstructure:"facebookads"`

	byKey       map[string]any
	typeNameMap map[string]string
}

func (c *Connections) ConnectionsSummaryList() map[string]string {
	if c.typeNameMap == nil {
		c.buildConnectionKeyMap()
	}

	return c.typeNameMap
}

func (c *Connections) Exists(name string) bool {
	if c.byKey == nil {
		c.buildConnectionKeyMap()
	}

	_, ok := c.typeNameMap[name]
	return ok
}

func (c *Connections) buildConnectionKeyMap() {
	c.byKey = make(map[string]any)
	c.typeNameMap = make(map[string]string)

	for i, conn := range c.AwsConnection {
		c.byKey[conn.Name] = &(c.AwsConnection[i])
		c.typeNameMap[conn.Name] = "aws"
	}

	for i, conn := range c.GoogleCloudPlatform {
		c.byKey[conn.Name] = &(c.GoogleCloudPlatform[i])
		c.typeNameMap[conn.Name] = "google_cloud_platform"
	}

	for i, conn := range c.Snowflake {
		c.byKey[conn.Name] = &(c.Snowflake[i])
		c.typeNameMap[conn.Name] = "snowflake"
	}

	for i, conn := range c.Postgres {
		c.byKey[conn.Name] = &(c.Postgres[i])
		c.typeNameMap[conn.Name] = "postgres"
	}

	for i, conn := range c.RedShift {
		c.byKey[conn.Name] = &(c.RedShift[i])
		c.typeNameMap[conn.Name] = "redshift"
	}

	for i, conn := range c.MsSQL {
		c.byKey[conn.Name] = &(c.MsSQL[i])
		c.typeNameMap[conn.Name] = "mssql"
	}

	for i, conn := range c.Databricks {
		c.byKey[conn.Name] = &(c.Databricks[i])
		c.typeNameMap[conn.Name] = "databricks"
	}

	for i, conn := range c.Synapse {
		c.byKey[conn.Name] = &(c.Synapse[i])
		c.typeNameMap[conn.Name] = "synapse"
	}

	for i, conn := range c.Mongo {
		c.byKey[conn.Name] = &(c.Mongo[i])
		c.typeNameMap[conn.Name] = "mongo"
	}

	for i, conn := range c.MySQL {
		c.byKey[conn.Name] = &(c.MySQL[i])
		c.typeNameMap[conn.Name] = "mysql"
	}

	for i, conn := range c.Notion {
		c.byKey[conn.Name] = &(c.Notion[i])
		c.typeNameMap[conn.Name] = "notion"
	}

	for i, conn := range c.HANA {
		c.byKey[conn.Name] = &(c.HANA[i])
		c.typeNameMap[conn.Name] = "hana"
	}

	for i, conn := range c.Shopify {
		c.byKey[conn.Name] = &(c.Shopify[i])
		c.typeNameMap[conn.Name] = "shopify"
	}

	for i, conn := range c.Gorgias {
		c.byKey[conn.Name] = &(c.Gorgias[i])
		c.typeNameMap[conn.Name] = "gorgias"
	}

	for i, conn := range c.Klaviyo {
		c.byKey[conn.Name] = &(c.Klaviyo[i])
		c.typeNameMap[conn.Name] = "klaviyo"
	}

	for i, conn := range c.Adjust {
		c.byKey[conn.Name] = &(c.Adjust[i])
		c.typeNameMap[conn.Name] = "adjust"
	}

	for i, conn := range c.Generic {
		c.byKey[conn.Name] = &(c.Generic[i])
		c.typeNameMap[conn.Name] = "generic"
	}

	for i, conn := range c.FacebookAds {
		c.byKey[conn.Name] = &(c.FacebookAds[i])
		c.typeNameMap[conn.Name] = "facebookads"
	}
}

type Environment struct {
	Connections *Connections `yaml:"connections" json:"connections" mapstructure:"connections"`
}

func (e *Environment) GetSecretByKey(key string) (string, error) {
	v, ok := e.Connections.byKey[key]
	if !ok {
		return "", nil
	}

	if v, ok := v.(*GenericConnection); ok {
		return v.Value, nil
	}

	res, err := json.Marshal(v)
	return string(res), err
}

type Config struct {
	fs   afero.Fs
	path string

	DefaultEnvironmentName  string                 `yaml:"default_environment" json:"default_environment_name" mapstructure:"default_environment_name"`
	SelectedEnvironmentName string                 `yaml:"-" json:"selected_environment_name" mapstructure:"selected_environment_name"`
	SelectedEnvironment     *Environment           `yaml:"-" json:"selected_environment" mapstructure:"selected_environment"`
	Environments            map[string]Environment `yaml:"environments" json:"environments" mapstructure:"environments"`
}

func (c *Config) GetEnvironmentNames() []string {
	envs := make([]string, len(c.Environments))
	i := 0
	for name := range c.Environments {
		envs[i] = name
		i++
	}

	return envs
}

func (c *Config) GetSecretByKey(key string) (string, error) {
	return c.SelectedEnvironment.GetSecretByKey(key)
}

func (c *Config) Persist() error {
	return c.PersistToFs(c.fs)
}

func (c *Config) PersistToFs(fs afero.Fs) error {
	return path2.WriteYaml(fs, c.path, c)
}

func (c *Config) SelectEnvironment(name string) error {
	e, ok := c.Environments[name]
	if !ok {
		return fmt.Errorf("environment '%s' not found in the configuration file", name)
	}

	c.SelectedEnvironment = &e
	c.SelectedEnvironmentName = name
	c.SelectedEnvironment.Connections.buildConnectionKeyMap()
	return nil
}

func LoadFromFile(fs afero.Fs, path string) (*Config, error) {
	var config Config

	err := path2.ReadYaml(fs, path, &config)
	if err != nil {
		return nil, err
	}

	config.fs = fs
	config.path = path

	if config.DefaultEnvironmentName == "" {
		config.DefaultEnvironmentName = "default"
	}

	err = config.SelectEnvironment(config.DefaultEnvironmentName)
	if err != nil {
		return nil, fmt.Errorf("failed to select default environment: %w", err)
	}

	return &config, nil
}

func LoadOrCreate(fs afero.Fs, path string) (*Config, error) {
	config, err := LoadFromFile(fs, path)
	if err != nil && !errors.Is(err, fs2.ErrNotExist) {
		return nil, err
	}

	if err == nil {
		return config, ensureConfigIsInGitignore(fs, path)
	}

	defaultEnv := Environment{
		Connections: &Connections{},
	}
	config = &Config{
		fs:   fs,
		path: path,

		DefaultEnvironmentName:  "default",
		SelectedEnvironment:     &defaultEnv,
		SelectedEnvironmentName: "default",
		Environments: map[string]Environment{
			"default": defaultEnv,
		},
	}

	err = config.Persist()
	if err != nil {
		return nil, fmt.Errorf("failed to persist config: %w", err)
	}

	config.SelectedEnvironment.Connections.buildConnectionKeyMap()

	return config, ensureConfigIsInGitignore(fs, path)
}

func ensureConfigIsInGitignore(fs afero.Fs, filePath string) error {
	return git.EnsureGivenPatternIsInGitignore(fs, filepath.Dir(filePath), filepath.Base(filePath))
}

func (c *Config) AddConnection(environmentName, name, connType string, creds map[string]interface{}) error {
	// Check if the environment exists
	env, exists := c.Environments[environmentName]
	if !exists {
		return fmt.Errorf("environment '%s' does not exist", environmentName)
	}

	switch connType {
	case "aws":
		var conn AwsConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.AwsConnection = append(env.Connections.AwsConnection, conn)
	case "google_cloud_platform":
		var conn GoogleCloudPlatformConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.GoogleCloudPlatform = append(env.Connections.GoogleCloudPlatform, conn)
	case "athena":
		var conn AthenaConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.AthenaConnection = append(env.Connections.AthenaConnection, conn)
	case "snowflake":
		var conn SnowflakeConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Snowflake = append(env.Connections.Snowflake, conn)
	case "postgres":
		var conn PostgresConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Postgres = append(env.Connections.Postgres, conn)
	case "redshift":
		var conn PostgresConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.RedShift = append(env.Connections.RedShift, conn)
	case "mssql":
		var conn MsSQLConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.MsSQL = append(env.Connections.MsSQL, conn)
	case "databricks":
		var conn DatabricksConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Databricks = append(env.Connections.Databricks, conn)
	case "synapse":
		var conn MsSQLConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Synapse = append(env.Connections.Synapse, conn)
	case "mongo":
		var conn MongoConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Mongo = append(env.Connections.Mongo, conn)
	case "mysql":
		var conn MySQLConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.MySQL = append(env.Connections.MySQL, conn)
	case "notion":
		var conn NotionConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Notion = append(env.Connections.Notion, conn)
	case "hana":
		var conn HANAConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.HANA = append(env.Connections.HANA, conn)
	case "shopify":
		var conn ShopifyConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Shopify = append(env.Connections.Shopify, conn)
	case "gorgias":
		var conn GorgiasConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Gorgias = append(env.Connections.Gorgias, conn)
	case "klaviyo":
		var conn KlaviyoConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Klaviyo = append(env.Connections.Klaviyo, conn)
	case "adjust":
		var conn AdjustConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Adjust = append(env.Connections.Adjust, conn)
	case "generic":
		var conn GenericConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Generic = append(env.Connections.Generic, conn)
	case "facebookads":
		var conn FacebookAdsConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.FacebookAds = append(env.Connections.FacebookAds, conn)
	default:
		return fmt.Errorf("unsupported connection type: %s", connType)
	}

	// Update the environment in the config
	c.Environments[environmentName] = env
	if environmentName == c.SelectedEnvironmentName {
		c.SelectedEnvironment = &env
		c.SelectedEnvironment.Connections.buildConnectionKeyMap()
	}

	return nil
}

func (c *Config) DeleteConnection(environmentName, connectionName string) error {
	err := c.SelectEnvironment(environmentName)
	if err != nil {
		return err
	}

	env, exists := c.Environments[environmentName]
	if !exists {
		return fmt.Errorf("environment '%s' does not exist", environmentName)
	}

	connType, exists := env.Connections.typeNameMap[connectionName]
	if !exists {
		return fmt.Errorf("connection '%s' does not exist in environment '%s'", connectionName, environmentName)
	}

	switch connType {
	case "google_cloud_platform":
		env.Connections.GoogleCloudPlatform = removeConnection(env.Connections.GoogleCloudPlatform, connectionName)
	case "mssql":
		env.Connections.MsSQL = removeConnection(env.Connections.MsSQL, connectionName)
	case "databricks":
		env.Connections.Databricks = removeConnection(env.Connections.Databricks, connectionName)
	case "mongo":
		env.Connections.Mongo = removeConnection(env.Connections.Mongo, connectionName)
	case "aws":
		env.Connections.AwsConnection = removeConnection(env.Connections.AwsConnection, connectionName)
	case "athena":
		env.Connections.AthenaConnection = removeConnection(env.Connections.AthenaConnection, connectionName)
	case "postgres":
		env.Connections.Postgres = removeConnection(env.Connections.Postgres, connectionName)
	case "redshift":
		env.Connections.RedShift = removeConnection(env.Connections.RedShift, connectionName)
	case "mysql":
		env.Connections.MySQL = removeConnection(env.Connections.MySQL, connectionName)
	case "notion":
		env.Connections.Notion = removeConnection(env.Connections.Notion, connectionName)
	case "hana":
		env.Connections.HANA = removeConnection(env.Connections.HANA, connectionName)
	case "shopify":
		env.Connections.Shopify = removeConnection(env.Connections.Shopify, connectionName)
	case "snowflake":
		env.Connections.Snowflake = removeConnection(env.Connections.Snowflake, connectionName)
	case "synapse":
		env.Connections.Synapse = removeConnection(env.Connections.Synapse, connectionName)
	case "gorgias":
		env.Connections.Gorgias = removeConnection(env.Connections.Gorgias, connectionName)
	case "klaviyo":
		env.Connections.Klaviyo = removeConnection(env.Connections.Klaviyo, connectionName)
	case "adjust":
		env.Connections.Adjust = removeConnection(env.Connections.Adjust, connectionName)
	case "generic":
		env.Connections.Generic = removeConnection(env.Connections.Generic, connectionName)
	case "facebookads":
		env.Connections.FacebookAds = removeConnection(env.Connections.FacebookAds, connectionName)
	default:
		return fmt.Errorf("unsupported connection type: %s", connType)
	}

	// Update the environment in the config
	c.Environments[environmentName] = env
	if environmentName == c.SelectedEnvironmentName {
		c.SelectedEnvironment = &env
		c.SelectedEnvironment.Connections.buildConnectionKeyMap()
	}

	delete(env.Connections.typeNameMap, connectionName)

	return nil
}

type Named interface {
	GetName() string
}

func removeConnection[T interface{ GetName() string }](connections []T, name string) []T {
	for i, conn := range connections {
		if conn.GetName() == name {
			return append(connections[:i], connections[i+1:]...)
		}
	}
	return connections
}
