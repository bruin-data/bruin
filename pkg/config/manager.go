package config

import (
	"encoding/json"
	"errors"
	"fmt"
	fs2 "io/fs"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/git"
	path2 "github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/go-viper/mapstructure/v2"
	"github.com/invopop/jsonschema"
	errors2 "github.com/pkg/errors"
	"github.com/spf13/afero"
)

type Connections struct {
	AwsConnection       []AwsConnection                 `yaml:"aws,omitempty" json:"aws,omitempty" mapstructure:"aws"`
	AthenaConnection    []AthenaConnection              `yaml:"athena,omitempty" json:"athena,omitempty" mapstructure:"athena"`
	GoogleCloudPlatform []GoogleCloudPlatformConnection `yaml:"google_cloud_platform,omitempty" json:"google_cloud_platform,omitempty" mapstructure:"google_cloud_platform"`
	Snowflake           []SnowflakeConnection           `yaml:"snowflake,omitempty" json:"snowflake,omitempty" mapstructure:"snowflake"`
	Postgres            []PostgresConnection            `yaml:"postgres,omitempty" json:"postgres,omitempty" mapstructure:"postgres"`
	RedShift            []RedshiftConnection            `yaml:"redshift,omitempty" json:"redshift,omitempty" mapstructure:"redshift"`
	MsSQL               []MsSQLConnection               `yaml:"mssql,omitempty" json:"mssql,omitempty" mapstructure:"mssql"`
	Databricks          []DatabricksConnection          `yaml:"databricks,omitempty" json:"databricks,omitempty" mapstructure:"databricks"`
	Synapse             []SynapseConnection             `yaml:"synapse,omitempty" json:"synapse,omitempty" mapstructure:"synapse"`
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
	Stripe              []StripeConnection              `yaml:"stripe,omitempty" json:"stripe,omitempty" mapstructure:"stripe"`
	Appsflyer           []AppsflyerConnection           `yaml:"appsflyer,omitempty" json:"appsflyer,omitempty" mapstructure:"appsflyer"`
	Kafka               []KafkaConnection               `yaml:"kafka,omitempty" json:"kafka,omitempty" mapstructure:"kafka"`
	DuckDB              []DuckDBConnection              `yaml:"duckdb,omitempty" json:"duckdb,omitempty" mapstructure:"duckdb"`
	Hubspot             []HubspotConnection             `yaml:"hubspot,omitempty" json:"hubspot,omitempty" mapstructure:"hubspot"`
	GoogleSheets        []GoogleSheetsConnection        `yaml:"google_sheets,omitempty" json:"google_sheets,omitempty" mapstructure:"google_sheets"`
	Chess               []ChessConnection               `yaml:"chess,omitempty" json:"chess,omitempty" mapstructure:"chess"`
	Airtable            []AirtableConnection            `yaml:"airtable,omitempty" json:"airtable,omitempty" mapstructure:"airtable"`
	Zendesk             []ZendeskConnection             `yaml:"zendesk,omitempty" json:"zendesk,omitempty" mapstructure:"zendesk"`
	S3                  []S3Connection                  `yaml:"s3,omitempty" json:"s3,omitempty" mapstructure:"s3"`
	Slack               []SlackConnection               `yaml:"slack,omitempty" json:"slack,omitempty" mapstructure:"slack"`

	byKey       map[string]any
	typeNameMap map[string]string
}

func (c *Connections) ConnectionsSummaryList() map[string]string {
	if c.typeNameMap == nil {
		c.buildConnectionKeyMap()
	}

	return c.typeNameMap
}

func GetConnectionsSchema() (string, error) {
	schema := jsonschema.Reflect(&Connections{})
	jsonStringSchema, err := json.Marshal(schema)
	if err != nil {
		return "", err
	}

	return string(jsonStringSchema), nil
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

	for i, conn := range c.AthenaConnection {
		c.byKey[conn.Name] = &(c.AthenaConnection[i])
		c.typeNameMap[conn.Name] = "athena"
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

	for i, conn := range c.Stripe {
		c.byKey[conn.Name] = &(c.Stripe[i])
		c.typeNameMap[conn.Name] = "stripe"
	}

	for i, conn := range c.Appsflyer {
		c.byKey[conn.Name] = &(c.Appsflyer[i])
		c.typeNameMap[conn.Name] = "appsflyer"
	}

	for i, conn := range c.Kafka {
		c.byKey[conn.Name] = &(c.Kafka[i])
		c.typeNameMap[conn.Name] = "kafka"
	}

	for i, conn := range c.DuckDB {
		c.byKey[conn.Name] = &(c.DuckDB[i])
		c.typeNameMap[conn.Name] = "duckdb"
	}

	for i, conn := range c.Hubspot {
		c.byKey[conn.Name] = &(c.Hubspot[i])
		c.typeNameMap[conn.Name] = "hubspot"
	}

	for i, conn := range c.GoogleSheets {
		c.byKey[conn.Name] = &(c.GoogleSheets[i])
		c.typeNameMap[conn.Name] = "google_sheets"
	}

	for i, conn := range c.Chess {
		c.byKey[conn.Name] = &(c.Chess[i])
		c.typeNameMap[conn.Name] = "chess"
	}

	for i, conn := range c.Airtable {
		c.byKey[conn.Name] = &(c.Airtable[i])
		c.typeNameMap[conn.Name] = "airtable"
	}

	for i, conn := range c.Zendesk {
		c.byKey[conn.Name] = &(c.Zendesk[i])
		c.typeNameMap[conn.Name] = "zendesk"
	}

	for i, conn := range c.S3 {
		c.byKey[conn.Name] = &(c.S3[i])
		c.typeNameMap[conn.Name] = "s3"
	}

	for i, conn := range c.Slack {
		c.byKey[conn.Name] = &(c.Slack[i])
		c.typeNameMap[conn.Name] = "slack"
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

func (c *Config) CanRunPipeline(p *pipeline.Pipeline) error {
	for _, asset := range p.Assets {
		connName, err := p.GetConnectionNameForAsset(asset)
		if err != nil {
			return errors2.Wrap(err, "Could not find connection name for asset "+asset.Name)
		}

		if asset.Type == pipeline.AssetTypePython {
			if asset.Connection == "" && asset.CheckCount() == 0 && len(asset.Secrets) == 0 {
				continue
			}
		}

		if !c.SelectedEnvironment.Connections.Exists(connName) {
			return errors2.Errorf("Connection '%s' does not exist in the selected environment and is needed for '%s'", connName, asset.Name)
		}
	}

	return nil
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
		var conn RedshiftConnection
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
		var conn SynapseConnection
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
	case "stripe":
		var conn StripeConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Stripe = append(env.Connections.Stripe, conn)
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
	case "appsflyer":
		var conn AppsflyerConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Appsflyer = append(env.Connections.Appsflyer, conn)
	case "kafka":
		var conn KafkaConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Kafka = append(env.Connections.Kafka, conn)
	case "hubspot":
		var conn HubspotConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Hubspot = append(env.Connections.Hubspot, conn)
	case "google_sheets":
		var conn GoogleSheetsConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.GoogleSheets = append(env.Connections.GoogleSheets, conn)
	case "chess":
		var conn ChessConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Chess = append(env.Connections.Chess, conn)
	case "airtable":
		var conn AirtableConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Airtable = append(env.Connections.Airtable, conn)
	case "zendesk":
		var conn ZendeskConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Zendesk = append(env.Connections.Zendesk, conn)
	case "s3":
		var conn S3Connection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.S3 = append(env.Connections.S3, conn)
	case "slack":
		var conn SlackConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Slack = append(env.Connections.Slack, conn)
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
	case "stripe":
		env.Connections.Stripe = removeConnection(env.Connections.Stripe, connectionName)
	case "appsflyer":
		env.Connections.Appsflyer = removeConnection(env.Connections.Appsflyer, connectionName)
	case "kafka":
		env.Connections.Kafka = removeConnection(env.Connections.Kafka, connectionName)
	case "hubspot":
		env.Connections.Hubspot = removeConnection(env.Connections.Hubspot, connectionName)
	case "google_sheets":
		env.Connections.GoogleSheets = removeConnection(env.Connections.GoogleSheets, connectionName)
	case "chess":
		env.Connections.Chess = removeConnection(env.Connections.Chess, connectionName)
	case "airtable":
		env.Connections.Airtable = removeConnection(env.Connections.Airtable, connectionName)
	case "s3":
		env.Connections.S3 = removeConnection(env.Connections.S3, connectionName)
	case "slack":
		env.Connections.Slack = removeConnection(env.Connections.Slack, connectionName)
	case "zendesk":
		env.Connections.Zendesk = removeConnection(env.Connections.Zendesk, connectionName)
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
