package config

import (
	"encoding/json"
	"errors"
	"fmt"
	fs2 "io/fs"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
	path2 "github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
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
	ClickHouse          []ClickHouseConnection          `yaml:"clickhouse,omitempty" json:"clickhouse,omitempty" mapstructure:"clickhouse"`
	Hubspot             []HubspotConnection             `yaml:"hubspot,omitempty" json:"hubspot,omitempty" mapstructure:"hubspot"`
	GitHub              []GitHubConnection              `yaml:"github,omitempty" json:"github,omitempty" mapstructure:"github"`
	GoogleSheets        []GoogleSheetsConnection        `yaml:"google_sheets,omitempty" json:"google_sheets,omitempty" mapstructure:"google_sheets"`
	Chess               []ChessConnection               `yaml:"chess,omitempty" json:"chess,omitempty" mapstructure:"chess"`
	Airtable            []AirtableConnection            `yaml:"airtable,omitempty" json:"airtable,omitempty" mapstructure:"airtable"`
	Zendesk             []ZendeskConnection             `yaml:"zendesk,omitempty" json:"zendesk,omitempty" mapstructure:"zendesk"`
	TikTokAds           []TikTokAdsConnection           `yaml:"tiktokads,omitempty" json:"tiktokads,omitempty" mapstructure:"tiktokads"`
	S3                  []S3Connection                  `yaml:"s3,omitempty" json:"s3,omitempty" mapstructure:"s3"`
	Slack               []SlackConnection               `yaml:"slack,omitempty" json:"slack,omitempty" mapstructure:"slack"`
	Asana               []AsanaConnection               `yaml:"asana,omitempty" json:"asana,omitempty" mapstructure:"asana"`
	DynamoDB            []DynamoDBConnection            `yaml:"dynamodb,omitempty" json:"dynamodb,omitempty" mapstructure:"dynamodb"`
	GoogleAds           []GoogleAdsConnection           `yaml:"googleads,omitempty" json:"googleads,omitempty" mapstructure:"googleads"`
	AppStore            []AppStoreConnection            `yaml:"appstore,omitempty" json:"appstore,omitempty" mapstructure:"appstore"`
	LinkedInAds         []LinkedInAdsConnection         `yaml:"linkedinads,omitempty" json:"linkedinads,omitempty" mapstructure:"linkedinads"`
	GCS                 []GCSConnection                 `yaml:"gcs,omitempty" json:"gcs,omitempty" mapstructure:"gcs"`
	ApplovinMax         []ApplovinMaxConnection         `yaml:"applovinmax,omitempty" json:"applovinmax,omitempty" mapstructure:"applovinmax"`

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

	connections := reflect.ValueOf(c).Elem()
	connectionsType := connections.Type()
	for i := range connectionsType.NumField() {
		field := connections.Field(i)
		if field.Type().Kind() != reflect.Slice {
			continue
		}

		yamlTag := connectionsType.Field(i).Tag.Get("yaml")
		if len(yamlTag) == 0 {
			// this is meant to be caught by our tests
			// this shouldn't happen during runtime.
			msg := fmt.Sprintf(
				"Expected field %s.%s to have a non-empty `yaml` tag",
				connectionsType.Name(),
				connectionsType.Field(i).Name,
			)
			panic(msg)
		}
		sep := strings.Index(yamlTag, ",")
		if sep > 0 {
			yamlTag = yamlTag[:sep]
		}

		for i := range field.Len() {
			fieldItem := field.Index(i)
			connection := fieldItem.Interface().(Named)
			c.byKey[connection.GetName()] = fieldItem.Addr().Interface()
			c.typeNameMap[connection.GetName()] = yamlTag
		}
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

func (c *Config) CanRunTaskInstances(p *pipeline.Pipeline, tasks []scheduler.TaskInstance) error {
	for _, task := range tasks {
		asset := task.GetAsset()
		connNames, err := p.GetAllConnectionNamesForAsset(asset)
		if err != nil {
			return errors2.Wrap(err, "Could not find connection name for asset "+asset.Name)
		}

		for _, connName := range connNames {
			if !c.SelectedEnvironment.Connections.Exists(connName) {
				return errors2.Errorf("Connection '%s' does not exist in the selected environment and is needed for '%s'", connName, asset.Name)
			}
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

	absoluteConfigPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}
	configLocation := filepath.Dir(absoluteConfigPath)

	// Make duckdb paths absolute
	for _, env := range config.Environments {
		for i, conn := range env.Connections.DuckDB {
			if filepath.IsAbs(conn.Path) {
				continue
			}
			env.Connections.DuckDB[i].Path = filepath.Join(configLocation, conn.Path)
		}
		// Make GoogleCloudPlatform service account file paths absolute
		for i, conn := range env.Connections.GoogleCloudPlatform {
			if conn.ServiceAccountFile == "" {
				continue
			}

			if filepath.IsAbs(conn.ServiceAccountFile) {
				continue
			}
			env.Connections.GoogleCloudPlatform[i].ServiceAccountFile = filepath.Join(configLocation, conn.ServiceAccountFile)
		}
		// Make MySQL SSL file paths absolute
		for i, conn := range env.Connections.MySQL {
			if conn.SslCaPath != "" && !filepath.IsAbs(conn.SslCaPath) {
				env.Connections.MySQL[i].SslCaPath = filepath.Join(configLocation, conn.SslCaPath)
			}

			if conn.SslCertPath != "" && !filepath.IsAbs(conn.SslCertPath) {
				env.Connections.MySQL[i].SslCertPath = filepath.Join(configLocation, conn.SslCertPath)
			}

			if conn.SslKeyPath != "" && !filepath.IsAbs(conn.SslKeyPath) {
				env.Connections.MySQL[i].SslKeyPath = filepath.Join(configLocation, conn.SslKeyPath)
			}
		}

		// Make Snowflake private key path absolute
		for i, conn := range env.Connections.Snowflake {
			if conn.PrivateKeyPath == "" {
				continue
			}

			if filepath.IsAbs(conn.PrivateKeyPath) {
				continue
			}

			env.Connections.Snowflake[i].PrivateKeyPath = filepath.Join(configLocation, conn.PrivateKeyPath)
		}
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

	// todo(turtledev): refactor this. It's full of unnecessary repetition
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
	case "duckdb":
		var conn DuckDBConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.DuckDB = append(env.Connections.DuckDB, conn)
	case "asana":
		var conn AsanaConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.Asana = append(env.Connections.Asana, conn)
	case "dynamodb":
		var conn DynamoDBConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.DynamoDB = append(env.Connections.DynamoDB, conn)

	case "googleads":
		var conn GoogleAdsConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.GoogleAds = append(env.Connections.GoogleAds, conn)
	case "tiktokads":
		var conn TikTokAdsConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.TikTokAds = append(env.Connections.TikTokAds, conn)
	case "github":
		var conn GitHubConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.GitHub = append(env.Connections.GitHub, conn)
	case "appstore":
		var conn AppStoreConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.AppStore = append(env.Connections.AppStore, conn)
	case "linkedinads":
		var conn LinkedInAdsConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.LinkedInAds = append(env.Connections.LinkedInAds, conn)
	case "gcs":
		var conn GCSConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.GCS = append(env.Connections.GCS, conn)
	case "clickhouse":
		var conn ClickHouseConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.ClickHouse = append(env.Connections.ClickHouse, conn)
	case "applovinmax":
		var conn ApplovinMaxConnection
		if err := mapstructure.Decode(creds, &conn); err != nil {
			return fmt.Errorf("failed to decode credentials: %w", err)
		}
		conn.Name = name
		env.Connections.ApplovinMax = append(env.Connections.ApplovinMax, conn)
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
	case "duckdb":
		env.Connections.DuckDB = removeConnection(env.Connections.DuckDB, connectionName)
	case "asana":
		env.Connections.Asana = removeConnection(env.Connections.Asana, connectionName)
	case "dynamodb":
		env.Connections.DynamoDB = removeConnection(env.Connections.DynamoDB, connectionName)
	case "googleads":
		env.Connections.GoogleAds = removeConnection(env.Connections.GoogleAds, connectionName)
	case "tiktokads":
		env.Connections.TikTokAds = removeConnection(env.Connections.TikTokAds, connectionName)
	case "github":
		env.Connections.GitHub = removeConnection(env.Connections.GitHub, connectionName)
	case "appstore":
		env.Connections.AppStore = removeConnection(env.Connections.AppStore, connectionName)
	case "linkedinads":
		env.Connections.LinkedInAds = removeConnection(env.Connections.LinkedInAds, connectionName)
	case "gcs":
		env.Connections.GCS = removeConnection(env.Connections.GCS, connectionName)
	case "clickhouse":
		env.Connections.ClickHouse = removeConnection(env.Connections.ClickHouse, connectionName)
	case "applovinmax":
		env.Connections.ApplovinMax = removeConnection(env.Connections.ApplovinMax, connectionName)
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
