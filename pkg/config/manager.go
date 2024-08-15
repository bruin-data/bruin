package config

import (
	"encoding/json"
	errors "errors"
	"fmt"
	fs2 "io/fs"
	"net/url"
	"os"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/git"
	path2 "github.com/bruin-data/bruin/pkg/path"
	"github.com/spf13/afero"
	"golang.org/x/oauth2/google"
)

type PostgresConnection struct {
	Name         string `yaml:"name" json:"name"`
	Username     string `yaml:"username" json:"username"`
	Password     string `yaml:"password" json:"password"`
	Host         string `yaml:"host" json:"host"`
	Port         int    `yaml:"port" json:"port"`
	Database     string `yaml:"database" json:"database"`
	Schema       string `yaml:"schema" json:"schema"`
	PoolMaxConns int    `yaml:"pool_max_conns" json:"pool_max_conns" default:"10"`
	SslMode      string `yaml:"ssl_mode" json:"ssl_mode" default:"disable"`
}

type MsSQLConnection struct {
	Name     string `yaml:"name" json:"name"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Host     string `yaml:"host"     json:"host"`
	Port     int    `yaml:"port"     json:"port"`
	Database string `yaml:"database" json:"database"`
}

type DatabricksConnection struct {
	Name  string `yaml:"name"  json:"name"`
	Token string `yaml:"token" json:"token"`
	Path  string `yaml:"path"  json:"path"`
	Host  string `yaml:"host"  json:"host"`
	Port  int    `yaml:"port"     json:"port"`
}

type GoogleCloudPlatformConnection struct {
	Name               string `yaml:"name"`
	ServiceAccountJSON string `yaml:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file"`
	ProjectID          string `yaml:"project_id"`
	rawCredentials     *google.Credentials
	Location           string `yaml:"location"`
}

type MongoConnection struct {
	Name     string `yaml:"name"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Host     string `yaml:"host"     json:"host"`
	Port     int    `yaml:"port"     json:"port"`
	Database string `yaml:"database" json:"database"`
}

type ShopifyConnection struct {
	Name   string `yaml:"name"`
	URL    string `yaml:"url"`
	APIKey string `yaml:"api_key"`
}

type AwsConnection struct {
	Name             string `yaml:"name"`
	Region           string `yaml:"region"`
	AccessKey        string `yaml:"access_key"`
	SecretKey        string `yaml:"secret_key"`
	QueryResultsPath string `yaml:"query_results_path"`
}

func (a *AwsConnection) EnsureConnectionIsValid() error {
	if a.QueryResultsPath == "" {
		return errors.New("query_results_path is required to connect AWS Athena")
	}

	if a.AccessKey == "" {
		return errors.New("access_key is required to connect AWS Athena")
	}

	if a.SecretKey == "" {
		return errors.New("secret_key is required to connect AWS Athena")
	}

	return nil
}

func (a *AwsConnection) ToAthenaDsn() (string, error) {
	err := a.EnsureConnectionIsValid()
	if err != nil {
		return "", err
	}

	// parse query results path into a url instance
	u, err := url.Parse(a.QueryResultsPath)
	if err != nil {
		return "", err
	}

	if u.Scheme != "s3" {
		return "", errors.New("query_results_path must be an S3 path, starting with s3://")
	}

	values := u.Query()
	if a.Region != "" {
		values.Add("region", a.Region)
	}

	values.Add("accessID", a.AccessKey)
	values.Add("secretAccessKey", a.SecretKey)

	u.RawQuery = values.Encode()

	return u.String(), nil
}

func (a *AwsConnection) ToIngestrURI() (string, error) {
	err := a.EnsureConnectionIsValid()
	if err != nil {
		return "", err
	}

	// parse query results path into a url instance
	u, err := url.Parse(a.QueryResultsPath)
	if err != nil {
		return "", err
	}

	if u.Scheme != "s3" {
		return "", errors.New("query_results_path must be an S3 path, starting with s3://")
	}

	u.Scheme = "athena"

	values := u.Query()
	if a.Region != "" {
		values.Add("region", a.Region)
	}

	values.Add("accessID", a.AccessKey)
	values.Add("secretAccessKey", a.SecretKey)

	u.RawQuery = values.Encode()

	return u.String(), nil
}

type GorgiasConnection struct {
	Name   string `yaml:"name"`
	Domain string `yaml:"domain"`
	APIKey string `yaml:"api_key"`
	Email  string `yaml:"email"`
}

type MySQLConnection struct {
	Name     string `yaml:"name" json:"name"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Host     string `yaml:"host"     json:"host"`
	Port     int    `yaml:"port"     json:"port"`
	Database string `yaml:"database" json:"database"`
	Driver   string `yaml:"driver" json:"driver"`
}

type NotionConnection struct {
	Name   string `yaml:"name" json:"name"`
	APIKey string `yaml:"api_key" json:"api_key"`
}

type HANAConnection struct {
	Name     string `yaml:"name" json:"name"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Host     string `yaml:"host"     json:"host"`
	Port     int    `yaml:"port"     json:"port"`
	Database string `yaml:"database" json:"database"`
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
		"service_account_json": c.ServiceAccountJSON,
		"project_id":           c.ProjectID,
	})
}

type SnowflakeConnection struct {
	Name      string `yaml:"name"`
	Account   string `yaml:"account"`
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Region    string `yaml:"region"`
	Role      string `yaml:"role"`
	Database  string `yaml:"database"`
	Schema    string `yaml:"schema"`
	Warehouse string `yaml:"warehouse"`
}

func (c SnowflakeConnection) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{
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

type GenericConnection struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

func (c GenericConnection) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{
		"name":  c.Name,
		"value": c.Value,
	})
}

type Connections struct {
	AwsConnection       []AwsConnection                 `yaml:"aws"`
	GoogleCloudPlatform []GoogleCloudPlatformConnection `yaml:"google_cloud_platform"`
	Snowflake           []SnowflakeConnection           `yaml:"snowflake"`
	Postgres            []PostgresConnection            `yaml:"postgres"`
	RedShift            []PostgresConnection            `yaml:"redshift"`
	MsSQL               []MsSQLConnection               `yaml:"mssql"`
	Databricks          []DatabricksConnection          `yaml:"databricks"`
	Synapse             []MsSQLConnection               `yaml:"synapse"`
	Mongo               []MongoConnection               `yaml:"mongo"`
	MySQL               []MySQLConnection               `yaml:"mysql"`
	Notion              []NotionConnection              `yaml:"notion"`
	HANA                []HANAConnection                `yaml:"hana"`
	Shopify             []ShopifyConnection             `yaml:"shopify"`
	Gorgias             []GorgiasConnection             `yaml:"gorgias"`
	Generic             []GenericConnection             `yaml:"generic"`

	byKey map[string]any
}

func (c *Connections) buildConnectionKeyMap() {
	c.byKey = make(map[string]any)

	for i, conn := range c.AwsConnection {
		c.byKey[conn.Name] = &(c.AwsConnection[i])
	}

	for i, conn := range c.GoogleCloudPlatform {
		c.byKey[conn.Name] = &(c.GoogleCloudPlatform[i])
	}

	for i, conn := range c.Snowflake {
		c.byKey[conn.Name] = &(c.Snowflake[i])
	}

	for i, conn := range c.Postgres {
		c.byKey[conn.Name] = &(c.Postgres[i])
	}

	for i, conn := range c.RedShift {
		c.byKey[conn.Name] = &(c.RedShift[i])
	}

	for i, conn := range c.MsSQL {
		c.byKey[conn.Name] = &(c.MsSQL[i])
	}

	for i, conn := range c.Databricks {
		c.byKey[conn.Name] = &(c.Databricks[i])
	}

	for i, conn := range c.Synapse {
		c.byKey[conn.Name] = &(c.Synapse[i])
	}

	for i, conn := range c.Mongo {
		c.byKey[conn.Name] = &(c.Mongo[i])
	}

	for i, conn := range c.MySQL {
		c.byKey[conn.Name] = &(c.MySQL[i])
	}

	for i, conn := range c.Notion {
		c.byKey[conn.Name] = &(c.Notion[i])
	}

	for i, conn := range c.HANA {
		c.byKey[conn.Name] = &(c.HANA[i])
	}

	for i, conn := range c.Shopify {
		c.byKey[conn.Name] = &(c.Shopify[i])
	}

	for i, conn := range c.Gorgias {
		c.byKey[conn.Name] = &(c.Gorgias[i])
	}

	for i, conn := range c.Generic {
		c.byKey[conn.Name] = &(c.Generic[i])
	}
}

type Environment struct {
	Connections Connections `yaml:"connections"`
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

	DefaultEnvironmentName  string                 `yaml:"default_environment"`
	SelectedEnvironmentName string                 `yaml:"-"`
	SelectedEnvironment     *Environment           `yaml:"-"`
	Environments            map[string]Environment `yaml:"environments"`
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

	e := config.Environments[config.DefaultEnvironmentName]

	config.SelectedEnvironment = &e
	config.SelectedEnvironmentName = config.DefaultEnvironmentName
	config.SelectedEnvironment.Connections.buildConnectionKeyMap()

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
		Connections: Connections{},
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
