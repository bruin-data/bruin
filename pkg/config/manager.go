package config

import (
	"encoding/json"
	errors "errors"
	"fmt"
	fs2 "io/fs"
	"os"
	"path"

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

type GoogleCloudPlatformConnection struct {
	Name               string `yaml:"name"`
	ServiceAccountJSON string `yaml:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file"`
	ProjectID          string `yaml:"project_id"`
	rawCredentials     *google.Credentials
	Location           string `yaml:"location"`
}

type MongoConnection struct {
	Name     string `yaml:"name" json:"name"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Host     string `yaml:"host"     json:"host"`
	Port     int    `yaml:"port"     json:"port"`
	Database string `yaml:"database" json:"database"`
}

type MySQLConnection struct {
	Name     string `yaml:"name" json:"name"`
	Username string `yaml:"username" json:"username"`
	Password string `yaml:"password" json:"password"`
	Host     string `yaml:"host"     json:"host"`
	Port     int    `yaml:"port"     json:"port"`
	Database string `yaml:"database" json:"database"`
}

type NotionConnection struct {
	Name   string `yaml:"name" json:"name"`
	APIKey string `yaml:"api_key" json:"api_key"`
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
	GoogleCloudPlatform []GoogleCloudPlatformConnection `yaml:"google_cloud_platform"`
	Snowflake           []SnowflakeConnection           `yaml:"snowflake"`
	Postgres            []PostgresConnection            `yaml:"postgres"`
	RedShift            []PostgresConnection            `yaml:"redshift"`
	MsSQL               []MsSQLConnection               `yaml:"mssql"`
	Synapse             []MsSQLConnection               `yaml:"synapse"`
	Mongo               []MongoConnection               `yaml:"mongo"`
	MySQL               []MySQLConnection               `yaml:"mysql"`
	Notion              []NotionConnection              `yaml:"notion"`
	Generic             []GenericConnection             `yaml:"generic"`

	byKey map[string]any
}

func (c *Connections) buildConnectionKeyMap() {
	c.byKey = make(map[string]any)
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
	return git.EnsureGivenPatternIsInGitignore(fs, path.Dir(filePath), path.Base(filePath))
}
