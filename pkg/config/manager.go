package config

import (
	"bufio"
	"encoding/json"
	errors "errors"
	"fmt"
	fs2 "io/fs"
	"os"
	"path"
	"strings"

	path2 "github.com/bruin-data/bruin/pkg/path"
	"github.com/spf13/afero"
	"golang.org/x/oauth2/google"
)

type GoogleCloudPlatformConnection struct {
	Name               string `yaml:"name"`
	ServiceAccountJSON string `yaml:"service_account_json"`
	ServiceAccountFile string `yaml:"service_account_file"`
	ProjectID          string `yaml:"project_id"`
	rawCredentials     *google.Credentials
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

func ensureConfigIsInGitignore(fs afero.Fs, filePath string) (err error) {
	// Check if .gitignore file exists in the root of the repository
	gitignorePath := path.Join(path.Dir(filePath), ".gitignore")
	exists, err := afero.Exists(fs, gitignorePath)
	if err != nil {
		return err
	}

	fileNameToIgnore := path.Base(filePath)
	if !exists {
		// Create a new .gitignore file if it doesn't exist
		if err = afero.WriteFile(fs, gitignorePath, []byte(fileNameToIgnore), 0o644); err != nil {
			return err
		}
		return nil
	}

	file, err := fs.OpenFile(gitignorePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return err
	}
	defer func(open afero.File) {
		tempErr := open.Close()
		if tempErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close file: %w", tempErr))
		}
	}(file)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.TrimSpace(scanner.Text()) == fileNameToIgnore {
			return nil
		}
	}

	_, err = file.Write([]byte("\n" + fileNameToIgnore))
	return err
}
