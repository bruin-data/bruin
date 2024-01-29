package pipeline

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

var ValidQualityChecks = map[string]bool{
	"not_null":        true,
	"unique":          true,
	"positive":        true,
	"min":             true,
	"max":             true,
	"accepted_values": true,
}

func mustBeStringArray(fieldName string, value *yaml.Node) ([]string, error) {
	var multi []string
	err := value.Decode(&multi)
	if err != nil {
		return nil, errors.New("`" + fieldName + "` field must be an array of strings")
	}
	return multi, nil
}

type depends []string

func (a *depends) UnmarshalYAML(value *yaml.Node) error {
	multi, err := mustBeStringArray("depends", value)
	*a = multi
	return err
}

type clusterBy []string

func (a *clusterBy) UnmarshalYAML(value *yaml.Node) error {
	multi, err := mustBeStringArray("cluster_by", value)
	*a = multi
	return err
}

type materialization struct {
	Type           string    `yaml:"type"`
	Strategy       string    `yaml:"strategy"`
	PartitionBy    string    `yaml:"partition_by"`
	ClusterBy      clusterBy `yaml:"cluster_by"`
	IncrementalKey string    `yaml:"incremental_key"`
}

type columnCheckValue struct {
	IntArray    *[]int
	Int         *int
	Float       *float64
	StringArray *[]string
	String      *string
	Bool        *bool
}

func (a *columnCheckValue) UnmarshalYAML(value *yaml.Node) error {
	var val interface{}
	err := value.Decode(&val)
	if err != nil {
		return err
	}

	switch v := val.(type) {
	case []interface{}:
		var multiInt []int
		err := value.Decode(&multiInt)
		if err == nil {
			*a = columnCheckValue{IntArray: &multiInt}
			return nil
		}

		var multi []string
		err = value.Decode(&multi)
		if err != nil {
			return err
		}

		*a = columnCheckValue{StringArray: &multi}
	case string:
		*a = columnCheckValue{String: &v}
	case int:
		*a = columnCheckValue{Int: &v}
	case float64:
		*a = columnCheckValue{Float: &v}
	case bool:
		*a = columnCheckValue{Bool: &v}
	default:
		return fmt.Errorf("unexpected type %T", v)
	}

	return nil
}

type columnCheck struct {
	Name  string           `yaml:"name"`
	Value columnCheckValue `yaml:"value"`
}

type column struct {
	Name        string        `yaml:"name"`
	Type        string        `yaml:"type"`
	Description string        `yaml:"description"`
	Tests       []columnCheck `yaml:"checks"`
}

type secretMapping struct {
	SecretKey   string `yaml:"key"`
	InjectedKey string `yaml:"inject_as"`
}

type customCheck struct {
	Name  string `yaml:"name"`
	Query string `yaml:"query"`
	Value int64  `yaml:"value"`
}

type taskDefinition struct {
	Name            string            `yaml:"name"`
	Description     string            `yaml:"description"`
	Type            string            `yaml:"type"`
	RunFile         string            `yaml:"run"`
	Depends         depends           `yaml:"depends"`
	Parameters      map[string]string `yaml:"parameters"`
	Connections     map[string]string `yaml:"connections"`
	Secrets         []secretMapping   `yaml:"secrets"`
	Connection      string            `yaml:"connection"`
	Image           string            `yaml:"image"`
	Materialization materialization   `yaml:"materialization"`
	Columns         []column          `yaml:"columns"`
	CustomChecks    []customCheck     `yaml:"custom_checks"`
}

func CreateTaskFromYamlDefinition(fs afero.Fs) TaskCreator {
	return func(filePath string) (*Asset, error) {
		filePath, err := filepath.Abs(filePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get absolute path for the definition file")
		}

		var definition taskDefinition
		err = path.ReadYaml(fs, filePath, &definition)
		if err != nil {
			return nil, err
		}

		buf, err := afero.ReadFile(fs, filePath)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read file %s", filePath)
		}

		task, err := ConvertYamlToTask(buf)
		if err != nil {
			return nil, err
		}

		executableFile := ExecutableFile{}
		if definition.RunFile != "" {
			relativeRunFilePath := filepath.Join(filepath.Dir(filePath), definition.RunFile)
			absRunFile, err := filepath.Abs(relativeRunFilePath)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to resolve the absolute run file path: %s", definition.RunFile)
			}

			executableFile.Name = filepath.Base(definition.RunFile)
			executableFile.Path = absRunFile

			content, err := afero.ReadFile(fs, absRunFile)
			if err != nil {
				return nil, errors.Wrapf(err, "unable to read the run file: %s", absRunFile)
			}
			executableFile.Content = string(content)
		}
		task.ExecutableFile = executableFile

		return task, nil
	}
}

func ConvertYamlToTask(content []byte) (*Asset, error) {
	var definition taskDefinition
	err := path.ConvertYamlToObject(content, &definition)
	if err != nil {
		return nil, err
	}

	mat := Materialization{
		Type:           MaterializationType(strings.ToLower(definition.Materialization.Type)),
		Strategy:       MaterializationStrategy(strings.ToLower(definition.Materialization.Strategy)),
		ClusterBy:      definition.Materialization.ClusterBy,
		PartitionBy:    definition.Materialization.PartitionBy,
		IncrementalKey: definition.Materialization.IncrementalKey,
	}

	columns := make([]Column, len(definition.Columns))
	for index, column := range definition.Columns {
		tests := make([]ColumnCheck, 0, len(column.Tests))

		seenTests := make(map[string]bool)

		for _, test := range column.Tests {
			if !ValidQualityChecks[test.Name] {
				continue
			}

			if seenTests[test.Name] {
				continue
			}

			seenTests[test.Name] = true

			tests = append(tests, NewColumnCheck(definition.Name, column.Name, test.Name, ColumnCheckValue(test.Value)))
		}

		columns[index] = Column{
			Name:        column.Name,
			Type:        strings.ToLower(strings.TrimSpace(column.Type)),
			Description: column.Description,
			Checks:      tests,
		}
	}

	dependsOn := make([]string, len(definition.Depends))
	copy(dependsOn, definition.Depends)

	task := Asset{
		ID:              hash(definition.Name),
		Name:            definition.Name,
		Description:     definition.Description,
		Type:            AssetType(definition.Type),
		Parameters:      definition.Parameters,
		Connection:      definition.Connection,
		Secrets:         make([]SecretMapping, len(definition.Secrets)),
		DependsOn:       dependsOn,
		ExecutableFile:  ExecutableFile{},
		Materialization: mat,
		Image:           definition.Image,
		Columns:         columns,
		CustomChecks:    make([]CustomCheck, len(definition.CustomChecks)),
	}

	for index, check := range definition.CustomChecks {
		// set the ID as the hash of the name
		task.CustomChecks[index] = CustomCheck{
			ID:    hash(fmt.Sprintf("%s-%s", task.Name, check.Name)),
			Name:  check.Name,
			Query: check.Query,
			Value: check.Value,
		}
	}

	for index, m := range definition.Secrets {
		task.Secrets[index] = SecretMapping(m)
	}

	return &task, nil
}

func hash(s string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(s)))[:64]
}
