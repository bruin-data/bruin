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
	"negative":        true,
	"non_negative":    true,
	"pattern":         true,
}

func mustBeStringArray(fieldName string, value *yaml.Node) ([]string, error) {
	var multi []string
	err := value.Decode(&multi)
	if err != nil {
		return nil, &ParseError{Msg: "`" + fieldName + "` field must be an array of strings"}
	}
	return multi, nil
}

type columns []upstreamColumn

type upstreamColumn struct {
	Name  string
	Usage string
}

type depends []upstream

type UpstreamMode int

const (
	UpstreamModeFull UpstreamMode = iota
	UpstreamModeSymbolic
)

func (m UpstreamMode) IsValid() bool {
	switch m {
	case UpstreamModeFull, UpstreamModeSymbolic:
		return true
	default:
		return false
	}
}

func (m UpstreamMode) String() string {
	switch m {
	case UpstreamModeFull:
		return "full"
	case UpstreamModeSymbolic:
		return "symbolic"
	default:
		return "full"
	}
}

func MarshalUpstreamMode(s string) UpstreamMode {
	switch strings.ToLower(s) {
	case "full":
		return UpstreamModeFull
	case "symbolic":
		return UpstreamModeSymbolic
	default:
		return UpstreamModeFull // default to full mode for safety
	}
}

type upstream struct {
	Value   string       `yaml:"value"`
	Type    string       `yaml:"type"`
	Mode    UpstreamMode `yaml:"mode"`
	Columns columns      `yaml:"columns"`
}

func (a *depends) UnmarshalYAML(value *yaml.Node) error {
	var multi []upstream
	err := value.Decode(&multi)
	if err != nil {
		return &ParseError{Msg: "Malformed `depends` items"}
	}
	*a = multi

	return nil
}

func (u *upstream) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind == yaml.ScalarNode {
		*u = upstream{Value: value.Value, Type: "asset", Columns: columns(nil), Mode: UpstreamModeFull}
		return nil
	}

	var us map[string]any
	err := value.Decode(&us)
	if err != nil {
		return &ParseError{Msg: "Malformed `depends` field"}
	}

	uri, foundURI := us["uri"]
	asset, foundAsset := us["asset"]
	cols, foundColumns := us["columns"]
	mode, foundMode := us["mode"]
	colsStruct := columns(nil)
	upstreamMode := UpstreamModeFull
	if foundMode {
		upstreamMode = MarshalUpstreamMode(mode.(string))
	}

	if upstreamMode != UpstreamModeFull && upstreamMode != UpstreamModeSymbolic {
		upstreamMode = UpstreamModeFull
	}

	if foundColumns {
		colsSlice, ok := cols.([]any)
		if !ok {
			return &ParseError{Msg: "`columns` is malformed"}
		}

		for _, col := range colsSlice {
			colString, ok := col.(string)
			if ok {
				colsStruct = append(colsStruct, upstreamColumn{Name: colString, Usage: ""})
				continue
			}
			colMap, ok := col.(map[string]any)
			if !ok {
				return &ParseError{Msg: "'columns' is malformed"}
			}
			name, ok := colMap["name"]
			if !ok {
				return &ParseError{Msg: "Missing 'name' in column"}
			}
			nameString, ok := name.(string)
			if !ok {
				return &ParseError{Msg: "Malformed 'name' in column"}
			}
			usage, ok := colMap["usage"]
			if !ok {
				usage = ""
			}

			colsStruct = append(colsStruct, upstreamColumn{Name: nameString, Usage: usage.(string)})
		}
	}

	if foundURI && !foundAsset {
		uriString, ok := uri.(string)
		if !ok {
			return &ParseError{Msg: "`uri` field must be a string"}
		}
		*u = upstream{Value: uriString, Type: "uri", Columns: colsStruct, Mode: upstreamMode}

		return nil
	}

	if foundAsset && !foundURI {
		assetString, ok := asset.(string)
		if !ok {
			return &ParseError{Msg: "`uri` field must be a string"}
		}
		*u = upstream{Value: assetString, Type: "asset", Columns: colsStruct, Mode: upstreamMode}

		return nil
	}

	return &ParseError{Msg: "Malformed \"depends\" field"}
}

type clusterBy []string

func (a *clusterBy) UnmarshalYAML(value *yaml.Node) error {
	multi, err := mustBeStringArray("cluster_by", value)
	*a = multi
	return err
}

type materialization struct {
	Type            string    `yaml:"type"`
	Strategy        string    `yaml:"strategy"`
	PartitionBy     string    `yaml:"partition_by"`
	ClusterBy       clusterBy `yaml:"cluster_by"`
	IncrementalKey  string    `yaml:"incremental_key"`
	TimeGranularity string    `yaml:"time_granularity,omitempty"`
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
			return &ParseError{Msg: err.Error()}
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
		return &ParseError{Msg: fmt.Sprintf("unexpected type %T", v)}
	}

	return nil
}

type columnCheck struct {
	Name     string           `yaml:"name"`
	Value    columnCheckValue `yaml:"value"`
	Blocking *bool            `yaml:"blocking"`
}

type columnUpstream struct {
	Column string `yaml:"column"`
	Table  string `yaml:"table"`
}

type column struct {
	Extends       string           `yaml:"extends"`
	Name          string           `yaml:"name"`
	Type          string           `yaml:"type"`
	Description   string           `yaml:"description"`
	Tests         []columnCheck    `yaml:"checks"`
	PrimaryKey    bool             `yaml:"primary_key"`
	UpdateOnMerge bool             `yaml:"update_on_merge"`
	Upstreams     []columnUpstream `yaml:"upstreams"`
}

type secretMapping struct {
	SecretKey   string `yaml:"key"`
	InjectedKey string `yaml:"inject_as"`
}

type customCheck struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	Query       string `yaml:"query"`
	Value       int64  `yaml:"value"`
	Blocking    *bool  `yaml:"blocking"`
}

type snowflake struct {
	Warehouse string `yaml:"warehouse"`
}

type athena struct {
	QueryResultsPath string `yaml:"query_results_path"`
}

type taskDefinition struct {
	Name            string            `yaml:"name"`
	URI             string            `yaml:"uri"`
	Description     string            `yaml:"description"`
	Type            string            `yaml:"type"`
	RunFile         string            `yaml:"run"`
	Depends         depends           `yaml:"depends"`
	Parameters      map[string]string `yaml:"parameters"`
	Connections     map[string]string `yaml:"connections"`
	Secrets         []secretMapping   `yaml:"secrets"`
	Connection      string            `yaml:"connection"`
	Image           string            `yaml:"image"`
	Instance        string            `yaml:"instance"`
	Materialization materialization   `yaml:"materialization"`
	Owner           string            `yaml:"owner"`
	Extends         []string          `yaml:"extends"`
	Columns         []column          `yaml:"columns"`
	CustomChecks    []customCheck     `yaml:"custom_checks"`
	Tags            []string          `yaml:"tags"`
	Snowflake       snowflake         `yaml:"snowflake"`
	Athena          athena            `yaml:"athena"`
}

func CreateTaskFromYamlDefinition(fs afero.Fs) TaskCreator {
	return func(filePath string) (*Asset, error) {
		filePath, err := filepath.Abs(filePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to get absolute path for the definition file")
		}

		yamlError := new(path.YamlParseError)
		var definition taskDefinition
		err = path.ReadYaml(fs, filePath, &definition)
		if err != nil && errors.As(err, &yamlError) {
			return nil, &ParseError{Msg: err.Error()}
		}
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

		executableFile := ExecutableFile{
			Name:    filepath.Base(filePath),
			Path:    filePath,
			Content: string(buf),
		}
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
		Type:            MaterializationType(strings.ToLower(definition.Materialization.Type)),
		Strategy:        MaterializationStrategy(strings.ToLower(definition.Materialization.Strategy)),
		ClusterBy:       definition.Materialization.ClusterBy,
		PartitionBy:     definition.Materialization.PartitionBy,
		IncrementalKey:  definition.Materialization.IncrementalKey,
		TimeGranularity: MaterializationTimeGranularity(strings.ToLower(definition.Materialization.TimeGranularity)),
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

			tests = append(tests, NewColumnCheck(definition.Name, column.Name, test.Name, ColumnCheckValue(test.Value), test.Blocking))
		}

		var entityDefinition *EntityAttribute
		if column.Extends != "" {
			fromBits := strings.Split(column.Extends, ".")
			if len(fromBits) != 2 {
				return nil, &ParseError{Msg: "'from' field must be in the format `entity.attribute`"}
			}

			entityDefinition = &EntityAttribute{
				Entity:    strings.TrimSpace(fromBits[0]),
				Attribute: strings.TrimSpace(fromBits[1]),
			}
		}

		upstreamColumns := make([]*UpstreamColumn, len(column.Upstreams))
		for index, upstream := range column.Upstreams {
			upstreamColumns[index] = &UpstreamColumn{
				Column: upstream.Column,
				Table:  upstream.Table,
			}
		}

		columns[index] = Column{
			Name:            column.Name,
			Type:            strings.TrimSpace(column.Type),
			Description:     column.Description,
			Checks:          tests,
			PrimaryKey:      column.PrimaryKey,
			UpdateOnMerge:   column.UpdateOnMerge,
			EntityAttribute: entityDefinition,
			Extends:         column.Extends,
			Upstreams:       upstreamColumns,
		}
	}

	upstreams := make([]Upstream, len(definition.Depends))
	for index, dep := range definition.Depends {
		cols := make([]DependsColumn, 0)
		for _, col := range dep.Columns {
			cols = append(cols, DependsColumn(col))
		}

		if !dep.Mode.IsValid() {
			dep.Mode = UpstreamModeFull
		}

		upstreams[index] = Upstream{
			Value:   dep.Value,
			Type:    dep.Type,
			Mode:    dep.Mode,
			Columns: cols,
		}
	}

	task := Asset{
		ID:              hash(definition.Name),
		URI:             definition.URI,
		Name:            definition.Name,
		Description:     definition.Description,
		Type:            AssetType(definition.Type),
		Parameters:      definition.Parameters,
		Connection:      definition.Connection,
		Secrets:         make([]SecretMapping, len(definition.Secrets)),
		Upstreams:       upstreams,
		ExecutableFile:  ExecutableFile{},
		Materialization: mat,
		Image:           definition.Image,
		Instance:        definition.Instance,
		Owner:           definition.Owner,
		Tags:            definition.Tags,
		Extends:         definition.Extends,
		Columns:         columns,
		CustomChecks:    make([]CustomCheck, len(definition.CustomChecks)),
		Snowflake:       SnowflakeConfig{Warehouse: definition.Snowflake.Warehouse},
		Athena:          AthenaConfig{Location: definition.Athena.QueryResultsPath},
	}

	for index, check := range definition.CustomChecks {
		// set the ID as the hash of the name
		task.CustomChecks[index] = CustomCheck{
			ID:          hash(fmt.Sprintf("%s-%s", task.Name, check.Name)),
			Name:        check.Name,
			Description: check.Description,
			Query:       check.Query,
			Value:       check.Value,
			Blocking:    DefaultTrueBool{Value: check.Blocking},
		}
	}

	for index, m := range definition.Secrets {
		mapping := SecretMapping(m)
		if mapping.InjectedKey == "" {
			mapping.InjectedKey = mapping.SecretKey
		}

		task.Secrets[index] = mapping
	}

	return &task, nil
}

func hash(s string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(s)))[:64]
}
