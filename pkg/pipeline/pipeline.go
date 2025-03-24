package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type RunConfig string

const (
	CommentTask TaskDefinitionType = "comment"
	YamlTask    TaskDefinitionType = "yaml"

	AssetTypePython               = AssetType("python")
	AssetTypeSnowflakeQuery       = AssetType("sf.sql")
	AssetTypeSnowflakeSeed        = AssetType("sf.seed")
	AssetTypeSnowflakeQuerySensor = AssetType("sf.sensor.query")
	AssetTypeBigqueryQuery        = AssetType("bq.sql")
	AssetTypeBigqueryTableSensor  = AssetType("bq.sensor.table")
	AssetTypeBigqueryQuerySensor  = AssetType("bq.sensor.query")
	AssetTypeBigquerySource       = AssetType("bq.source")
	AssetTypeBigquerySeed         = AssetType("bq.seed")
	AssetTypeDuckDBQuery          = AssetType("duckdb.sql")
	AssetTypeDuckDBSeed           = AssetType("duckdb.seed")
	AssetTypeEmpty                = AssetType("empty")
	AssetTypePostgresQuery        = AssetType("pg.sql")
	AssetTypePostgresSeed         = AssetType("pg.seed")
	AssetTypeRedshiftQuery        = AssetType("rs.sql")
	AssetTypeRedshiftSeed         = AssetType("rs.seed")
	AssetTypeAthenaQuery          = AssetType("athena.sql")
	AssetTypeAthenaSQLSensor      = AssetType("athena.sensor.query")
	AssetTypeAthenaSeed           = AssetType("athena.seed")
	AssetTypeMsSQLQuery           = AssetType("ms.sql")
	AssetTypeMsSQLSeed            = AssetType("ms.seed")
	AssetTypeDatabricksQuery      = AssetType("databricks.sql")
	AssetTypeDatabricksSeed       = AssetType("databricks.seed")
	AssetTypeSynapseQuery         = AssetType("synapse.sql")
	AssetTypeSynapseSeed          = AssetType("synapse.seed")
	AssetTypeIngestr              = AssetType("ingestr")
	AssetTypeTableau              = AssetType("tableau")
	AssetTypeClickHouse           = AssetType("clickhouse.sql")
	AssetTypeClickHouseSeed       = AssetType("clickhouse.seed")
	RunConfigFullRefresh          = RunConfig("full-refresh")
	RunConfigStartDate            = RunConfig("start-date")
	RunConfigEndDate              = RunConfig("end-date")
)

var defaultMapping = map[string]string{
	"aws":                   "aws-default",
	"athena":                "athena-default",
	"gcp":                   "gcp-default",
	"google_cloud_platform": "gcp-default",
	"snowflake":             "snowflake-default",
	"postgres":              "postgres-default",
	"redshift":              "redshift-default",
	"mssql":                 "mssql-default",
	"databricks":            "databricks-default",
	"synapse":               "synapse-default",
	"mongo":                 "mongo-default",
	"mysql":                 "mysql-default",
	"notion":                "notion-default",
	"hana":                  "hana-default",
	"shopify":               "shopify-default",
	"gorgias":               "gorgias-default",
	"facebookads":           "facebookads-default",
	"klaviyo":               "klaviyo-default",
	"adjust":                "adjust-default",
	"stripe":                "stripe-default",
	"appsflyer":             "appsflyer-default",
	"kafka":                 "kafka-default",
	"duckdb":                "duckdb-default",
	"clickhouse":            "clickhouse-default",
	"hubspot":               "hubspot-default",
	"google_sheets":         "google-sheets-default",
	"chess":                 "chess-default",
	"airtable":              "airtable-default",
	"zendesk":               "zendesk-default",
	"s3":                    "s3-default",
	"slack":                 "slack-default",
	"asana":                 "asana-default",
	"dynamodb":              "dynamodb-default",
	"googleads":             "googleads-default",
	"tiktokads":             "tiktokads-default",
	"appstore":              "appstore-default",
	"gcs":                   "gcs-default",
}

var SupportedFileSuffixes = []string{"asset.yml", "asset.yaml", ".sql", ".py", "task.yml", "task.yaml"}

type (
	Schedule           string
	TaskDefinitionType string
)

type ExecutableFile struct {
	Name    string `json:"name"`
	Path    string `json:"path"`
	Content string `json:"content"`
}

type TaskDefinitionFile struct {
	Name string             `json:"name"`
	Path string             `json:"path"`
	Type TaskDefinitionType `json:"type"`
}

type DefinitionFile struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

type TaskSchedule struct {
	Days []string `json:"days"`
}

type Notifications struct {
	Slack   []SlackNotification   `yaml:"slack" json:"slack" mapstructure:"slack"`
	MSTeams []MSTeamsNotification `yaml:"ms_teams" json:"ms_teams" mapstructure:"ms_teams"`
	Discord []DiscordNotification `yaml:"discord" json:"discord" mapstructure:"discord"`
}

type DefaultTrueBool struct { //nolint:recvcheck
	Value *bool
}

func (b *DefaultTrueBool) UnmarshalJSON(data []byte) error {
	if data == nil {
		return nil
	}

	var v bool
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	b.Value = &v
	return nil
}

func (b DefaultTrueBool) IsZero() bool {
	return b.Value == nil || *b.Value
}

func (b DefaultTrueBool) MarshalJSON() ([]byte, error) {
	if b.Value == nil {
		return []byte("true"), nil
	}

	return json.Marshal(*b.Value)
}

func (b *DefaultTrueBool) UnmarshalYAML(value *yaml.Node) error {
	var multi *bool
	err := value.Decode(&multi)
	if err != nil {
		return err
	}
	b.Value = multi

	return nil
}

func (b *DefaultTrueBool) Bool() bool {
	if b.Value == nil {
		return true
	}

	return *b.Value
}

func (b DefaultTrueBool) MarshalYAML() (interface{}, error) {
	if b.Value == nil || *b.Value {
		return nil, nil
	}

	return *b.Value, nil
}

type NotificationCommon struct {
	Success DefaultTrueBool `yaml:"success" json:"success" mapstructure:"success"`
	Failure DefaultTrueBool `yaml:"failure" json:"failure" mapstructure:"failure"`
}

type SlackNotification struct {
	Channel            string `json:"channel"`
	NotificationCommon `yaml:",inline" json:",inline" mapstructure:",inline"`
}

type MSTeamsNotification struct {
	Connection         string `yaml:"connection" json:"connection" mapstructure:"connection"`
	NotificationCommon `yaml:",inline" json:",inline" mapstructure:",inline"`
}

type DiscordNotification struct {
	Connection         string `yaml:"connection" json:"connection" mapstructure:"connection"`
	NotificationCommon `yaml:",inline" json:",inline" mapstructure:",inline"`
}

func (n Notifications) MarshalJSON() ([]byte, error) {
	slack := make([]SlackNotification, 0, len(n.Slack))
	MSTeams := make([]MSTeamsNotification, 0, len(n.MSTeams))
	discord := make([]DiscordNotification, 0, len(n.MSTeams))
	for _, s := range n.Slack {
		if !reflect.ValueOf(s).IsZero() {
			slack = append(slack, s)
		}
	}
	for _, s := range n.MSTeams {
		if !reflect.ValueOf(s).IsZero() {
			MSTeams = append(MSTeams, s)
		}
	}
	for _, s := range n.Discord {
		if !reflect.ValueOf(s).IsZero() {
			discord = append(discord, s)
		}
	}

	return json.Marshal(struct {
		Slack   []SlackNotification   `json:"slack"`
		MSTeams []MSTeamsNotification `json:"ms_teams"`
		Discord []DiscordNotification `json:"discord"`
	}{
		Slack:   slack,
		MSTeams: MSTeams,
		Discord: discord,
	})
}

type MaterializationType string

const (
	MaterializationTypeNone  MaterializationType = ""
	MaterializationTypeView  MaterializationType = "view"
	MaterializationTypeTable MaterializationType = "table"
)

type (
	MaterializationStrategy        string
	MaterializationTimeGranularity string
)

const (
	MaterializationStrategyNone             MaterializationStrategy        = ""
	MaterializationStrategyCreateReplace    MaterializationStrategy        = "create+replace"
	MaterializationStrategyDeleteInsert     MaterializationStrategy        = "delete+insert"
	MaterializationStrategyAppend           MaterializationStrategy        = "append"
	MaterializationStrategyMerge            MaterializationStrategy        = "merge"
	MaterializationStrategyTimeInterval     MaterializationStrategy        = "time_interval"
	MaterializationTimeGranularityDate      MaterializationTimeGranularity = "date"
	MaterializationTimeGranularityTimestamp MaterializationTimeGranularity = "timestamp"
)

var AllAvailableMaterializationStrategies = []MaterializationStrategy{
	MaterializationStrategyCreateReplace,
	MaterializationStrategyDeleteInsert,
	MaterializationStrategyAppend,
	MaterializationStrategyMerge,
	MaterializationStrategyTimeInterval,
}

type Materialization struct {
	Type            MaterializationType            `json:"type" yaml:"type,omitempty" mapstructure:"type"`
	Strategy        MaterializationStrategy        `json:"strategy" yaml:"strategy,omitempty" mapstructure:"strategy"`
	PartitionBy     string                         `json:"partition_by" yaml:"partition_by,omitempty" mapstructure:"partition_by"`
	ClusterBy       []string                       `json:"cluster_by" yaml:"cluster_by,omitempty" mapstructure:"cluster_by"`
	IncrementalKey  string                         `json:"incremental_key" yaml:"incremental_key,omitempty" mapstructure:"incremental_key"`
	TimeGranularity MaterializationTimeGranularity `json:"time_granularity" yaml:"time_granularity,omitempty" mapstructure:"time_granularity"`
}

func (m Materialization) MarshalJSON() ([]byte, error) {
	if m.Type == "" && m.Strategy == "" && m.PartitionBy == "" && len(m.ClusterBy) == 0 && m.IncrementalKey == "" {
		return []byte("null"), nil
	}

	type Alias Materialization
	return json.Marshal(&struct {
		*Alias
	}{
		Alias: (*Alias)(&m),
	})
}

type ColumnCheckValue struct {
	IntArray    *[]int    `json:"int_array"`
	Int         *int      `json:"int"`
	Float       *float64  `json:"float"`
	StringArray *[]string `json:"string_array"`
	String      *string   `json:"string"`
	Bool        *bool     `json:"bool"`
}

func (ccv *ColumnCheckValue) MarshalJSON() ([]byte, error) {
	actual := *ccv

	if actual.IntArray != nil {
		return json.Marshal(actual.IntArray)
	}
	if actual.Int != nil {
		return json.Marshal(actual.Int)
	}
	if actual.Float != nil {
		return json.Marshal(actual.Float)
	}
	if actual.StringArray != nil {
		return json.Marshal(actual.StringArray)
	}
	if actual.String != nil {
		return json.Marshal(actual.String)
	}
	if actual.Bool != nil {
		return json.Marshal(actual.Bool)
	}

	return []byte("null"), nil
}

func (ccv ColumnCheckValue) MarshalYAML() (interface{}, error) {
	if ccv.IntArray != nil {
		return ccv.IntArray, nil
	}
	if ccv.Int != nil {
		return ccv.Int, nil
	}
	if ccv.Float != nil {
		return ccv.Float, nil
	}
	if ccv.StringArray != nil {
		return ccv.StringArray, nil
	}
	if ccv.String != nil {
		return ccv.String, nil
	}
	if ccv.Bool != nil {
		return ccv.Bool, nil
	}
	return nil, nil
}

func (ccv *ColumnCheckValue) UnmarshalJSON(data []byte) error {
	var temp interface{}
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	if temp == nil {
		return nil
	}

	switch v := temp.(type) {
	case []interface{}:

		var intSlice []int
		if err := json.Unmarshal(data, &intSlice); err == nil {
			ccv.IntArray = &intSlice
			return nil
		}

		var stringSlice []string
		if err := json.Unmarshal(data, &stringSlice); err == nil {
			ccv.StringArray = &stringSlice
			return nil
		}

		return fmt.Errorf("unable to parse JSON structure %v into ColumnCheckValue", v)
	case float64:

		if v == float64(int(v)) {
			i := int(v)
			ccv.Int = &i
			return nil
		}

		ccv.Float = &v
	case string:
		ccv.String = &v
	case bool:
		ccv.Bool = &v
	default:
		return fmt.Errorf("unexpected type %T", v)
	}

	return nil
}

func (ccv *ColumnCheckValue) ToString() string {
	if ccv.IntArray != nil {
		var ints []string
		for _, i := range *ccv.IntArray {
			ints = append(ints, strconv.Itoa(i))
		}
		return fmt.Sprintf("[%s]", strings.Join(ints, ", "))
	}
	if ccv.Int != nil {
		return strconv.Itoa(*ccv.Int)
	}
	if ccv.Float != nil {
		return fmt.Sprintf("%f", *ccv.Float)
	}
	if ccv.StringArray != nil {
		return strings.Join(*ccv.StringArray, ", ")
	}
	if ccv.String != nil {
		return *ccv.String
	}
	if ccv.Bool != nil {
		return strconv.FormatBool(*ccv.Bool)
	}

	return ""
}

type ColumnCheck struct {
	ID       string           `json:"id" yaml:"-" mapstructure:"-"`
	Name     string           `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	Value    ColumnCheckValue `json:"value" yaml:"value,omitempty" mapstructure:"value"`
	Blocking DefaultTrueBool  `json:"blocking" yaml:"blocking,omitempty" mapstructure:"blocking"`
}

func NewColumnCheck(assetName, columnName, name string, value ColumnCheckValue, blocking *bool) ColumnCheck {
	return ColumnCheck{
		ID:       hash(fmt.Sprintf("%s-%s-%s", assetName, columnName, name)),
		Name:     strings.TrimSpace(name),
		Value:    value,
		Blocking: DefaultTrueBool{Value: blocking},
	}
}

type EntityAttribute struct {
	Entity    string `json:"entity"`
	Attribute string `json:"attribute"`
}

type UpstreamColumn struct {
	Column string `json:"column" yaml:"column,omitempty" mapstructure:"column"`
	Table  string `json:"table" yaml:"table,omitempty" mapstructure:"table"`
}

type Column struct {
	EntityAttribute *EntityAttribute  `json:"entity_attribute" yaml:"-" mapstructure:"-"`
	Name            string            `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	Type            string            `json:"type" yaml:"type,omitempty" mapstructure:"type"`
	Description     string            `json:"description" yaml:"description,omitempty" mapstructure:"description"`
	PrimaryKey      bool              `json:"primary_key" yaml:"primary_key,omitempty" mapstructure:"primary_key"`
	UpdateOnMerge   bool              `json:"update_on_merge" yaml:"update_on_merge,omitempty" mapstructure:"update_on_merge"`
	Extends         string            `json:"-" yaml:"extends,omitempty" mapstructure:"extends"`
	Checks          []ColumnCheck     `json:"checks" yaml:"checks,omitempty" mapstructure:"checks"`
	Upstreams       []*UpstreamColumn `json:"upstreams" yaml:"-" mapstructure:"-"`
}

func (c *Column) HasCheck(check string) bool {
	for _, cc := range c.Checks {
		if cc.Name == check {
			return true
		}
	}

	return false
}

type AssetType string

var AssetTypeConnectionMapping = map[AssetType]string{
	AssetTypeBigqueryQuery:        "google_cloud_platform",
	AssetTypeBigqueryTableSensor:  "google_cloud_platform",
	AssetTypeBigquerySeed:         "google_cloud_platform",
	AssetTypeBigquerySource:       "google_cloud_platform",
	AssetTypeSnowflakeQuery:       "snowflake",
	AssetTypeSnowflakeQuerySensor: "snowflake",
	AssetTypeSnowflakeSeed:        "snowflake",
	AssetTypePostgresQuery:        "postgres",
	AssetTypePostgresSeed:         "postgres",
	AssetTypeRedshiftQuery:        "redshift",
	AssetTypeRedshiftSeed:         "redshift",
	AssetTypeMsSQLQuery:           "mssql",
	AssetTypeMsSQLSeed:            "mssql",
	AssetTypeDatabricksQuery:      "databricks",
	AssetTypeDatabricksSeed:       "databricks",
	AssetTypeSynapseQuery:         "synapse",
	AssetTypeSynapseSeed:          "synapse",
	AssetTypeAthenaQuery:          "athena",
	AssetTypeAthenaSeed:           "athena",
	AssetTypeAthenaSQLSensor:      "athena",
	AssetTypeDuckDBQuery:          "duckdb",
	AssetTypeDuckDBSeed:           "duckdb",
	AssetTypeClickHouse:           "clickhouse",
	AssetTypeClickHouseSeed:       "clickhouse",
}

var IngestrTypeConnectionMapping = map[string]AssetType{
	"athena":     AssetTypeAthenaQuery,
	"bigquery":   AssetTypeBigqueryQuery,
	"snowflake":  AssetTypeSnowflakeQuery,
	"postgres":   AssetTypePostgresQuery,
	"redshift":   AssetTypeRedshiftQuery,
	"mssql":      AssetTypeMsSQLQuery,
	"databricks": AssetTypeDatabricksQuery,
	"synapse":    AssetTypeSynapseQuery,
	"duckdb":     AssetTypeDuckDBQuery,
	"clickhouse": AssetTypeClickHouse,
}

type SecretMapping struct {
	SecretKey   string `json:"secret_key"`
	InjectedKey string `json:"injected_key"`
}

func (s SecretMapping) MarshalYAML() (interface{}, error) {
	if s.SecretKey == "" {
		return nil, nil
	}

	type Alias struct {
		Key      string `yaml:"key"`
		InjectAs string `yaml:"inject_as"`
	}

	return Alias{
		Key:      s.SecretKey,
		InjectAs: s.InjectedKey,
	}, nil
}

type CustomCheck struct {
	ID          string          `json:"id" yaml:"-" mapstructure:"-"`
	Name        string          `json:"name" yaml:"name" mapstructure:"name"`
	Description string          `json:"description" yaml:"description,omitempty" mapstructure:"description"`
	Value       int64           `json:"value" yaml:"value" mapstructure:"value"`
	Blocking    DefaultTrueBool `json:"blocking" yaml:"blocking,omitempty" mapstructure:"blocking"`
	Query       string          `json:"query" yaml:"query" mapstructure:"query"`
}

type DependsColumn struct {
	Name  string `json:"name" yaml:"name" mapstructure:"name"`
	Usage string `json:"usage" yaml:"usage" mapstructure:"usage"`
}

type Upstream struct {
	Type     string          `json:"type" yaml:"type" mapstructure:"type"`
	Value    string          `json:"value" yaml:"value" mapstructure:"value"`
	Metadata EmptyStringMap  `json:"metadata,omitempty" yaml:"metadata,omitempty" mapstructure:"metadata"`
	Columns  []DependsColumn `json:"columns" yaml:"columns,omitempty" mapstructure:"columns"`
	Mode     UpstreamMode    `json:"mode" yaml:"mode,omitempty" mapstructure:"mode"`
}

func (u Upstream) MarshalYAML() (interface{}, error) {
	if u.Type == "" || u.Type == "asset" {
		return u.Value, nil
	}

	if strings.ToLower(u.Type) == "uri" {
		return map[string]string{
			"uri": u.Value,
		}, nil
	}

	return nil, nil
}

type SnowflakeConfig struct {
	Warehouse string `json:"warehouse"`
}

func (s SnowflakeConfig) MarshalJSON() ([]byte, error) {
	if s.Warehouse == "" {
		return []byte("null"), nil
	}

	return json.Marshal(s)
}

type AthenaConfig struct {
	Location string `json:"location"`
}

func (s AthenaConfig) MarshalJSON() ([]byte, error) {
	if s.Location == "" {
		return []byte("null"), nil
	}

	return json.Marshal(s)
}

type Asset struct {
	ID              string             `json:"id" yaml:"-" mapstructure:"-"`
	URI             string             `json:"uri" yaml:"uri,omitempty" mapstructure:"uri"`
	Name            string             `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	Type            AssetType          `json:"type" yaml:"type,omitempty" mapstructure:"type"`
	Description     string             `json:"description" yaml:"description,omitempty" mapstructure:"description"`
	Connection      string             `json:"connection" yaml:"connection,omitempty" mapstructure:"connection"`
	Tags            EmptyStringArray   `json:"tags" yaml:"tags,omitempty" mapstructure:"tags"`
	Materialization Materialization    `json:"materialization" yaml:"materialization,omitempty" mapstructure:"materialization"`
	Upstreams       []Upstream         `json:"upstreams" yaml:"depends,omitempty" mapstructure:"depends"`
	Image           string             `json:"image" yaml:"image,omitempty" mapstructure:"image"`
	Instance        string             `json:"instance" yaml:"instance,omitempty" mapstructure:"instance"`
	Owner           string             `json:"owner" yaml:"owner,omitempty" mapstructure:"owner"`
	ExecutableFile  ExecutableFile     `json:"executable_file" yaml:"-" mapstructure:"-"`
	DefinitionFile  TaskDefinitionFile `json:"definition_file" yaml:"-" mapstructure:"-"`
	Parameters      EmptyStringMap     `json:"parameters" yaml:"parameters,omitempty" mapstructure:"parameters"`
	Secrets         []SecretMapping    `json:"secrets" yaml:"secrets,omitempty" mapstructure:"secrets"`
	Extends         []string           `json:"extends" yaml:"extends,omitempty" mapstructure:"extends"`
	Columns         []Column           `json:"columns" yaml:"columns,omitempty" mapstructure:"columns"`
	CustomChecks    []CustomCheck      `json:"custom_checks" yaml:"custom_checks,omitempty" mapstructure:"custom_checks"`
	Metadata        EmptyStringMap     `json:"metadata" yaml:"metadata,omitempty" mapstructure:"metadata"`
	Snowflake       SnowflakeConfig    `json:"snowflake" yaml:"snowflake,omitempty" mapstructure:"snowflake"`
	Athena          AthenaConfig       `json:"athena" yaml:"athena,omitempty" mapstructure:"athena"`

	upstream   []*Asset
	downstream []*Asset
}

func (a *Asset) AddUpstream(asset *Asset) {
	a.upstream = append(a.upstream, asset)
}

func (a *Asset) PrefixSchema(prefix string) {
	if prefix == "" {
		return
	}

	nameParts := strings.Split(a.Name, ".")
	if len(nameParts) == 2 {
		a.Name = prefix + nameParts[0] + "." + nameParts[1]
	}
}

// removeRedundanciesBeforePersisting aims to remove unnecessary configuration from the asset.
// This is particularly useful when we save a formatted version of the asset itself.
func (a *Asset) removeRedundanciesBeforePersisting() {
	a.clearDuplicateUpstreams()
	a.removeExtraSpacesAtLineEndingsInTextContent()

	// python assets don't require a type anymore
	if a.Type == AssetTypePython && strings.HasSuffix(a.ExecutableFile.Path, ".py") {
		a.Type = ""
	}
}

// removeRedundanciesBeforePersisting aims to remove unnecessary configuration from the asset.
// This is particularly useful when we save a formatted version of the asset itself.
func (a *Asset) clearDuplicateUpstreams() {
	if a.Upstreams == nil {
		return
	}

	seenUpstreams := make(map[string]bool, len(a.Upstreams))
	uniqueUpstreams := make([]*Upstream, 0, len(a.Upstreams))
	for _, u := range a.Upstreams {
		key := fmt.Sprintf("%s-%s", u.Type, u.Value)
		if _, ok := seenUpstreams[key]; ok {
			continue
		}
		seenUpstreams[key] = true
		uniqueUpstreams = append(uniqueUpstreams, &u)
	}

	a.Upstreams = make([]Upstream, len(uniqueUpstreams))
	for i, val := range uniqueUpstreams {
		a.Upstreams[i] = *val
	}
}

func ClearSpacesAtLineEndings(content string) string {
	lines := strings.Split(content, "\n")
	for i, l := range lines {
		for strings.HasSuffix(l, " ") {
			l = strings.TrimSuffix(l, " ")
		}
		lines[i] = l
	}

	return strings.Join(lines, "\n")
}

// removeExtraSpacesAtLineEndingsInTextContent clears text content from various strings that might be multi-line.
//
// You might be thinking "why the hell would anyone want to do that?", but hear me out.
//
// Basically, when marshalling a string to YAML, the go-yaml library will have two ways of representing strings:
//   - if the string contains no lines that end with a space, it'll convert them to multi-line yaml, which is good.
//   - however, if there's a space at the end of any line within that multi-line string, then the lib will instead convert
//     the content to be a single-line string with escaped "\n" characters, which breaks all the existing multi-line documentation.
//
// This was the only solution I could think of to not break multi-line strings, happy to hear better ideas that would make the existing tests pass.
// I am also open to performance improvements here, as I think this is a suboptimal implementation.
func (a *Asset) removeExtraSpacesAtLineEndingsInTextContent() {
	if a.Description != "" && strings.Contains(a.Description, "\n") {
		a.Description = ClearSpacesAtLineEndings(a.Description)
	}

	for i := range a.Columns {
		a.Columns[i].Description = ClearSpacesAtLineEndings(a.Columns[i].Description)
	}
	for i := range a.CustomChecks {
		a.CustomChecks[i].Description = ClearSpacesAtLineEndings(a.CustomChecks[i].Description)
		a.CustomChecks[i].Query = ClearSpacesAtLineEndings(a.CustomChecks[i].Query)
	}
}

func (a *Asset) GetUpstream() []*Asset {
	return a.upstream
}

func (a *Asset) GetFullUpstream() []*Asset {
	upstream := make([]*Asset, 0)

	for _, asset := range a.upstream {
		upstream = append(upstream, asset)
		upstream = append(upstream, asset.GetFullUpstream()...)
	}

	return uniqueAssets(upstream)
}

func (a *Asset) AddDownstream(asset *Asset) {
	a.downstream = append(a.downstream, asset)
}

func (a *Asset) GetDownstream() []*Asset {
	return a.downstream
}

func (a *Asset) GetFullDownstream() []*Asset {
	downstream := make([]*Asset, 0)

	for _, asset := range a.downstream {
		downstream = append(downstream, asset)
		downstream = append(downstream, asset.GetFullDownstream()...)
	}

	return uniqueAssets(downstream)
}

func (a *Asset) ColumnNames() []string {
	columns := make([]string, len(a.Columns))
	for i, c := range a.Columns {
		columns[i] = c.Name
	}
	return columns
}

func (a *Asset) ColumnNamesWithUpdateOnMerge() []string {
	columns := make([]string, 0)
	for _, c := range a.Columns {
		if c.UpdateOnMerge {
			columns = append(columns, c.Name)
		}
	}
	return columns
}

func (a *Asset) ColumnNamesWithPrimaryKey() []string {
	columns := make([]string, 0)
	for _, c := range a.Columns {
		if c.PrimaryKey {
			columns = append(columns, c.Name)
		}
	}
	return columns
}

func (a *Asset) GetColumnWithName(name string) *Column {
	for _, c := range a.Columns {
		if strings.EqualFold(c.Name, name) {
			return &c
		}
	}

	return nil
}

func (a *Asset) CheckCount() int {
	checkCount := 0
	for _, c := range a.Columns {
		checkCount += len(c.Checks)
	}

	checkCount += len(a.CustomChecks)
	return checkCount
}

func (a *Asset) EnrichFromEntityAttributes(entities []*glossary.Entity) error {
	entityMap := make(map[string]*glossary.Entity, len(entities))
	for _, e := range entities {
		entityMap[e.Name] = e
	}

	for i, c := range a.Columns {
		if c.EntityAttribute == nil {
			continue
		}

		entity := c.EntityAttribute.Entity

		e, ok := entityMap[entity]
		if !ok {
			return errors.Errorf("entity '%s' not found", entity)
		}

		attr, ok := e.Attributes[c.EntityAttribute.Attribute]
		if !ok {
			return errors.Errorf("attribute '%s' not found in entity '%s'", c.EntityAttribute.Attribute, entity)
		}

		if c.Name == "" {
			a.Columns[i].Name = attr.Name
		}

		if c.Type == "" {
			a.Columns[i].Type = attr.Type
		}

		if c.Description == "" {
			a.Columns[i].Description = attr.Description
		}
	}

	return nil
}

func (a *Asset) Persist(fs afero.Fs) error {
	if a == nil {
		return errors.New("failed to build an asset, therefore cannot persist it")
	}

	// Reuse the logic from PersistWithoutWriting
	content, err := a.FormatContent()
	if err != nil {
		return errors.Wrap(err, "failed to generate content for persistence")
	}

	// Write the generated content to the file
	filePath := a.ExecutableFile.Path
	return afero.WriteFile(fs, filePath, content, 0o644)
}

func (a *Asset) FormatContent() ([]byte, error) {
	if a == nil {
		return nil, errors.New("failed to build an asset, therefore cannot persist it")
	}

	a.removeRedundanciesBeforePersisting()

	buf := bytes.NewBuffer(nil)
	enc := yaml.NewEncoder(buf)
	enc.SetIndent(2)

	err := enc.Encode(a)
	if err != nil {
		return nil, err
	}

	yamlConfig := buf.Bytes()

	keysToAddSpace := []string{"custom_checks", "depends", "columns", "materialization", "secrets", "parameters"}
	for _, key := range keysToAddSpace {
		yamlConfig = bytes.ReplaceAll(yamlConfig, []byte("\n"+key+":"), []byte("\n\n"+key+":"))
	}

	beginning := ""
	end := ""
	executableContent := ""

	if strings.HasSuffix(a.ExecutableFile.Path, ".sql") {
		beginning = "/* " + configMarkerString + "\n\n"
		end = "\n" + configMarkerString + " */" + "\n\n"
		executableContent = a.ExecutableFile.Content
	}

	if strings.HasSuffix(a.ExecutableFile.Path, ".py") {
		beginning = `"""` + configMarkerString + "\n\n"
		end = "\n" + configMarkerString + `"""` + "\n\n"
		executableContent = a.ExecutableFile.Content
	}

	stringVersion := beginning + string(yamlConfig) + end + executableContent
	if !strings.HasSuffix(stringVersion, "\n") {
		stringVersion += "\n"
	}

	return []byte(stringVersion), nil
}

func uniqueAssets(assets []*Asset) []*Asset {
	seenValues := make(map[string]bool, len(assets))
	unique := make([]*Asset, 0, len(assets))
	for _, value := range assets {
		if seenValues[value.Name] {
			continue
		}

		seenValues[value.Name] = true
		unique = append(unique, value)
	}
	return unique
}

type EmptyStringMap map[string]string

func (m EmptyStringMap) MarshalJSON() ([]byte, error) { //nolint: stylecheck
	if m == nil {
		return []byte{'{', '}'}, nil
	}

	return json.Marshal(map[string]string(m))
}

func (b *EmptyStringMap) UnmarshalJSON(data []byte) error {
	if data == nil {
		return nil
	}

	var v map[string]string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	if len(v) == 0 {
		return nil
	}

	*b = v
	return nil
}

type EmptyStringArray []string

func (a EmptyStringArray) MarshalJSON() ([]byte, error) {
	if a == nil {
		return []byte{'[', ']'}, nil
	}

	return json.Marshal([]string(a))
}

func (a *EmptyStringArray) UnmarshalJSON(data []byte) error {
	if data == nil {
		return nil
	}

	var v []string
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	if len(v) == 0 {
		return nil
	}

	*a = v
	return nil
}

func PipelineFromPath(filePath string, fs afero.Fs) (*Pipeline, error) {
	yamlError := new(path.YamlParseError)
	var pl Pipeline
	err := path.ReadYaml(fs, filePath, &pl)
	if err != nil && errors.As(err, &yamlError) {
		return nil, &ParseError{Msg: "error parsing bruin pipeline definition :" + err.Error()}
	}

	if err != nil {
		return nil, errors.Wrapf(err, "error parsing bruin pipeline file at '%s'", filePath)
	}

	pl.Assets = make([]*Asset, 0)
	pl.DefinitionFile.Name = filepath.Base(filePath)
	pl.DefinitionFile.Path = filePath
	return &pl, nil
}

type MetadataPush struct {
	Global   bool `json:"-"`
	BigQuery bool `json:"bigquery" yaml:"bigquery" mapstructure:"bigquery"`
}

func (mp *MetadataPush) HasAnyEnabled() bool {
	return mp.BigQuery || mp.Global
}

type Pipeline struct {
	LegacyID           string                 `json:"legacy_id" yaml:"id" mapstructure:"id"`
	Name               string                 `json:"name" yaml:"name" mapstructure:"name"`
	Schedule           Schedule               `json:"schedule" yaml:"schedule" mapstructure:"schedule"`
	StartDate          string                 `json:"start_date" yaml:"start_date" mapstructure:"start_date"`
	DefinitionFile     DefinitionFile         `json:"definition_file"`
	DefaultConnections EmptyStringMap         `json:"default_connections" yaml:"default_connections" mapstructure:"default_connections"`
	Assets             []*Asset               `json:"assets"`
	Notifications      Notifications          `json:"notifications" yaml:"notifications" mapstructure:"notifications"`
	Catchup            bool                   `json:"catchup" yaml:"catchup" mapstructure:"catchup"`
	MetadataPush       MetadataPush           `json:"metadata_push" yaml:"metadata_push" mapstructure:"metadata_push"`
	Retries            int                    `json:"retries" yaml:"retries" mapstructure:"retries"`
	DefaultValues      *DefaultValues         `json:"default,omitempty" yaml:"default,omitempty" mapstructure:"default,omitempty"`
	Commit             string                 `json:"commit"`
	Snapshot           string                 `json:"snapshot"`
	TasksByType        map[AssetType][]*Asset `json:"-"`
	tasksByName        map[string]*Asset
}

type DefaultValues struct {
	Type       string            `json:"type" yaml:"type" mapstructure:"type"`
	Parameters map[string]string `json:"parameters" yaml:"parameters" mapstructure:"parameters"`
	Secrets    []secretMapping   `json:"secrets" yaml:"secrets" mapstructure:"secrets"`
}

func (p *Pipeline) GetCompatibilityHash() string {
	parts := make([]string, 0, len(p.Assets)+1)
	parts = append(parts, p.Name)
	for _, asset := range p.Assets {
		assetPart := fmt.Sprintf(":%s{", asset.Name)
		for _, upstream := range asset.Upstreams {
			assetPart += fmt.Sprintf(":%s:%s:", upstream.Value, upstream.Type)
		}
		assetPart += "}"
		parts = append(parts, assetPart)
	}
	parts = append(parts, ":")
	hash := sha256.New()
	hash.Write([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(hash.Sum(nil))
}

func (p *Pipeline) GetAllConnectionNamesForAsset(asset *Asset) ([]string, error) {
	assetType := asset.Type
	if assetType == AssetTypePython { //nolint
		secretKeys := make([]string, 0)
		for _, secret := range asset.Secrets {
			secretKeys = append(secretKeys, secret.SecretKey)
		}
		return secretKeys, nil
	} else if assetType == AssetTypeIngestr {
		ingestrSource, ok := asset.Parameters["source_connection"]
		if !ok {
			return []string{}, errors.Errorf("No source connection in asset")
		}

		ingestrDestination, ok := asset.Parameters["destination_connection"]
		if ok {
			return []string{ingestrDestination, ingestrSource}, nil
		}

		// if destination connection not specified, we infer from destination type
		assetType, ok = IngestrTypeConnectionMapping[asset.Parameters["destination"]]
		if !ok {
			return []string{}, errors.Errorf("connection type could not be inferred for destination '%s', please specify a `connection` key in the asset", asset.Parameters["destination"])
		}

		mapping, ok := AssetTypeConnectionMapping[assetType]
		if !ok {
			return []string{}, errors.Errorf("No connection mapping found for asset type:'%s' (%s)", assetType, asset.Name)
		}
		conn, ok := p.DefaultConnections[mapping]
		if ok {
			ingestrDestination = conn
			return []string{ingestrDestination, ingestrSource}, nil
		}

		ingestrDestination, ok = defaultMapping[mapping]
		if ok {
			return []string{ingestrDestination, ingestrSource}, nil
		}

		return []string{}, errors.Errorf("No default connection for type: '%s'", assetType)
	} else {
		conn, err := p.GetConnectionNameForAsset(asset)
		if err != nil {
			return []string{}, err
		}

		return []string{conn}, nil
	}
}

func (p *Pipeline) GetConnectionNameForAsset(asset *Asset) (string, error) {
	if asset.Connection != "" {
		return asset.Connection, nil
	}

	assetType := asset.Type
	var ok bool
	if assetType == AssetTypeIngestr {
		assetType, ok = IngestrTypeConnectionMapping[asset.Parameters["destination"]]
		if !ok {
			return "", errors.Errorf("connection type could not be inferred for destination '%s', please specify a `connection` key in the asset", asset.Parameters["destination"])
		}
	} else if assetType == AssetTypePython || assetType == AssetTypeEmpty {
		assetType = p.GetMajorityAssetTypesFromSQLAssets(AssetTypeBigqueryQuery)
	}

	mapping, ok := AssetTypeConnectionMapping[assetType]
	if !ok {
		return "", errors.Errorf("no connection mapping found for asset type '%s'", assetType)
	}

	conn, ok := p.DefaultConnections[mapping]
	if ok {
		return conn, nil
	}

	defaultConn, ok := defaultMapping[mapping]

	if !ok {
		return "", errors.Errorf("no default connection found for type '%s'", assetType)
	}

	return defaultConn, nil
}

// WipeContentOfAssets removes the content of the executable files of all assets in the pipeline.
// This is useful when we want to serialize the pipeline to JSON and we don't want to include the content of the assets.
func (p *Pipeline) WipeContentOfAssets() {
	for _, asset := range p.Assets {
		asset.ExecutableFile.Content = ""
	}
}

func (p *Pipeline) GetMajorityAssetTypesFromSQLAssets(defaultIfNone AssetType) AssetType {
	taskTypeCounts := map[AssetType]int{
		AssetTypeBigqueryQuery:   0,
		AssetTypeSnowflakeQuery:  0,
		AssetTypePostgresQuery:   0,
		AssetTypeMsSQLQuery:      0,
		AssetTypeDatabricksQuery: 0,
		AssetTypeRedshiftQuery:   0,
		AssetTypeSynapseQuery:    0,
		AssetTypeAthenaQuery:     0,
		AssetTypeDuckDBQuery:     0,
	}
	maxTasks := 0
	maxTaskType := defaultIfNone

	searchTypeMap := make(map[AssetType]bool)
	for t := range taskTypeCounts {
		searchTypeMap[t] = true
	}

	for _, asset := range p.Assets {
		assetType := asset.Type

		if assetType == AssetTypeIngestr {
			ingestrDestination, ok := asset.Parameters["destination"]
			if !ok {
				continue
			}

			assetType, ok = IngestrTypeConnectionMapping[ingestrDestination]
			if !ok {
				continue
			}
		}

		if !searchTypeMap[assetType] {
			continue
		}

		if _, ok := taskTypeCounts[assetType]; !ok {
			taskTypeCounts[assetType] = 0
		}

		taskTypeCounts[assetType]++

		if taskTypeCounts[assetType] > maxTasks {
			maxTasks = taskTypeCounts[assetType]
			maxTaskType = assetType
		} else if taskTypeCounts[assetType] == maxTasks {
			maxTaskType = defaultIfNone
		}
	}

	return maxTaskType
}

func (p *Pipeline) RelativeAssetPath(t *Asset) string {
	absolutePipelineRoot := filepath.Dir(p.DefinitionFile.Path)

	pipelineDirectory, err := filepath.Rel(absolutePipelineRoot, t.DefinitionFile.Path)
	if err != nil {
		return absolutePipelineRoot
	}

	return pipelineDirectory
}

func (p *Pipeline) HasAssetType(taskType AssetType) bool {
	_, ok := p.TasksByType[taskType]
	return ok
}

func (p *Pipeline) ensureTaskNameMapIsFilled() {
	if p.tasksByName != nil {
		return
	}

	p.tasksByName = make(map[string]*Asset)
	for _, asset := range p.Assets {
		p.tasksByName[asset.Name] = asset
	}
}

func (p *Pipeline) GetAssetByPath(assetPath string) *Asset {
	assetPath, err := filepath.Abs(assetPath)
	if err != nil {
		return nil
	}

	for _, asset := range p.Assets {
		if asset.DefinitionFile.Path == assetPath {
			return asset
		}
	}

	return nil
}

func (p *Pipeline) GetAssetByNameCaseInsensitive(assetName string) *Asset {
	for key, asset := range p.tasksByName {
		if strings.EqualFold(key, assetName) {
			return asset
		}
	}

	return nil
}

func (p *Pipeline) GetAssetByName(assetName string) *Asset {
	p.ensureTaskNameMapIsFilled()

	asset, ok := p.tasksByName[assetName]
	if !ok {
		return nil
	}

	return asset
}

func (p *Pipeline) GetAssetsByTag(tag string) []*Asset {
	assets := make([]*Asset, 0)
	for _, asset := range p.Assets {
		for _, t := range asset.Tags {
			if t == tag {
				assets = append(assets, asset)
			}
		}
	}

	return assets
}

type TaskCreator func(path string) (*Asset, error)

type BuilderConfig struct {
	PipelineFileName    []string
	TasksDirectoryName  string
	TasksDirectoryNames []string
	TasksFileSuffixes   []string
}

type glossaryReader interface {
	GetEntities(pathToPipeline string) ([]*glossary.Entity, error)
}

type assetMutator func(ctx context.Context, asset *Asset, foundPipeline *Pipeline) (*Asset, error)

type Builder struct {
	config             BuilderConfig
	yamlTaskCreator    TaskCreator
	commentTaskCreator TaskCreator
	fs                 afero.Fs
	mutators           []assetMutator

	GlossaryReader glossaryReader
}

func (b *Builder) SetGlossaryReader(reader glossaryReader) {
	b.GlossaryReader = reader
}

func (b *Builder) AddMutator(m assetMutator) {
	b.mutators = append(b.mutators, m)
}

type ParseError struct {
	Msg string
}

func (e ParseError) Error() string {
	return e.Msg
}

func NewBuilder(config BuilderConfig, yamlTaskCreator TaskCreator, commentTaskCreator TaskCreator, fs afero.Fs, gr glossaryReader) *Builder {
	b := &Builder{
		config:             config,
		yamlTaskCreator:    yamlTaskCreator,
		commentTaskCreator: commentTaskCreator,
		fs:                 fs,
		GlossaryReader:     gr,
	}

	b.mutators = []assetMutator{
		b.fillGlossaryStuff,
		b.setupDefaultsFromPipeline,
	}

	return b
}

func matchPipelineFileName(filePath string, validFileNames []string) bool {
	for _, fileName := range validFileNames {
		if strings.HasSuffix(filePath, fileName) {
			return true
		}
	}
	return false
}

func resolvePipelineFilePath(basePath string, validFileNames []string, fs afero.Fs) (string, error) {
	for _, fileName := range validFileNames {
		candidatePath := filepath.Join(basePath, fileName)
		if exists, _ := afero.Exists(fs, candidatePath); exists {
			return candidatePath, nil
		}
	}
	return "", fmt.Errorf("no pipeline file found in '%s'. Supported files: %v", basePath, validFileNames)
}

type createPipelineConfig struct {
	parseGitMetadata bool
	isMutate         bool
}

type CreatePipelineOption func(*createPipelineConfig)

func WithGitMetadata() CreatePipelineOption {
	return func(o *createPipelineConfig) {
		o.parseGitMetadata = true
	}
}

func WithMutate() CreatePipelineOption {
	return func(o *createPipelineConfig) {
		o.isMutate = true
	}
}

func (b *Builder) CreatePipelineFromPath(ctx context.Context, pathToPipeline string, opts ...CreatePipelineOption) (*Pipeline, error) {
	config := createPipelineConfig{}
	for _, opt := range opts {
		opt(&config)
	}

	pipelineFilePath := pathToPipeline
	if !matchPipelineFileName(pipelineFilePath, b.config.PipelineFileName) {
		resolvedPath, err := resolvePipelineFilePath(pathToPipeline, b.config.PipelineFileName, b.fs)
		if err != nil {
			return nil, err
		}
		pipelineFilePath = resolvedPath
	} else {
		pathToPipeline = filepath.Dir(pipelineFilePath)
	}
	pipeline, err := PipelineFromPath(pipelineFilePath, b.fs)
	if err != nil {
		return nil, err
	}
	// this is needed until we migrate all the pipelines to use the new naming convention
	if pipeline.Name == "" {
		pipeline.Name = pipeline.LegacyID
	}
	pipeline.TasksByType = make(map[AssetType][]*Asset)
	pipeline.tasksByName = make(map[string]*Asset)

	if config.parseGitMetadata {
		pipeline.Commit, err = git.CurrentCommit(pathToPipeline)
		if err != nil {
			return nil, fmt.Errorf("error parsing commit: %w", err)
		}
	}

	absPipelineFilePath, err := filepath.Abs(pipelineFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "error getting absolute path for pipeline file at '%s'", pipelineFilePath)
	}

	pipeline.DefinitionFile = DefinitionFile{
		Name: filepath.Base(pipelineFilePath),
		Path: absPipelineFilePath,
	}

	taskFiles := make([]string, 0)
	for _, tasksDirectoryName := range b.config.TasksDirectoryNames {
		tasksPath := filepath.Join(pathToPipeline, tasksDirectoryName)
		files, err := path.GetAllFilesRecursive(tasksPath, SupportedFileSuffixes)
		if err != nil {
			continue
		}

		taskFiles = append(taskFiles, files...)
	}

	for _, file := range taskFiles {
		task, err := b.CreateAssetFromFile(file, pipeline)
		if err != nil {
			return nil, err
		}

		if config.isMutate {
			task, err = b.MutateAsset(ctx, task, pipeline)
			if err != nil {
				return nil, err
			}
		}

		if task == nil {
			continue
		}

		task.upstream = make([]*Asset, 0)
		task.downstream = make([]*Asset, 0)

		pipeline.Assets = append(pipeline.Assets, task)

		if _, ok := pipeline.TasksByType[task.Type]; !ok {
			pipeline.TasksByType[task.Type] = make([]*Asset, 0)
		}

		pipeline.TasksByType[task.Type] = append(pipeline.TasksByType[task.Type], task)
		pipeline.tasksByName[task.Name] = task
	}

	var entities []*glossary.Entity
	if b.GlossaryReader != nil {
		entities, err = b.GlossaryReader.GetEntities(pathToPipeline)
		if err != nil {
			return nil, errors.Wrap(err, "error getting entities")
		}
	}

	for _, asset := range pipeline.Assets {
		for _, upstream := range asset.Upstreams {
			if upstream.Type != "asset" {
				continue
			}
			u, ok := pipeline.tasksByName[upstream.Value]
			if !ok {
				continue
			}

			asset.AddUpstream(u)
			u.AddDownstream(asset)
		}

		if len(entities) > 0 {
			err := asset.EnrichFromEntityAttributes(entities)
			if err != nil {
				return nil, errors.Wrap(err, "error enriching asset from entity attributes")
			}
		}
	}

	return pipeline, nil
}

func fileHasSuffix(arr []string, str string) bool {
	for _, a := range arr {
		if strings.HasSuffix(str, a) {
			return true
		}
	}
	return false
}

func (b *Builder) CreateAssetFromFile(filePath string, foundPipeline *Pipeline) (*Asset, error) {
	isSeparateDefinitionFile := false
	creator := b.commentTaskCreator

	if fileHasSuffix(b.config.TasksFileSuffixes, filePath) {
		creator = b.yamlTaskCreator
		isSeparateDefinitionFile = true
	}

	task, err := creator(filePath)
	if err != nil {
		if errors.As(err, &ParseError{}) {
			return nil, err
		}

		return nil, errors.Wrapf(err, "error creating asset from file '%s'", filePath)
	}

	if task == nil {
		return nil, nil
	}

	// if the definition comes from a Python file the asset is always a Python asset, so force it
	// at least that's the hypothesis for now
	if strings.HasSuffix(task.ExecutableFile.Path, ".py") && task.Type == "" {
		task.Type = AssetTypePython
	}

	task.DefinitionFile.Name = filepath.Base(filePath)
	task.DefinitionFile.Path = filePath
	task.DefinitionFile.Type = CommentTask
	if isSeparateDefinitionFile {
		task.DefinitionFile.Type = YamlTask
	}

	return task, nil
}

func (b *Builder) MutateAsset(ctx context.Context, task *Asset, foundPipeline *Pipeline) (*Asset, error) {
	for _, mutator := range b.mutators {
		var err error
		task, err = mutator(ctx, task, foundPipeline)
		if err != nil {
			return nil, err
		}
	}

	return task, nil
}

func (b *Builder) setupDefaultsFromPipeline(ctx context.Context, asset *Asset, foundPipeline *Pipeline) (*Asset, error) {
	if foundPipeline == nil {
		return asset, nil
	}

	if foundPipeline.DefaultValues == nil {
		return asset, nil
	}

	if asset == nil {
		return asset, nil
	}

	if len(asset.Type) == 0 && len(foundPipeline.DefaultValues.Type) > 0 {
		asset.Type = AssetType(foundPipeline.DefaultValues.Type)
	}

	// merge parameters from the default values to asset parameters
	if len(asset.Parameters) == 0 {
		asset.Parameters = EmptyStringMap{}
	}

	for key, value := range foundPipeline.DefaultValues.Parameters {
		if _, exists := asset.Parameters[key]; !exists {
			asset.Parameters[key] = value
		}
	}
	// merge secrets from the default values to asset secrets
	existingSecrets := make(map[string]bool)
	for _, secret := range asset.Secrets {
		existingSecrets[secret.SecretKey] = true
	}
	for _, secret := range foundPipeline.DefaultValues.Secrets {
		secretMap := SecretMapping(secret)
		if !existingSecrets[secretMap.SecretKey] {
			asset.Secrets = append(asset.Secrets, secretMap)
		}
	}

	return asset, nil
}

func (b *Builder) fillGlossaryStuff(ctx context.Context, asset *Asset, foundPipeline *Pipeline) (*Asset, error) {
	if foundPipeline == nil {
		return asset, nil
	}

	if b.GlossaryReader == nil {
		return asset, nil
	}

	if asset == nil {
		return asset, nil
	}

	entities, err := b.GlossaryReader.GetEntities(foundPipeline.DefinitionFile.Path)
	if err != nil {
		return nil, errors.Wrap(err, "error getting entities")
	}

	cache := make(map[string][]Column)
	cacheEntityColumns := make(map[string]bool)
	for _, column := range asset.Columns {
		cacheEntityColumns[column.Extends] = true
	}

	for _, entity := range entities {
		attributeNames := make([]string, 0, len(entity.Attributes))
		for attrName := range entity.Attributes {
			attributeNames = append(attributeNames, attrName)
		}
		sort.Strings(attributeNames)

		cache[entity.Name] = make([]Column, 0)
		for _, attrName := range attributeNames {
			attribute := entity.Attributes[attrName]
			if _, ok := cacheEntityColumns[fmt.Sprintf("%s.%s", entity.Name, attribute.Name)]; !ok {
				cache[entity.Name] = append(cache[entity.Name], Column{
					EntityAttribute: &EntityAttribute{
						Entity:    entity.Name,
						Attribute: attribute.Name,
					},
				})
			}
		}
	}

	for _, extend := range asset.Extends {
		asset.Columns = append(asset.Columns, cache[extend]...)
	}

	return asset, nil
}

func (a *Asset) IsSQLAsset() bool {
	sqlAssetTypes := map[AssetType]bool{
		AssetTypeBigqueryQuery:   true,
		AssetTypeSnowflakeQuery:  true,
		AssetTypePostgresQuery:   true,
		AssetTypeRedshiftQuery:   true,
		AssetTypeMsSQLQuery:      true,
		AssetTypeDatabricksQuery: true,
		AssetTypeSynapseQuery:    true,
		AssetTypeAthenaQuery:     true,
		AssetTypeDuckDBQuery:     true,
		AssetTypeClickHouse:      true,
	}

	return sqlAssetTypes[a.Type]
}
