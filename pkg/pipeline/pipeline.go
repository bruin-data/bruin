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
	"time"

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

	AssetTypeAgentClaudeCode            = AssetType("agent.claude_code")
	AssetTypeAthenaQuery                = AssetType("athena.sql")
	AssetTypeAthenaSeed                 = AssetType("athena.seed")
	AssetTypeAthenaSource               = AssetType("athena.source")
	AssetTypeAthenaSQLSensor            = AssetType("athena.sensor.query")
	AssetTypeAthenaTableSensor          = AssetType("athena.sensor.table")
	AssetTypeBigqueryQuery              = AssetType("bq.sql")
	AssetTypeBigqueryQuerySensor        = AssetType("bq.sensor.query")
	AssetTypeBigquerySeed               = AssetType("bq.seed")
	AssetTypeBigquerySource             = AssetType("bq.source")
	AssetTypeBigqueryTableSensor        = AssetType("bq.sensor.table")
	AssetTypeClickHouse                 = AssetType("clickhouse.sql")
	AssetTypeClickHouseQuerySensor      = AssetType("clickhouse.sensor.query")
	AssetTypeClickHouseSeed             = AssetType("clickhouse.seed")
	AssetTypeClickHouseSource           = AssetType("clickhouse.source")
	AssetTypeClickHouseTableSensor      = AssetType("clickhouse.sensor.table")
	AssetTypeDatabricksQuery            = AssetType("databricks.sql")
	AssetTypeDatabricksQuerySensor      = AssetType("databricks.sensor.query")
	AssetTypeDatabricksSeed             = AssetType("databricks.seed")
	AssetTypeDatabricksSource           = AssetType("databricks.source")
	AssetTypeDatabricksTableSensor      = AssetType("databricks.sensor.table")
	AssetTypeDataprocServerlessPyspark  = AssetType("dataproc_serverless.pyspark")
	AssetTypeDomo                       = AssetType("domo")
	AssetTypeDuckDBQuery                = AssetType("duckdb.sql")
	AssetTypeDuckDBQuerySensor          = AssetType("duckdb.sensor.query")
	AssetTypeDuckDBSeed                 = AssetType("duckdb.seed")
	AssetTypeDuckDBSource               = AssetType("duckdb.source")
	AssetTypeElasticsearch              = AssetType("elasticsearch")
	AssetTypeEmpty                      = AssetType("empty")
	AssetTypeEMRServerlessPyspark       = AssetType("emr_serverless.pyspark")
	AssetTypeEMRServerlessSpark         = AssetType("emr_serverless.spark")
	AssetTypeGoodData                   = AssetType("gooddata")
	AssetTypeGrafana                    = AssetType("grafana")
	AssetTypeIngestr                    = AssetType("ingestr")
	AssetTypeLooker                     = AssetType("looker")
	AssetTypeLookerStudio               = AssetType("looker_studio")
	AssetTypeMetabase                   = AssetType("metabase")
	AssetTypeModeBI                     = AssetType("modebi")
	AssetTypeMotherduckQuery            = AssetType("motherduck.sql")
	AssetTypeMsSQLQuery                 = AssetType("ms.sql")
	AssetTypeMsSQLQuerySensor           = AssetType("ms.sensor.query")
	AssetTypeMsSQLSeed                  = AssetType("ms.seed")
	AssetTypeMsSQLSource                = AssetType("ms.source")
	AssetTypeMsSQLTableSensor           = AssetType("ms.sensor.table")
	AssetTypeFabricWarehouseQuery       = AssetType("fw.sql")
	AssetTypeFabricWarehouseQuerySensor = AssetType("fw.sensor.query")
	AssetTypeFabricWarehouseSeed        = AssetType("fw.seed")
	AssetTypeFabricWarehouseTableSensor = AssetType("fw.sensor.table")
	AssetTypeMySQLQuery                 = AssetType("my.sql")
	AssetTypeMySQLQuerySensor           = AssetType("my.sensor.query")
	AssetTypeMySQLSeed                  = AssetType("my.seed")
	AssetTypeMySQLTableSensor           = AssetType("my.sensor.table")
	AssetTypeOracleQuery                = AssetType("oracle.sql")
	AssetTypeOracleSource               = AssetType("oracle.source")
	AssetTypePostgresQuery              = AssetType("pg.sql")
	AssetTypePostgresQuerySensor        = AssetType("pg.sensor.query")
	AssetTypePostgresSeed               = AssetType("pg.seed")
	AssetTypePostgresSource             = AssetType("pg.source")
	AssetTypePostgresTableSensor        = AssetType("pg.sensor.table")
	AssetTypePowerBI                    = AssetType("powerbi")
	AssetTypePython                     = AssetType("python")
	AssetTypeQlikSense                  = AssetType("qliksense")
	AssetTypeQlikView                   = AssetType("qlikview")
	AssetTypeQuicksight                 = AssetType("quicksight")
	AssetTypeR                          = AssetType("r")
	AssetTypeRedash                     = AssetType("redash")
	AssetTypeRedshiftQuery              = AssetType("rs.sql")
	AssetTypeRedshiftQuerySensor        = AssetType("rs.sensor.query")
	AssetTypeRedshiftSeed               = AssetType("rs.seed")
	AssetTypeRedshiftSource             = AssetType("rs.source")
	AssetTypeRedshiftTableSensor        = AssetType("rs.sensor.table")
	AssetTypeS3KeySensor                = AssetType("s3.sensor.key_sensor")
	AssetTypeSisense                    = AssetType("sisense")
	AssetTypeSnowflakeQuery             = AssetType("sf.sql")
	AssetTypeSnowflakeQuerySensor       = AssetType("sf.sensor.query")
	AssetTypeSnowflakeSeed              = AssetType("sf.seed")
	AssetTypeSnowflakeSource            = AssetType("sf.source")
	AssetTypeSnowflakeTableSensor       = AssetType("sf.sensor.table")
	AssetTypeSuperset                   = AssetType("superset")
	AssetTypeSynapseQuery               = AssetType("synapse.sql")
	AssetTypeSynapseQuerySensor         = AssetType("synapse.sensor.query")
	AssetTypeSynapseSeed                = AssetType("synapse.seed")
	AssetTypeSynapseSource              = AssetType("synapse.source")
	AssetTypeSynapseTableSensor         = AssetType("synapse.sensor.table")
	AssetTypeTableau                    = AssetType("tableau")
	AssetTypeTableauDashboard           = AssetType("tableau.dashboard")
	AssetTypeTableauDatasource          = AssetType("tableau.datasource")
	AssetTypeTableauWorkbook            = AssetType("tableau.workbook")
	AssetTypeTableauWorksheet           = AssetType("tableau.worksheet")
	AssetTypeTrinoQuery                 = AssetType("trino.sql")
	AssetTypeTrinoQuerySensor           = AssetType("trino.sensor.query")
	RunConfigApplyIntervalModifiers     = RunConfig("apply-interval-modifiers")
	RunConfigEndDate                    = RunConfig("end-date")
	RunConfigFullRefresh                = RunConfig("full-refresh")
	RunConfigQueryAnnotations           = RunConfig("query-annotations")
	RunConfigRunID                      = RunConfig("run-id")
	RunConfigStartDate                  = RunConfig("start-date")
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
	"fabric_warehouse":      "fabric_warehouse-default",
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
	"emr_serverless":        "emr_serverless-default",
	"dataproc_serverless":   "dataproc_serverless-default",
	"trino":                 "trino-default",
	"oracle":                "oracle-default",
	"googleanalytics":       "googleanalytics-default",
	"applovin":              "applovin-default",
	"salesforce":            "salesforce-default",
	"solidgate":             "solidgate-default",
	"smartsheet":            "smartsheet-default",
	"sftp":                  "sftp-default",
	"motherduck":            "motherduck-default",
	"elasticsearch":         "elasticsearch-default",
}

var SupportedFileSuffixes = []string{"asset.yml", "asset.yaml", ".sql", ".py", ".r", "task.yml", "task.yaml"}

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
	Webhook []WebhookNotification `yaml:"webhook" json:"webhook" mapstructure:"webhook"`
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

type WebhookNotification struct {
	Connection         string `yaml:"connection" json:"connection" mapstructure:"connection"`
	NotificationCommon `yaml:",inline" json:",inline" mapstructure:",inline"`
}

func (n Notifications) MarshalJSON() ([]byte, error) {
	slack := make([]SlackNotification, 0, len(n.Slack))
	MSTeams := make([]MSTeamsNotification, 0, len(n.MSTeams))
	discord := make([]DiscordNotification, 0, len(n.Discord))
	webhook := make([]WebhookNotification, 0, len(n.Webhook))
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
	for _, s := range n.Webhook {
		if !reflect.ValueOf(s).IsZero() {
			webhook = append(webhook, s)
		}
	}

	return json.Marshal(struct {
		Slack   []SlackNotification   `json:"slack"`
		MSTeams []MSTeamsNotification `json:"ms_teams"`
		Discord []DiscordNotification `json:"discord"`
		Webhook []WebhookNotification `json:"webhook"`
	}{
		Slack:   slack,
		MSTeams: MSTeams,
		Discord: discord,
		Webhook: webhook,
	})
}

type (
	MaterializationType string
)

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
	MaterializationStrategyTruncateInsert   MaterializationStrategy        = "truncate+insert"
	MaterializationStrategyAppend           MaterializationStrategy        = "append"
	MaterializationStrategyMerge            MaterializationStrategy        = "merge"
	MaterializationStrategyTimeInterval     MaterializationStrategy        = "time_interval"
	MaterializationStrategyDDL              MaterializationStrategy        = "ddl"
	MaterializationTimeGranularityDate      MaterializationTimeGranularity = "date"
	MaterializationTimeGranularityTimestamp MaterializationTimeGranularity = "timestamp"
	MaterializationStrategySCD2ByTime       MaterializationStrategy        = "scd2_by_time"
	MaterializationStrategySCD2ByColumn     MaterializationStrategy        = "scd2_by_column"
)

var AllAvailableMaterializationStrategies = []MaterializationStrategy{
	MaterializationStrategyCreateReplace,
	MaterializationStrategyDeleteInsert,
	MaterializationStrategyTruncateInsert,
	MaterializationStrategyAppend,
	MaterializationStrategyMerge,
	MaterializationStrategyTimeInterval,
	MaterializationStrategyDDL,
	MaterializationStrategySCD2ByTime,
	MaterializationStrategySCD2ByColumn,
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
	ID          string           `json:"id" yaml:"-" mapstructure:"-"`
	Name        string           `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	Value       ColumnCheckValue `json:"value" yaml:"value,omitempty" mapstructure:"value"`
	Blocking    DefaultTrueBool  `json:"blocking" yaml:"blocking,omitempty" mapstructure:"blocking"`
	Description string           `json:"description" yaml:"description,omitempty" mapstructure:"description"`
}

func NewColumnCheck(assetName, columnName, name string, value ColumnCheckValue, blocking *bool, description string) ColumnCheck {
	return ColumnCheck{
		ID:          hash(fmt.Sprintf("%s-%s-%s", assetName, columnName, name)),
		Name:        strings.TrimSpace(name),
		Value:       value,
		Blocking:    DefaultTrueBool{Value: blocking},
		Description: description,
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
	Tags            EmptyStringArray  `json:"tags" yaml:"tags,omitempty" mapstructure:"tags"`
	PrimaryKey      bool              `json:"primary_key" yaml:"primary_key,omitempty" mapstructure:"primary_key"`
	UpdateOnMerge   bool              `json:"update_on_merge" yaml:"update_on_merge,omitempty" mapstructure:"update_on_merge"`
	MergeSQL        string            `json:"merge_sql" yaml:"merge_sql,omitempty" mapstructure:"merge_sql"`
	Nullable        DefaultTrueBool   `json:"nullable" yaml:"nullable,omitempty" mapstructure:"nullable"`
	Owner           string            `json:"owner" yaml:"owner,omitempty" mapstructure:"owner"`
	Domains         EmptyStringArray  `json:"domains" yaml:"domains,omitempty" mapstructure:"domains"`
	Meta            EmptyStringMap    `json:"meta" yaml:"meta,omitempty" mapstructure:"meta"`
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
	AssetTypeBigqueryQuery:       "google_cloud_platform",
	AssetTypeBigqueryTableSensor: "google_cloud_platform",
	AssetTypeBigquerySeed:        "google_cloud_platform",
	AssetTypeBigquerySource:      "google_cloud_platform",
	AssetTypeBigqueryQuerySensor: "google_cloud_platform",

	AssetTypeSnowflakeQuery:             "snowflake",
	AssetTypeSnowflakeQuerySensor:       "snowflake",
	AssetTypeSnowflakeTableSensor:       "snowflake",
	AssetTypeSnowflakeSeed:              "snowflake",
	AssetTypeSnowflakeSource:            "snowflake",
	AssetTypePostgresQuery:              "postgres",
	AssetTypePostgresSeed:               "postgres",
	AssetTypePostgresQuerySensor:        "postgres",
	AssetTypePostgresTableSensor:        "postgres",
	AssetTypePostgresSource:             "postgres",
	AssetTypeMySQLQuery:                 "mysql",
	AssetTypeMySQLSeed:                  "mysql",
	AssetTypeMySQLQuerySensor:           "mysql",
	AssetTypeMySQLTableSensor:           "mysql",
	AssetTypeRedshiftQuery:              "redshift",
	AssetTypeRedshiftSeed:               "redshift",
	AssetTypeRedshiftQuerySensor:        "redshift",
	AssetTypeRedshiftSource:             "redshift",
	AssetTypeRedshiftTableSensor:        "redshift",
	AssetTypeMsSQLQuery:                 "mssql",
	AssetTypeMsSQLSeed:                  "mssql",
	AssetTypeMsSQLQuerySensor:           "mssql",
	AssetTypeMsSQLTableSensor:           "mssql",
	AssetTypeMsSQLSource:                "mssql",
	AssetTypeFabricWarehouseQuery:       "fabric_warehouse",
	AssetTypeFabricWarehouseSeed:        "fabric_warehouse",
	AssetTypeFabricWarehouseQuerySensor: "fabric_warehouse",
	AssetTypeFabricWarehouseTableSensor: "fabric_warehouse",
	AssetTypeDatabricksQuery:            "databricks",
	AssetTypeDatabricksSeed:             "databricks",
	AssetTypeDatabricksQuerySensor:      "databricks",
	AssetTypeDatabricksSource:           "databricks",
	AssetTypeDatabricksTableSensor:      "databricks",
	AssetTypeSynapseQuery:               "synapse",
	AssetTypeSynapseSeed:                "synapse",
	AssetTypeSynapseQuerySensor:         "synapse",
	AssetTypeSynapseSource:              "synapse",
	AssetTypeAthenaQuery:                "athena",
	AssetTypeAthenaSeed:                 "athena",
	AssetTypeAthenaSQLSensor:            "athena",
	AssetTypeAthenaTableSensor:          "athena",
	AssetTypeAthenaSource:               "athena",
	AssetTypeDuckDBQuery:                "duckdb",
	AssetTypeDuckDBSeed:                 "duckdb",
	AssetTypeDuckDBQuerySensor:          "duckdb",
	AssetTypeDuckDBSource:               "duckdb",
	AssetTypeMotherduckQuery:            "motherduck",
	AssetTypeClickHouse:                 "clickhouse",
	AssetTypeClickHouseSeed:             "clickhouse",
	AssetTypeClickHouseQuerySensor:      "clickhouse",
	AssetTypeClickHouseTableSensor:      "clickhouse",
	AssetTypeClickHouseSource:           "clickhouse",
	AssetTypeEMRServerlessSpark:         "emr_serverless",
	AssetTypeEMRServerlessPyspark:       "emr_serverless",
	AssetTypeDataprocServerlessPyspark:  "dataproc_serverless",
	AssetTypeTrinoQuery:                 "trino",
	AssetTypeTrinoQuerySensor:           "trino",
	AssetTypeOracleQuery:                "oracle",
	AssetTypeOracleSource:               "oracle",
	AssetTypeS3KeySensor:                "aws",
	AssetTypeElasticsearch:              "elasticsearch",
}

var IngestrTypeConnectionMapping = map[string]AssetType{
	"athena":        AssetTypeAthenaQuery,
	"bigquery":      AssetTypeBigqueryQuery,
	"snowflake":     AssetTypeSnowflakeQuery,
	"postgres":      AssetTypePostgresQuery,
	"redshift":      AssetTypeRedshiftQuery,
	"mssql":         AssetTypeMsSQLQuery,
	"databricks":    AssetTypeDatabricksQuery,
	"synapse":       AssetTypeSynapseQuery,
	"duckdb":        AssetTypeDuckDBQuery,
	"clickhouse":    AssetTypeClickHouse,
	"oracle":        AssetTypeOracleQuery,
	"motherduck":    AssetTypeMotherduckQuery,
	"elasticsearch": AssetTypeElasticsearch,
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
	Count       *int64          `json:"count,omitempty" yaml:"count,omitempty" mapstructure:"count"`
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
	isAsset := u.Type == "" || u.Type == "asset"
	if u.Mode == UpstreamModeFull && isAsset {
		return u.Value, nil
	}

	val := map[string]any{}
	id := "uri"
	if isAsset {
		id = "asset"
	}
	val[id] = u.Value

	if u.Mode == UpstreamModeSymbolic {
		val["mode"] = u.Mode.String()
	}

	return val, nil
}

type SnowflakeConfig struct {
	Warehouse string `json:"warehouse"  yaml:"warehouse" `
}

func (s SnowflakeConfig) MarshalJSON() ([]byte, error) {
	if s.Warehouse == "" {
		return []byte("null"), nil
	}

	type Alias SnowflakeConfig
	return json.Marshal(Alias(s))
}

type AthenaConfig struct {
	Location string `json:"location"`
}

func (s AthenaConfig) MarshalJSON() ([]byte, error) {
	if s.Location == "" {
		return []byte("null"), nil
	}

	type Alias AthenaConfig
	return json.Marshal(Alias(s))
}

type Asset struct { //nolint:recvcheck
	ID                string             `json:"id" yaml:"-" mapstructure:"-"`
	URI               string             `json:"uri" yaml:"uri,omitempty" mapstructure:"uri"`
	Name              string             `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	Type              AssetType          `json:"type" yaml:"type,omitempty" mapstructure:"type"`
	Description       string             `json:"description" yaml:"description,omitempty" mapstructure:"description"`
	StartDate         string             `json:"start_date" yaml:"start_date,omitempty" mapstructure:"start_date"`
	Connection        string             `json:"connection" yaml:"connection,omitempty" mapstructure:"connection"`
	Tags              EmptyStringArray   `json:"tags" yaml:"tags,omitempty" mapstructure:"tags"`
	Domains           EmptyStringArray   `json:"domains" yaml:"domains,omitempty" mapstructure:"domains"`
	Meta              EmptyStringMap     `json:"meta" yaml:"meta,omitempty" mapstructure:"meta"`
	Materialization   Materialization    `json:"materialization" yaml:"materialization,omitempty" mapstructure:"materialization"`
	Upstreams         []Upstream         `json:"upstreams" yaml:"depends,omitempty" mapstructure:"depends"`
	Image             string             `json:"image" yaml:"image,omitempty" mapstructure:"image"`
	Instance          string             `json:"instance" yaml:"instance,omitempty" mapstructure:"instance"`
	Owner             string             `json:"owner" yaml:"owner,omitempty" mapstructure:"owner"`
	Tier              int                `json:"tier,omitempty" yaml:"tier,omitempty" mapstructure:"tier"`
	ExecutableFile    ExecutableFile     `json:"executable_file" yaml:"-" mapstructure:"-"`
	DefinitionFile    TaskDefinitionFile `json:"definition_file" yaml:"-" mapstructure:"-"`
	Parameters        EmptyStringMap     `json:"parameters" yaml:"parameters,omitempty" mapstructure:"parameters"`
	Secrets           []SecretMapping    `json:"secrets" yaml:"secrets,omitempty" mapstructure:"secrets"`
	Extends           []string           `json:"extends" yaml:"extends,omitempty" mapstructure:"extends"`
	Columns           []Column           `json:"columns" yaml:"columns,omitempty" mapstructure:"columns"`
	CustomChecks      []CustomCheck      `json:"custom_checks" yaml:"custom_checks,omitempty" mapstructure:"custom_checks"`
	Hooks             Hooks              `json:"hooks,omitempty" yaml:"hooks,omitempty" mapstructure:"hooks"`
	Metadata          EmptyStringMap     `json:"metadata" yaml:"metadata,omitempty" mapstructure:"metadata"`
	Snowflake         SnowflakeConfig    `json:"snowflake" yaml:"snowflake,omitempty" mapstructure:"snowflake"`
	Athena            AthenaConfig       `json:"athena" yaml:"athena,omitempty" mapstructure:"athena"`
	IntervalModifiers IntervalModifiers  `json:"interval_modifiers" yaml:"interval_modifiers,omitempty" mapstructure:"interval_modifiers"`
	RerunCooldown     *int               `json:"rerun_cooldown,omitempty" yaml:"rerun_cooldown,omitempty" mapstructure:"rerun_cooldown"`
	RetriesDelay      *int               `json:"retries_delay,omitempty" yaml:"-" mapstructure:"-"`
	RefreshRestricted *bool              `json:"refresh_restricted,omitempty" yaml:"refresh_restricted,omitempty" mapstructure:"refresh_restricted"`

	upstream   []*Asset
	downstream []*Asset
}

type Hook struct {
	Query string `json:"query" yaml:"query" mapstructure:"query"`
}

type Hooks struct {
	Pre  []Hook `json:"pre,omitempty" yaml:"pre,omitempty" mapstructure:"pre"`
	Post []Hook `json:"post,omitempty" yaml:"post,omitempty" mapstructure:"post"`
}

func (h Hooks) IsZero() bool {
	return len(h.Pre) == 0 && len(h.Post) == 0
}

//nolint:recvcheck
type TimeModifier struct {
	Months       int    `json:"months" yaml:"months,omitempty" mapstructure:"months"`
	Days         int    `json:"days" yaml:"days,omitempty" mapstructure:"days"`
	Hours        int    `json:"hours" yaml:"hours,omitempty" mapstructure:"hours"`
	Minutes      int    `json:"minutes" yaml:"minutes,omitempty" mapstructure:"minutes"`
	Seconds      int    `json:"seconds" yaml:"seconds,omitempty" mapstructure:"seconds"`
	Milliseconds int    `json:"milliseconds" yaml:"milliseconds,omitempty" mapstructure:"milliseconds"`
	Nanoseconds  int    `json:"nanoseconds" yaml:"nanoseconds,omitempty" mapstructure:"nanoseconds"`
	CronPeriods  int    `json:"cron_periods" yaml:"cron_periods,omitempty" mapstructure:"cron_periods"`
	Template     string `json:"template" yaml:"template,omitempty" mapstructure:"template"`
}

func (t *TimeModifier) UnmarshalYAML(value *yaml.Node) error {
	if value.Kind != yaml.ScalarNode {
		return fmt.Errorf("expected scalar node, got %v", value.Kind)
	}

	modifier := value.Value
	if len(modifier) < 2 {
		return fmt.Errorf("invalid time modifier format: %s", modifier)
	}

	// Check if it's a Jinja template
	if strings.Contains(modifier, "{{") || strings.Contains(modifier, "{%") {
		t.Template = modifier
		return nil
	}

	return t.parseTimeModifier(modifier)
}

func (t *TimeModifier) parseTimeModifier(modifier string) error {
	// Check for two-character suffixes first (ms, ns)
	if len(modifier) >= 3 {
		twoCharSuffix := modifier[len(modifier)-2:]
		if twoCharSuffix == "ms" || twoCharSuffix == "ns" {
			numeric := modifier[:len(modifier)-2]
			num, err := strconv.Atoi(numeric)
			if err != nil {
				return fmt.Errorf("invalid numeric portion %q in interval %q: %w", numeric, modifier, err)
			}

			switch twoCharSuffix {
			case "ms":
				t.Milliseconds = num
			case "ns":
				t.Nanoseconds = num
			}

			return nil
		}
	}

	suffix := modifier[len(modifier)-1]
	numeric := modifier[:len(modifier)-1]

	num, err := strconv.Atoi(numeric)
	if err != nil {
		return fmt.Errorf("invalid numeric portion %q in interval %q: %w", numeric, modifier, err)
	}

	switch suffix {
	case 'h':
		t.Hours = num
	case 'm':
		t.Minutes = num
	case 's':
		t.Seconds = num
	case 'd':
		t.Days = num
	case 'M':
		t.Months = num
	default:
		return fmt.Errorf("unknown interval suffix %q in %q; must be h, m, s, d, M, ms, or ns", string(suffix), modifier)
	}

	return nil
}

// RendererInterface is used to avoid circular dependencies.
type RendererInterface interface {
	Render(template string) (string, error)
}

func (t TimeModifier) ResolveTemplateToNew(renderer RendererInterface) (TimeModifier, error) {
	if t.Template == "" {
		return t, nil
	}

	rendered, err := renderer.Render(t.Template)
	if err != nil {
		return t, fmt.Errorf("failed to render interval modifier template: %w", err)
	}

	newModifier := TimeModifier{}
	rendered = strings.TrimSpace(rendered)
	if err := newModifier.parseTimeModifier(rendered); err != nil {
		return t, fmt.Errorf("failed to parse rendered template result '%s': %w", rendered, err)
	}

	return newModifier, nil
}

func (t TimeModifier) MarshalJSON() ([]byte, error) {
	if t.Months == 0 && t.Days == 0 && t.Hours == 0 && t.Minutes == 0 && t.Seconds == 0 && t.Milliseconds == 0 && t.Nanoseconds == 0 && t.CronPeriods == 0 && t.Template == "" {
		return []byte("null"), nil
	}

	type Alias TimeModifier
	return json.Marshal(Alias(t))
}

type IntervalModifiers struct {
	Start TimeModifier `json:"start" yaml:"start,omitempty" mapstructure:"start"`
	End   TimeModifier `json:"end" yaml:"end,omitempty" mapstructure:"end"`
}

func (im IntervalModifiers) MarshalJSON() ([]byte, error) {
	if (im.Start == TimeModifier{} && im.End == TimeModifier{}) {
		return []byte("null"), nil
	}

	type Alias IntervalModifiers
	return json.Marshal(Alias(im))
}

func (a *Asset) AddUpstream(asset *Asset) {
	a.upstream = append(a.upstream, asset)

	for _, u := range a.Upstreams {
		if strings.EqualFold(u.Value, asset.Name) {
			return
		}
	}

	a.Upstreams = append(a.Upstreams, Upstream{
		Type:  "asset",
		Value: asset.Name,
		Mode:  UpstreamModeFull,
	})
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

func (a *Asset) PrefixUpstreams(prefix string) {
	if prefix == "" {
		return
	}

	for i, u := range a.Upstreams {
		if u.Type != "asset" {
			continue
		}

		nameParts := strings.Split(u.Value, ".")
		if len(nameParts) == 2 {
			a.Upstreams[i].Value = prefix + nameParts[0] + "." + nameParts[1]
		}
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
	for i := range a.Hooks.Pre {
		a.Hooks.Pre[i].Query = ClearSpacesAtLineEndings(a.Hooks.Pre[i].Query)
	}
	for i := range a.Hooks.Post {
		a.Hooks.Post[i].Query = ClearSpacesAtLineEndings(a.Hooks.Post[i].Query)
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

func (a *Asset) GetNameIfItWasSetFromItsPath(foundPipeline *Pipeline) (string, error) {
	var err error
	var baseFolder string
	relativePath := a.DefinitionFile.Path
	if filepath.IsAbs(a.DefinitionFile.Path) {
		if foundPipeline != nil {
			pipelinePath := foundPipeline.DefinitionFile.Path
			baseFolder = filepath.Join(filepath.Dir(pipelinePath), "assets")
		} else {
			pipelinePath, err := path.GetPipelineRootFromTask(a.DefinitionFile.Path, []string{"pipeline.yml", "pipeline.yaml"})
			if err != nil {
				return "", err
			}
			baseFolder = filepath.Join(pipelinePath, "assets")
		}

		relativePath, err = filepath.Rel(baseFolder, a.DefinitionFile.Path)
		if err != nil {
			return "", err
		}
	}

	parts := strings.Split(relativePath, string(filepath.Separator))
	for i, part := range parts {
		if part == "assets" {
			parts = parts[i+1:]
			break
		}
	}

	name := strings.Join(parts, ".")

	switch {
	case strings.HasSuffix(name, ".asset.yml"):
		name = strings.TrimSuffix(name, ".asset.yml")
	case strings.HasSuffix(name, ".asset.yaml"):
		name = strings.TrimSuffix(name, ".asset.yaml")
	default:
		name = strings.TrimSuffix(name, filepath.Ext(name))
	}

	return name, nil
}

func (a Asset) Persist(fs afero.Fs, pipeline ...*Pipeline) error {
	// Save original values
	originalParams := a.Parameters

	// Remove parameters that match pipeline defaults
	// pipeline is optional - if provided and has defaults, filter them out
	if len(pipeline) > 0 && pipeline[0] != nil && pipeline[0].DefaultValues != nil && len(pipeline[0].DefaultValues.Parameters) > 0 {
		filteredParams := EmptyStringMap{}
		for key, value := range a.Parameters {
			// Only keep parameters that are NOT in defaults or have different values
			if defaultValue, existsInDefaults := pipeline[0].DefaultValues.Parameters[key]; !existsInDefaults || defaultValue != value {
				filteredParams[key] = value
			}
		}

		// If no parameters remain, set to nil instead of empty map
		if len(filteredParams) == 0 {
			a.Parameters = nil
		} else {
			a.Parameters = filteredParams
		}
	}

	// Reuse the logic from PersistWithoutWriting
	content, err := a.FormatContent()

	// Restore original values (even if formatting failed)
	a.Parameters = originalParams

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

	keysToAddSpace := []string{"custom_checks", "depends", "columns", "materialization", "secrets", "parameters", "hooks"}
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

func (p *Pipeline) Persist(fs afero.Fs) error {
	// Generate the YAML content for the pipeline
	content, err := p.FormatContent()
	if err != nil {
		return errors.Wrap(err, "failed to generate content for persistence")
	}

	// Write the generated content to the file
	filePath := p.DefinitionFile.Path
	return afero.WriteFile(fs, filePath, content, 0o644)
}

func (p *Pipeline) FormatContent() ([]byte, error) {
	if p == nil {
		return nil, errors.New("failed to build a pipeline, therefore cannot persist it")
	}

	buf := bytes.NewBuffer(nil)
	enc := yaml.NewEncoder(buf)
	enc.SetIndent(2)

	err := enc.Encode(p)
	if err != nil {
		return nil, err
	}

	yamlConfig := buf.Bytes()

	keysToAddSpace := []string{"assets", "notifications", "default_connections", "variables"}
	for _, key := range keysToAddSpace {
		yamlConfig = bytes.ReplaceAll(yamlConfig, []byte("\n"+key+":"), []byte("\n\n"+key+":"))
	}

	stringVersion := string(yamlConfig)
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
		*a = nil
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

	pipelineDir := filepath.Dir(filePath)
	pl.MacrosPath = filepath.Join(pipelineDir, "macros")

	pl.Macros, err = LoadMacrosFromPath(pl.MacrosPath, fs)
	if err != nil {
		return nil, errors.Wrap(err, "error parsing macros")
	}

	return &pl, nil
}

func LoadMacrosFromPath(macrosPath string, fs afero.Fs) ([]Macro, error) {
	// Check if the path exists and is a directory
	info, err := fs.Stat(macrosPath)
	if err != nil || !info.IsDir() {
		return []Macro{}, nil //nolint:nilerr
	}

	macros := []Macro{}
	files, err := path.GetAllFilesRecursive(macrosPath, []string{".sql"})
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read macros folder file at '%s'", macrosPath)
	}

	for _, file := range files {
		content, err := afero.ReadFile(fs, file)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to read macro file at '%s'", file)
		}
		macros = append(macros, Macro(string(content)))
	}

	return macros, nil
}

type MetadataPush struct {
	Global   bool `json:"-"`
	BigQuery bool `json:"bigquery" yaml:"bigquery" mapstructure:"bigquery"`
}

func (mp *MetadataPush) HasAnyEnabled() bool {
	return mp.BigQuery || mp.Global
}

type Macro string

type Pipeline struct {
	LegacyID           string                 `json:"legacy_id" yaml:"id,omitempty" mapstructure:"id"`
	Name               string                 `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	Tags               EmptyStringArray       `json:"tags" yaml:"tags,omitempty" mapstructure:"tags"`
	Domains            EmptyStringArray       `json:"domains" yaml:"domains,omitempty" mapstructure:"domains"`
	Meta               EmptyStringMap         `json:"meta" yaml:"meta,omitempty" mapstructure:"meta"`
	Schedule           Schedule               `json:"schedule" yaml:"schedule,omitempty" mapstructure:"schedule"`
	StartDate          string                 `json:"start_date" yaml:"start_date,omitempty" mapstructure:"start_date"`
	DefinitionFile     DefinitionFile         `json:"definition_file" yaml:"-"`
	DefaultConnections EmptyStringMap         `json:"default_connections" yaml:"default_connections,omitempty" mapstructure:"default_connections"`
	Assets             []*Asset               `json:"assets" yaml:"assets,omitempty"`
	Notifications      Notifications          `json:"notifications" yaml:"notifications,omitempty" mapstructure:"notifications"`
	Catchup            bool                   `json:"catchup" yaml:"catchup,omitempty" mapstructure:"catchup"`
	MetadataPush       MetadataPush           `json:"metadata_push" yaml:"metadata_push,omitempty" mapstructure:"metadata_push"`
	Retries            int                    `json:"retries" yaml:"retries,omitempty" mapstructure:"retries"`
	RetriesDelay       *int                   `json:"retries_delay,omitempty" yaml:"-" mapstructure:"-"`
	Concurrency        int                    `json:"concurrency" yaml:"concurrency,omitempty" mapstructure:"concurrency"`
	DefaultValues      *DefaultValues         `json:"default,omitempty" yaml:"default,omitempty" mapstructure:"default,omitempty"`
	Commit             string                 `json:"commit" yaml:"commit,omitempty"`
	Snapshot           string                 `json:"snapshot" yaml:"snapshot,omitempty"`
	Agent              bool                   `json:"agent" yaml:"agent,omitempty" mapstructure:"agent"`
	Variables          Variables              `json:"variables" yaml:"variables,omitempty" mapstructure:"variables"`
	TasksByType        map[AssetType][]*Asset `json:"-" yaml:"-"`
	tasksByName        map[string]*Asset      `yaml:"-"`
	MacrosPath         string                 `json:"-" yaml:"-"`
	Macros             []Macro                `json:"macros" yaml:"macros,omitempty" mapstructure:"macros"`
}

func (p *Pipeline) UnmarshalYAML(unmarshal func(interface{}) error) error {
	type pipelineAlias Pipeline
	aux := (*pipelineAlias)(p)

	if err := unmarshal(aux); err != nil {
		return err
	}

	if p.Concurrency == 0 {
		p.Concurrency = 1
	}

	return nil
}

func (p *Pipeline) UnmarshalJSON(data []byte) error {
	type pipelineAlias Pipeline
	aux := (*pipelineAlias)(p)

	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}

	if p.Concurrency == 0 {
		p.Concurrency = 1
	}

	return nil
}

type DefaultValues struct {
	Type              string            `json:"type" yaml:"type" mapstructure:"type"`
	Parameters        map[string]string `json:"parameters" yaml:"parameters" mapstructure:"parameters"`
	Secrets           []secretMapping   `json:"secrets" yaml:"secrets" mapstructure:"secrets"`
	Hooks             Hooks             `json:"hooks" yaml:"hooks" mapstructure:"hooks"`
	IntervalModifiers IntervalModifiers `json:"interval_modifiers" yaml:"interval_modifiers" mapstructure:"interval_modifiers"`
	RerunCooldown     *int              `json:"rerun_cooldown,omitempty" yaml:"rerun_cooldown,omitempty" mapstructure:"rerun_cooldown"`
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
		AssetTypeBigqueryQuery:        0,
		AssetTypeSnowflakeQuery:       0,
		AssetTypePostgresQuery:        0,
		AssetTypeMsSQLQuery:           0,
		AssetTypeDatabricksQuery:      0,
		AssetTypeRedshiftQuery:        0,
		AssetTypeSynapseQuery:         0,
		AssetTypeFabricWarehouseQuery: 0,
		AssetTypeAthenaQuery:          0,
		AssetTypeDuckDBQuery:          0,
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

type AssetMutator func(ctx context.Context, asset *Asset, foundPipeline *Pipeline) (*Asset, error)

type PipelineMutator func(ctx context.Context, pipeline *Pipeline) (*Pipeline, error)

type Builder struct {
	config             BuilderConfig
	yamlTaskCreator    TaskCreator
	commentTaskCreator TaskCreator
	fs                 afero.Fs
	assetMutators      []AssetMutator
	pipelineMutators   []PipelineMutator

	GlossaryReader glossaryReader
}

func (b *Builder) SetGlossaryReader(reader glossaryReader) {
	b.GlossaryReader = reader
}

func (b *Builder) AddAssetMutator(m AssetMutator) {
	b.assetMutators = append(b.assetMutators, m)
}

func (b *Builder) AddPipelineMutator(m PipelineMutator) {
	b.pipelineMutators = append(b.pipelineMutators, m)
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

	b.assetMutators = []AssetMutator{
		b.fillGlossaryStuff,
		b.SetupDefaultsFromPipeline,
		b.translateRetryConfig,
		b.SetNameFromPath,
	}

	b.pipelineMutators = []PipelineMutator{
		b.translatePipelineRetryConfig,
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
	onlyPipeline     bool
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

func WithOnlyPipeline() CreatePipelineOption {
	return func(o *createPipelineConfig) {
		o.onlyPipeline = true
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

	if config.isMutate {
		pipeline, err = b.MutatePipeline(ctx, pipeline)
		if err != nil {
			return nil, err
		}
	}

	if config.onlyPipeline {
		return pipeline, nil
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

		if task == nil {
			continue
		}

		if config.isMutate {
			task, err = b.MutateAsset(ctx, task, pipeline)
			if err != nil {
				return nil, err
			}
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
			if upstream.Mode != UpstreamModeFull && upstream.Mode != UpstreamModeSymbolic {
				upstream.Mode = UpstreamModeFull
			}

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

	// if the definition comes from an R file the asset is always an R asset
	if strings.HasSuffix(task.ExecutableFile.Path, ".r") && task.Type == "" {
		task.Type = AssetTypeR
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
	for _, mutator := range b.assetMutators {
		var err error
		task, err = mutator(ctx, task, foundPipeline)
		if err != nil {
			return nil, err
		}
	}

	return task, nil
}

func (b *Builder) MutatePipeline(ctx context.Context, pipeline *Pipeline) (*Pipeline, error) {
	for _, mutator := range b.pipelineMutators {
		var err error
		pipeline, err = mutator(ctx, pipeline)
		if err != nil {
			return nil, err
		}
	}

	return pipeline, nil
}

func (b *Builder) translatePipelineRetryConfig(ctx context.Context, pipeline *Pipeline) (*Pipeline, error) {
	if pipeline == nil {
		return nil, nil
	}

	// Translate pipeline default rerun_cooldown to retries_delay
	if pipeline.DefaultValues != nil && pipeline.DefaultValues.RerunCooldown != nil {
		if *pipeline.DefaultValues.RerunCooldown > 0 {
			pipeline.RetriesDelay = pipeline.DefaultValues.RerunCooldown
		} else if *pipeline.DefaultValues.RerunCooldown == -1 {
			zero := 0
			pipeline.RetriesDelay = &zero
		}
	}

	return pipeline, nil
}

func (b *Builder) SetupDefaultsFromPipeline(ctx context.Context, asset *Asset, foundPipeline *Pipeline) (*Asset, error) {
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
	if (asset.IntervalModifiers.Start == TimeModifier{}) {
		asset.IntervalModifiers.Start = foundPipeline.DefaultValues.IntervalModifiers.Start
	}
	if (asset.IntervalModifiers.End == TimeModifier{}) {
		asset.IntervalModifiers.End = foundPipeline.DefaultValues.IntervalModifiers.End
	}
	if len(asset.Hooks.Pre) == 0 {
		asset.Hooks.Pre = append([]Hook(nil), foundPipeline.DefaultValues.Hooks.Pre...)
	}
	if len(asset.Hooks.Post) == 0 {
		asset.Hooks.Post = append([]Hook(nil), foundPipeline.DefaultValues.Hooks.Post...)
	}

	return asset, nil
}

func (b *Builder) translateRetryConfig(ctx context.Context, asset *Asset, foundPipeline *Pipeline) (*Asset, error) {
	if asset == nil {
		return asset, nil
	}

	// Translate asset-level rerun_cooldown to retries_delay
	if asset.RerunCooldown != nil {
		if *asset.RerunCooldown > 0 {
			asset.RetriesDelay = asset.RerunCooldown
		} else if *asset.RerunCooldown == -1 {
			zero := 0
			asset.RetriesDelay = &zero
		}
	} else if foundPipeline != nil && foundPipeline.DefaultValues != nil && foundPipeline.DefaultValues.RerunCooldown != nil && *foundPipeline.DefaultValues.RerunCooldown > 0 {
		// Inherit from pipeline default if asset has no specific retry config
		asset.RetriesDelay = foundPipeline.DefaultValues.RerunCooldown
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
		AssetTypeBigqueryQuery:        true,
		AssetTypeSnowflakeQuery:       true,
		AssetTypePostgresQuery:        true,
		AssetTypeRedshiftQuery:        true,
		AssetTypeMsSQLQuery:           true,
		AssetTypeDatabricksQuery:      true,
		AssetTypeSynapseQuery:         true,
		AssetTypeFabricWarehouseQuery: true,
		AssetTypeAthenaQuery:          true,
		AssetTypeDuckDBQuery:          true,
		AssetTypeClickHouse:           true,
		AssetTypeTrinoQuery:           true,
		AssetTypeOracleQuery:          true,
	}

	return sqlAssetTypes[a.Type]
}

func (b *Builder) SetAssetColumnFromGlossary(asset *Asset, pathToPipeline string) error {
	var entities []*glossary.Entity
	var err error
	if b.GlossaryReader != nil {
		entities, err = b.GlossaryReader.GetEntities(pathToPipeline)
		if err != nil {
			return errors.Wrap(err, "error getting entities")
		}
	}

	if len(entities) > 0 {
		err := asset.EnrichFromEntityAttributes(entities)
		if err != nil {
			return errors.Wrap(err, "error enriching asset from entity attributes")
		}
	}
	return nil
}

func ModifyDate(t time.Time, modifier TimeModifier) time.Time {
	t = t.AddDate(0, modifier.Months, modifier.Days).
		Add(time.Duration(modifier.Hours) * time.Hour).
		Add(time.Duration(modifier.Minutes) * time.Minute).
		Add(time.Duration(modifier.Seconds) * time.Second).
		Add(time.Duration(modifier.Milliseconds) * time.Millisecond).
		Add(time.Duration(modifier.Nanoseconds) * time.Nanosecond)

	return t
}

func (b *Builder) SetNameFromPath(ctx context.Context, asset *Asset, foundPipeline *Pipeline) (*Asset, error) {
	if foundPipeline == nil {
		return asset, nil
	}
	if asset == nil {
		return asset, nil
	}

	if asset.Name != "" {
		return asset, nil
	}

	name, err := asset.GetNameIfItWasSetFromItsPath(foundPipeline)
	if err != nil {
		return asset, errors.Wrap(err, "error getting asset name")
	}

	asset.Name = name
	asset.ID = hash(name)
	return asset, nil
}

func (t TimeModifier) MarshalYAML() (interface{}, error) {
	if t.Template != "" {
		return t.Template, nil
	}

	// Helper to check if only one field is set (excluding the specified one)
	allZeroExcept := func(exceptDays, exceptMonths, exceptHours, exceptMinutes, exceptSeconds, exceptMilliseconds, exceptNanoseconds bool) bool {
		return (exceptDays || t.Days == 0) &&
			(exceptMonths || t.Months == 0) &&
			(exceptHours || t.Hours == 0) &&
			(exceptMinutes || t.Minutes == 0) &&
			(exceptSeconds || t.Seconds == 0) &&
			(exceptMilliseconds || t.Milliseconds == 0) &&
			(exceptNanoseconds || t.Nanoseconds == 0) &&
			t.CronPeriods == 0
	}

	switch {
	case t.Days != 0 && allZeroExcept(true, false, false, false, false, false, false):
		return fmt.Sprintf("%dd", t.Days), nil
	case t.Months != 0 && allZeroExcept(false, true, false, false, false, false, false):
		return fmt.Sprintf("%dM", t.Months), nil
	case t.Hours != 0 && allZeroExcept(false, false, true, false, false, false, false):
		return fmt.Sprintf("%dh", t.Hours), nil
	case t.Minutes != 0 && allZeroExcept(false, false, false, true, false, false, false):
		return fmt.Sprintf("%dm", t.Minutes), nil
	case t.Seconds != 0 && allZeroExcept(false, false, false, false, true, false, false):
		return fmt.Sprintf("%ds", t.Seconds), nil
	case t.Milliseconds != 0 && allZeroExcept(false, false, false, false, false, true, false):
		return fmt.Sprintf("%dms", t.Milliseconds), nil
	case t.Nanoseconds != 0 && allZeroExcept(false, false, false, false, false, false, true):
		return fmt.Sprintf("%dns", t.Nanoseconds), nil
	default:
		// fallback to struct/object form
		type Alias TimeModifier
		return Alias(t), nil
	}
}
