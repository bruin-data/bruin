package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	AssetTypeAgentClaudeCode           = AssetType("agent.claude_code")
	AssetTypeAthenaQuery               = AssetType("athena.sql")
	AssetTypeAthenaSeed                = AssetType("athena.seed")
	AssetTypeAthenaSource              = AssetType("athena.source")
	AssetTypeAthenaSQLSensor           = AssetType("athena.sensor.query")
	AssetTypeAthenaTableSensor         = AssetType("athena.sensor.table")
	AssetTypeBigqueryQuery             = AssetType("bq.sql")
	AssetTypeBigqueryQuerySensor       = AssetType("bq.sensor.query")
	AssetTypeBigquerySeed              = AssetType("bq.seed")
	AssetTypeBigquerySource            = AssetType("bq.source")
	AssetTypeBigqueryTableSensor       = AssetType("bq.sensor.table")
	AssetTypeClickHouse                = AssetType("clickhouse.sql")
	AssetTypeClickHouseQuerySensor     = AssetType("clickhouse.sensor.query")
	AssetTypeClickHouseSeed            = AssetType("clickhouse.seed")
	AssetTypeClickHouseSource          = AssetType("clickhouse.source")
	AssetTypeClickHouseTableSensor     = AssetType("clickhouse.sensor.table")
	AssetTypeDatabricksQuery           = AssetType("databricks.sql")
	AssetTypeDatabricksQuerySensor     = AssetType("databricks.sensor.query")
	AssetTypeDatabricksSeed            = AssetType("databricks.seed")
	AssetTypeDatabricksSource          = AssetType("databricks.source")
	AssetTypeDatabricksTableSensor     = AssetType("databricks.sensor.table")
	AssetTypeDataprocServerlessPyspark = AssetType("dataproc_serverless.pyspark")
	AssetTypeDomo                      = AssetType("domo")
	AssetTypeDuckDBQuery               = AssetType("duckdb.sql")
	AssetTypeDuckDBQuerySensor         = AssetType("duckdb.sensor.query")
	AssetTypeDuckDBSeed                = AssetType("duckdb.seed")
	AssetTypeDuckDBSource              = AssetType("duckdb.source")
	AssetTypeDynamoDB                  = AssetType("dynamodb")
	AssetTypeElasticsearch             = AssetType("elasticsearch")
	AssetTypeEmpty                     = AssetType("empty")
	AssetTypeEMRServerlessPyspark      = AssetType("emr_serverless.pyspark")
	AssetTypeEMRServerlessSpark        = AssetType("emr_serverless.spark")
	AssetTypeGoodData                  = AssetType("gooddata")
	AssetTypeGoogleSheets              = AssetType("gsheets")
	AssetTypeGrafana                   = AssetType("grafana")
	AssetTypeIngestr                   = AssetType("ingestr")
	AssetTypeLooker                    = AssetType("looker")
	AssetTypeLookerStudio              = AssetType("looker_studio")
	AssetTypeMetabase                  = AssetType("metabase")
	AssetTypeModeBI                    = AssetType("modebi")
	AssetTypeMongoSource               = AssetType("mongo.source")
	AssetTypeMotherduckQuery           = AssetType("motherduck.sql")
	AssetTypeMsSQLQuery                = AssetType("ms.sql")
	AssetTypeMsSQLQuerySensor          = AssetType("ms.sensor.query")
	AssetTypeMsSQLSeed                 = AssetType("ms.seed")
	AssetTypeMsSQLSource               = AssetType("ms.source")
	AssetTypeMsSQLTableSensor          = AssetType("ms.sensor.table")
	AssetTypeFabricQuery               = AssetType("fabric.sql")
	AssetTypeFabricQuerySensor         = AssetType("fabric.sensor.query")
	AssetTypeFabricSeed                = AssetType("fabric.seed")
	AssetTypeFabricTableSensor         = AssetType("fabric.sensor.table")
	AssetTypeFabricQueryLegacy         = AssetType("fw.sql")
	AssetTypeFabricQuerySensorLegacy   = AssetType("fw.sensor.query")
	AssetTypeFabricSeedLegacy          = AssetType("fw.seed")
	AssetTypeFabricTableSensorLegacy   = AssetType("fw.sensor.table")
	AssetTypeMySQLQuery                = AssetType("my.sql")
	AssetTypeMySQLQuerySensor          = AssetType("my.sensor.query")
	AssetTypeMySQLSeed                 = AssetType("my.seed")
	AssetTypeMySQLTableSensor          = AssetType("my.sensor.table")
	AssetTypeOracleQuery               = AssetType("oracle.sql")
	AssetTypeOracleSource              = AssetType("oracle.source")
	AssetTypePostgresQuery             = AssetType("pg.sql")
	AssetTypePostgresQuerySensor       = AssetType("pg.sensor.query")
	AssetTypePostgresSeed              = AssetType("pg.seed")
	AssetTypePostgresSource            = AssetType("pg.source")
	AssetTypePostgresTableSensor       = AssetType("pg.sensor.table")
	AssetTypePowerBI                   = AssetType("powerbi")
	AssetTypePython                    = AssetType("python")
	AssetTypeQlikSense                 = AssetType("qliksense")
	AssetTypeQlikView                  = AssetType("qlikview")
	AssetTypeQuicksight                = AssetType("quicksight")
	AssetTypeQuicksightDashboard       = AssetType("quicksight.dashboard")
	AssetTypeQuicksightDataset         = AssetType("quicksight.dataset")
	AssetTypeR                         = AssetType("r")
	AssetTypeRedash                    = AssetType("redash")
	AssetTypeRedshiftQuery             = AssetType("rs.sql")
	AssetTypeRedshiftQuerySensor       = AssetType("rs.sensor.query")
	AssetTypeRedshiftSeed              = AssetType("rs.seed")
	AssetTypeRedshiftSource            = AssetType("rs.source")
	AssetTypeRedshiftTableSensor       = AssetType("rs.sensor.table")
	AssetTypeS3KeySensor               = AssetType("s3.sensor.key_sensor")
	AssetTypeSisense                   = AssetType("sisense")
	AssetTypeSnowflakeQuery            = AssetType("sf.sql")
	AssetTypeSnowflakeQuerySensor      = AssetType("sf.sensor.query")
	AssetTypeSnowflakeSeed             = AssetType("sf.seed")
	AssetTypeSnowflakeSource           = AssetType("sf.source")
	AssetTypeSnowflakeTableSensor      = AssetType("sf.sensor.table")
	AssetTypeStarRocks                 = AssetType("starrocks")
	AssetTypeSuperset                  = AssetType("superset")
	AssetTypeSynapseQuery              = AssetType("synapse.sql")
	AssetTypeSynapseQuerySensor        = AssetType("synapse.sensor.query")
	AssetTypeSynapseSeed               = AssetType("synapse.seed")
	AssetTypeSynapseSource             = AssetType("synapse.source")
	AssetTypeSynapseTableSensor        = AssetType("synapse.sensor.table")
	AssetTypeTableau                   = AssetType("tableau")
	AssetTypeTableauDashboard          = AssetType("tableau.dashboard")
	AssetTypeTableauDatasource         = AssetType("tableau.datasource")
	AssetTypeTableauWorkbook           = AssetType("tableau.workbook")
	AssetTypeTableauWorksheet          = AssetType("tableau.worksheet")
	AssetTypeTrinoQuery                = AssetType("trino.sql")
	AssetTypeTrinoQuerySensor          = AssetType("trino.sensor.query")
	AssetTypeDremioQuery               = AssetType("dremio.sql")
	AssetTypeDremioQuerySensor         = AssetType("dremio.sensor.query")
	AssetTypeSailQuery                 = AssetType("sail.sql")
	AssetTypeSailQuerySensor           = AssetType("sail.sensor.query")
	AssetTypeVerticaQuery              = AssetType("vertica.sql")
	AssetTypeVerticaQuerySensor        = AssetType("vertica.sensor.query")
	AssetTypeVerticaSeed               = AssetType("vertica.seed")
	AssetTypeVerticaSource             = AssetType("vertica.source")
	AssetTypeVerticaTableSensor        = AssetType("vertica.sensor.table")
	RunConfigApplyIntervalModifiers    = RunConfig("apply-interval-modifiers")
	RunConfigEndDate                   = RunConfig("end-date")
	RunConfigFullRefresh               = RunConfig("full-refresh")
	RunConfigQueryAnnotations          = RunConfig("query-annotations")
	RunConfigRunID                     = RunConfig("run-id")
	RunConfigStartDate                 = RunConfig("start-date")
	RunConfigExecutionDate             = RunConfig("execution-date")
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
	"fabric":                "fabric-default",
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
	"paddle":                "paddle-default",
	"chargebee":             "chargebee-default",
	"recurly":               "recurly-default",
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
	"starrocks":             "starrocks-default",
	"dremio":                "dremio-default",
	"sail":                  "sail-default",
	"oracle":                "oracle-default",
	"googleanalytics":       "googleanalytics-default",
	"gsc":                   "gsc-default",
	"applovin":              "applovin-default",
	"salesforce":            "salesforce-default",
	"solidgate":             "solidgate-default",
	"square":                "square-default",
	"smartsheet":            "smartsheet-default",
	"sftp":                  "sftp-default",
	"motherduck":            "motherduck-default",
	"elasticsearch":         "elasticsearch-default",
	"vertica":               "vertica-default",
	"adls":                  "adls-default",
	"allium":                "allium-default",
	"anthropic":             "anthropic-default",
	"apifootball":           "apifootball-default",
	"appleads":              "appleads-default",
	"applovinmax":           "applovinmax-default",
	"attio":                 "attio-default",
	"balldontlie":           "balldontlie-default",
	"braze":                 "braze-default",
	"bruin":                 "bruin-default",
	"cassandra":             "cassandra-default",
	"clickup":               "clickup-default",
	"couchbase":             "couchbase-default",
	"cratedb":               "cratedb-default",
	"csv":                   "csv-default",
	"cursor":                "cursor-default",
	"customerio":            "customerio-default",
	"db2":                   "db2-default",
	"docebo":                "docebo-default",
	"dune":                  "dune-default",
	"espn":                  "espn-default",
	"fireflies":             "fireflies-default",
	"fluxx":                 "fluxx-default",
	"footballdata":          "footballdata-default",
	"frankfurter":           "frankfurter-default",
	"freshdesk":             "freshdesk-default",
	"fundraiseup":           "fundraiseup-default",
	"g2":                    "g2-default",
	"github":                "github-default",
	"gitlab":                "gitlab-default",
	"granola":               "granola-default",
	"hostaway":              "hostaway-default",
	"http":                  "http-default",
	"indeed":                "indeed-default",
	"influxdb":              "influxdb-default",
	"intercom":              "intercom-default",
	"isoc_pulse":            "isoc_pulse-default",
	"jira":                  "jira-default",
	"jobtread":              "jobtread-default",
	"kalshi":                "kalshi-default",
	"kinesis":               "kinesis-default",
	"linear":                "linear-default",
	"linkedinads":           "linkedinads-default",
	"mailchimp":             "mailchimp-default",
	"manifold":              "manifold-default",
	"mixpanel":              "mixpanel-default",
	"monday":                "monday-default",
	"personio":              "personio-default",
	"phantombuster":         "phantombuster-default",
	"pinterest":             "pinterest-default",
	"pipedrive":             "pipedrive-default",
	"plusvibeai":            "plusvibeai-default",
	"polymarket":            "polymarket-default",
	"posthog":               "posthog-default",
	"primer":                "primer-default",
	"quickbooks":            "quickbooks-default",
	"quicksight":            "quicksight-default",
	"rabbitmq":              "rabbitmq-default",
	"reddit_ads":            "reddit_ads-default",
	"revenuecat":            "revenuecat-default",
	"sendgrid":              "sendgrid-default",
	"sharepoint":            "sharepoint-default",
	"snapchatads":           "snapchatads-default",
	"socrata":               "socrata-default",
	"spanner":               "spanner-default",
	"sqlite":                "sqlite-default",
	"surveymonkey":          "surveymonkey-default",
	"trustpilot":            "trustpilot-default",
	"twilio":                "twilio-default",
	"wise":                  "wise-default",
	"wistia":                "wistia-default",
	"zoom":                  "zoom-default",
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

type TemplatedBool struct { //nolint:recvcheck
	Value    *bool
	Template string
}

func NewTemplatedBool(value bool) *TemplatedBool {
	return &TemplatedBool{Value: &value}
}

func ParseTemplatedBool(raw string) (*TemplatedBool, error) {
	trimmed := strings.TrimSpace(raw)
	if parsed, err := strconv.ParseBool(trimmed); err == nil {
		return NewTemplatedBool(parsed), nil
	}
	if strings.Contains(trimmed, "{{") || strings.Contains(trimmed, "{%") {
		return &TemplatedBool{Template: trimmed}, nil
	}
	return nil, fmt.Errorf("expected boolean or Jinja template, got %q", raw)
}

func (b *TemplatedBool) UnmarshalJSON(data []byte) error {
	if data == nil || string(data) == "null" {
		return nil
	}

	var value bool
	if err := json.Unmarshal(data, &value); err == nil {
		b.Value = &value
		b.Template = ""
		return nil
	}

	var raw string
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	parsed, err := ParseTemplatedBool(raw)
	if err != nil {
		return err
	}
	*b = *parsed
	return nil
}

func (b TemplatedBool) MarshalJSON() ([]byte, error) {
	if b.Template != "" {
		return json.Marshal(b.Template)
	}
	if b.Value == nil {
		return []byte("true"), nil
	}
	return json.Marshal(*b.Value)
}

func (b *TemplatedBool) UnmarshalYAML(value *yaml.Node) error {
	if value == nil || value.Kind == yaml.DocumentNode {
		return nil
	}

	if value.Kind != yaml.ScalarNode {
		return errors.New("expected scalar boolean or Jinja template")
	}

	if value.Tag == "!!bool" {
		var parsed bool
		if err := value.Decode(&parsed); err != nil {
			return err
		}
		b.Value = &parsed
		b.Template = ""
		return nil
	}

	var raw string
	if err := value.Decode(&raw); err != nil {
		return err
	}

	parsed, err := ParseTemplatedBool(raw)
	if err != nil {
		return err
	}
	*b = *parsed
	return nil
}

func (b *TemplatedBool) Bool() (bool, error) {
	if b == nil || b.Value == nil && b.Template == "" {
		return true, nil
	}
	if b.Template != "" {
		return true, fmt.Errorf("enabled contains unresolved template %q", b.Template)
	}
	return *b.Value, nil
}

func (b TemplatedBool) MarshalYAML() (interface{}, error) {
	if b.Template != "" {
		return b.Template, nil
	}
	if b.Value == nil {
		return true, nil
	}
	return *b.Value, nil
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
	MaterializationStrategyNone               MaterializationStrategy        = ""
	MaterializationStrategyCreateReplace      MaterializationStrategy        = "create+replace"
	MaterializationStrategyDeleteInsert       MaterializationStrategy        = "delete+insert"
	MaterializationStrategyTruncateInsert     MaterializationStrategy        = "truncate+insert"
	MaterializationStrategyAppend             MaterializationStrategy        = "append"
	MaterializationStrategyMerge              MaterializationStrategy        = "merge"
	MaterializationStrategyTimeInterval       MaterializationStrategy        = "time_interval"
	MaterializationStrategyDDL                MaterializationStrategy        = "ddl"
	MaterializationTimeGranularityDate        MaterializationTimeGranularity = "date"
	MaterializationTimeGranularityTimestamp   MaterializationTimeGranularity = "timestamp"
	MaterializationStrategySCD2ByTime         MaterializationStrategy        = "scd2_by_time"
	MaterializationStrategySCD2ByColumn       MaterializationStrategy        = "scd2_by_column"
	MaterializationStrategyDataVaultHub       MaterializationStrategy        = "datavault_hub"
	MaterializationStrategyDataVaultLink      MaterializationStrategy        = "datavault_link"
	MaterializationStrategyDataVaultSatellite MaterializationStrategy        = "datavault_satellite"
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
	MaterializationStrategyDataVaultHub,
	MaterializationStrategyDataVaultLink,
	MaterializationStrategyDataVaultSatellite,
}

type Materialization struct {
	Type            MaterializationType            `json:"type" yaml:"type,omitempty" mapstructure:"type"`
	Strategy        MaterializationStrategy        `json:"strategy" yaml:"strategy,omitempty" mapstructure:"strategy"`
	PartitionBy     string                         `json:"partition_by" yaml:"partition_by,omitempty" mapstructure:"partition_by"`
	ClusterBy       []string                       `json:"cluster_by" yaml:"cluster_by,omitempty" mapstructure:"cluster_by"`
	IncrementalKey  string                         `json:"incremental_key" yaml:"incremental_key,omitempty" mapstructure:"incremental_key"`
	TimeGranularity MaterializationTimeGranularity `json:"time_granularity" yaml:"time_granularity,omitempty" mapstructure:"time_granularity"`
}

func (m Materialization) IsSCD2() bool {
	return m.Strategy == MaterializationStrategySCD2ByColumn || m.Strategy == MaterializationStrategySCD2ByTime
}

func (m Materialization) MarshalJSON() ([]byte, error) {
	if m.Type == "" && m.Strategy == "" && m.PartitionBy == "" && len(m.ClusterBy) == 0 && m.IncrementalKey == "" && m.TimeGranularity == "" {
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
		ints := make([]string, 0, len(*ccv.IntArray))
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
	ID            string           `json:"id" yaml:"-" mapstructure:"-"`
	Name          string           `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	Value         ColumnCheckValue `json:"value" yaml:"value,omitempty" mapstructure:"value"`
	Blocking      DefaultTrueBool  `json:"blocking" yaml:"blocking,omitempty" mapstructure:"blocking"`
	Description   string           `json:"description" yaml:"description,omitempty" mapstructure:"description"`
	Retries       *int             `json:"retries" yaml:"retries,omitempty" mapstructure:"retries"`
	Notifications *Notifications   `json:"notifications,omitempty" yaml:"notifications,omitempty" mapstructure:"notifications"`
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

// ColumnReference points to a column in another asset. It is used to describe a
// foreign-key relationship between this column and the referenced table/column.
type ColumnReference struct {
	Table  string `json:"table" yaml:"table,omitempty" mapstructure:"table"`
	Column string `json:"column" yaml:"column,omitempty" mapstructure:"column"`
}

type Column struct {
	EntityAttribute *EntityAttribute  `json:"entity_attribute" yaml:"-" mapstructure:"-"`
	Name            string            `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	SourceColumn    string            `json:"source_column" yaml:"source_column,omitempty" mapstructure:"source_column"`
	Type            string            `json:"type" yaml:"type,omitempty" mapstructure:"type"`
	Mask            string            `json:"mask,omitempty" yaml:"mask,omitempty" mapstructure:"mask"`
	Description     string            `json:"description" yaml:"description,omitempty" mapstructure:"description"`
	Tags            EmptyStringArray  `json:"tags" yaml:"tags,omitempty" mapstructure:"tags"`
	PrimaryKey      bool              `json:"primary_key" yaml:"primary_key,omitempty" mapstructure:"primary_key"`
	UpdateOnMerge   bool              `json:"update_on_merge" yaml:"update_on_merge,omitempty" mapstructure:"update_on_merge"`
	MergeSQL        string            `json:"merge_sql" yaml:"merge_sql,omitempty" mapstructure:"merge_sql"`
	Nullable        DefaultTrueBool   `json:"nullable" yaml:"nullable,omitempty" mapstructure:"nullable"`
	Default         string            `json:"default" yaml:"default,omitempty" mapstructure:"default"`
	Precision       *int              `json:"precision" yaml:"precision,omitempty" mapstructure:"precision"`
	Scale           *int              `json:"scale" yaml:"scale,omitempty" mapstructure:"scale"`
	Length          *int              `json:"length" yaml:"length,omitempty" mapstructure:"length"`
	Collation       string            `json:"collation" yaml:"collation,omitempty" mapstructure:"collation"`
	ForeignKey      *ColumnReference  `json:"foreign_key" yaml:"foreign_key,omitempty" mapstructure:"foreign_key"`
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

// SQLType returns the column's SQL type, augmenting the declared Type with
// precision/scale or length when those are provided as separate fields and the
// type does not already encode them. For example, type "decimal" with precision
// 10 and scale 2 becomes "decimal(10, 2)", and type "varchar" with length 255
// becomes "varchar(255)". If Type is empty or already parameterized (contains a
// parenthesis), it is returned unchanged.
func (c *Column) SQLType() string {
	t := strings.TrimSpace(c.Type)
	if t == "" || strings.Contains(t, "(") {
		return t
	}

	switch {
	case c.Precision != nil && c.Scale != nil:
		return fmt.Sprintf("%s(%d, %d)", t, *c.Precision, *c.Scale)
	case c.Precision != nil:
		return fmt.Sprintf("%s(%d)", t, *c.Precision)
	case c.Length != nil:
		return fmt.Sprintf("%s(%d)", t, *c.Length)
	default:
		return t
	}
}

type AssetType string

var AssetTypeConnectionMapping = map[AssetType]string{
	AssetTypeBigqueryQuery:       "google_cloud_platform",
	AssetTypeBigqueryTableSensor: "google_cloud_platform",
	AssetTypeBigquerySeed:        "google_cloud_platform",
	AssetTypeBigquerySource:      "google_cloud_platform",
	AssetTypeBigqueryQuerySensor: "google_cloud_platform",

	AssetTypeSnowflakeQuery:            "snowflake",
	AssetTypeSnowflakeQuerySensor:      "snowflake",
	AssetTypeSnowflakeTableSensor:      "snowflake",
	AssetTypeSnowflakeSeed:             "snowflake",
	AssetTypeSnowflakeSource:           "snowflake",
	AssetTypeStarRocks:                 "starrocks",
	AssetTypePostgresQuery:             "postgres",
	AssetTypePostgresSeed:              "postgres",
	AssetTypePostgresQuerySensor:       "postgres",
	AssetTypePostgresTableSensor:       "postgres",
	AssetTypePostgresSource:            "postgres",
	AssetTypeMySQLQuery:                "mysql",
	AssetTypeMySQLSeed:                 "mysql",
	AssetTypeMySQLQuerySensor:          "mysql",
	AssetTypeMySQLTableSensor:          "mysql",
	AssetTypeMongoSource:               "mongo",
	AssetTypeRedshiftQuery:             "redshift",
	AssetTypeRedshiftSeed:              "redshift",
	AssetTypeRedshiftQuerySensor:       "redshift",
	AssetTypeRedshiftSource:            "redshift",
	AssetTypeRedshiftTableSensor:       "redshift",
	AssetTypeMsSQLQuery:                "mssql",
	AssetTypeMsSQLSeed:                 "mssql",
	AssetTypeMsSQLQuerySensor:          "mssql",
	AssetTypeMsSQLTableSensor:          "mssql",
	AssetTypeMsSQLSource:               "mssql",
	AssetTypeFabricQuery:               "fabric",
	AssetTypeFabricSeed:                "fabric",
	AssetTypeFabricQuerySensor:         "fabric",
	AssetTypeFabricTableSensor:         "fabric",
	AssetTypeFabricQueryLegacy:         "fabric",
	AssetTypeFabricSeedLegacy:          "fabric",
	AssetTypeFabricQuerySensorLegacy:   "fabric",
	AssetTypeFabricTableSensorLegacy:   "fabric",
	AssetTypeDatabricksQuery:           "databricks",
	AssetTypeDatabricksSeed:            "databricks",
	AssetTypeDatabricksQuerySensor:     "databricks",
	AssetTypeDatabricksSource:          "databricks",
	AssetTypeDatabricksTableSensor:     "databricks",
	AssetTypeSynapseQuery:              "synapse",
	AssetTypeSynapseSeed:               "synapse",
	AssetTypeSynapseQuerySensor:        "synapse",
	AssetTypeSynapseTableSensor:        "synapse",
	AssetTypeSynapseSource:             "synapse",
	AssetTypeAthenaQuery:               "athena",
	AssetTypeAthenaSeed:                "athena",
	AssetTypeAthenaSQLSensor:           "athena",
	AssetTypeAthenaTableSensor:         "athena",
	AssetTypeAthenaSource:              "athena",
	AssetTypeDuckDBQuery:               "duckdb",
	AssetTypeDuckDBSeed:                "duckdb",
	AssetTypeDuckDBQuerySensor:         "duckdb",
	AssetTypeDuckDBSource:              "duckdb",
	AssetTypeMotherduckQuery:           "motherduck",
	AssetTypeClickHouse:                "clickhouse",
	AssetTypeClickHouseSeed:            "clickhouse",
	AssetTypeClickHouseQuerySensor:     "clickhouse",
	AssetTypeClickHouseTableSensor:     "clickhouse",
	AssetTypeClickHouseSource:          "clickhouse",
	AssetTypeEMRServerlessSpark:        "emr_serverless",
	AssetTypeEMRServerlessPyspark:      "emr_serverless",
	AssetTypeDataprocServerlessPyspark: "dataproc_serverless",
	AssetTypeTrinoQuery:                "trino",
	AssetTypeTrinoQuerySensor:          "trino",
	AssetTypeDremioQuery:               "dremio",
	AssetTypeDremioQuerySensor:         "dremio",
	AssetTypeSailQuery:                 "sail",
	AssetTypeSailQuerySensor:           "sail",
	AssetTypeOracleQuery:               "oracle",
	AssetTypeOracleSource:              "oracle",
	AssetTypeS3KeySensor:               "aws",
	AssetTypeDynamoDB:                  "dynamodb",
	AssetTypeElasticsearch:             "elasticsearch",
	AssetTypeGoogleSheets:              "google_sheets",
	AssetTypeVerticaQuery:              "vertica",
	AssetTypeVerticaSeed:               "vertica",
	AssetTypeVerticaQuerySensor:        "vertica",
	AssetTypeVerticaTableSensor:        "vertica",
	AssetTypeVerticaSource:             "vertica",
	AssetTypeQuicksightDataset:         "quicksight",
	AssetTypeQuicksightDashboard:       "quicksight",
}

// assetTypeConnectionAlternates lists every connection platform key that can back
// an asset type, in priority order, for the asset types that more than one
// connection type can serve. A mongo.source asset, for example, can be backed by
// either a "mongo" or a "mongo_atlas" connection, so resolving its default
// connection must consider both keys. Asset types absent here resolve through
// their single AssetTypeConnectionMapping entry.
var assetTypeConnectionAlternates = map[AssetType][]string{
	AssetTypeMongoSource: {"mongo", "mongo_atlas"},
}

// connectionPlatformsForAssetType returns the connection platform keys to try when
// resolving an asset's default connection, in priority order. It falls back to the
// asset type's single AssetTypeConnectionMapping entry (passed as primary) when the
// type has no alternates.
func connectionPlatformsForAssetType(assetType AssetType, primary string) []string {
	if alternates, ok := assetTypeConnectionAlternates[assetType]; ok {
		return alternates
	}
	return []string{primary}
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
	"starrocks":     AssetTypeStarRocks,
	"oracle":        AssetTypeOracleQuery,
	"motherduck":    AssetTypeMotherduckQuery,
	"dynamodb":      AssetTypeDynamoDB,
	"elasticsearch": AssetTypeElasticsearch,
	"gsheets":       AssetTypeGoogleSheets,
	"vertica":       AssetTypeVerticaQuery,
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
	ID            string          `json:"id" yaml:"-" mapstructure:"-"`
	Name          string          `json:"name" yaml:"name" mapstructure:"name"`
	Description   string          `json:"description" yaml:"description,omitempty" mapstructure:"description"`
	Value         int64           `json:"value" yaml:"value" mapstructure:"value"`
	Count         *int64          `json:"count,omitempty" yaml:"count,omitempty" mapstructure:"count"`
	Blocking      DefaultTrueBool `json:"blocking" yaml:"blocking,omitempty" mapstructure:"blocking"`
	Query         string          `json:"query" yaml:"query" mapstructure:"query"`
	Retries       *int            `json:"retries" yaml:"retries,omitempty" mapstructure:"retries"`
	Notifications *Notifications  `json:"notifications,omitempty" yaml:"notifications,omitempty" mapstructure:"notifications"`
}

// UnitTest pins an asset's transformation logic by running it against mocked
// input rows and asserting the produced output, independent of production data.
// Unlike quality checks (which validate real data after a run), a unit test
// substitutes the tables the query reads with fixtures. See
// docs/proposals/sql-unit-tests.md.
type UnitTest struct {
	Name          string                 `json:"name" yaml:"name" mapstructure:"name"`
	Description   string                 `json:"description,omitempty" yaml:"description,omitempty" mapstructure:"description"`
	Inputs        []UnitTestInput        `json:"inputs,omitempty" yaml:"inputs,omitempty" mapstructure:"inputs"`
	Fixtures      []string               `json:"fixtures,omitempty" yaml:"fixtures,omitempty" mapstructure:"fixtures"`
	Variables     map[string]interface{} `json:"variables,omitempty" yaml:"variables,omitempty" mapstructure:"variables"`
	ExecutionTime string                 `json:"execution_time,omitempty" yaml:"execution_time,omitempty" mapstructure:"execution_time"`
	Expected      UnitTestExpected       `json:"expected" yaml:"expected,omitempty" mapstructure:"expected"`
}

// Fixture is a named, reusable set of mock rows for one asset, defined at the
// pipeline level and pulled into a unit test by name (UnitTest.Fixtures). It
// lets many tests share a baseline set of input rows (typically lookup or
// dimension tables) instead of repeating them in every test. A test's own
// inputs take precedence over a referenced fixture for the same asset.
type Fixture struct {
	Name  string                   `json:"name" yaml:"name" mapstructure:"name"`
	Asset string                   `json:"asset" yaml:"asset" mapstructure:"asset"`
	Rows  []map[string]interface{} `json:"rows,omitempty" yaml:"rows,omitempty" mapstructure:"rows"`
}

// UnitTestInput is a mocked table the asset's query reads from, identified by
// the upstream asset name as it appears in the SQL. Rows are sparse: any column
// not listed defaults to NULL.
type UnitTestInput struct {
	Asset string                   `json:"asset" yaml:"asset" mapstructure:"asset"`
	Rows  []map[string]interface{} `json:"rows,omitempty" yaml:"rows,omitempty" mapstructure:"rows"`
}

// UnitTestExpected describes the output a unit test asserts. Count and Rows are
// independent: a test may set either or both, and when both are set both must
// hold. Count asserts the total number of produced rows. Rows compares the
// produced rows against the listed ones. Match is "subset" (the default: every
// expected row must appear, extra rows allowed) or "exact"; Order is "any" (the
// default) or "strict".
type UnitTestExpected struct {
	Rows  []map[string]interface{} `json:"rows,omitempty" yaml:"rows,omitempty" mapstructure:"rows"`
	Count *int64                   `json:"count,omitempty" yaml:"count,omitempty" mapstructure:"count"`
	Match string                   `json:"match,omitempty" yaml:"match,omitempty" mapstructure:"match"`
	Order string                   `json:"order,omitempty" yaml:"order,omitempty" mapstructure:"order"`
	// CTEs asserts the output of named intermediate CTEs inside the asset's
	// query, keyed by CTE name, so sub-logic can be pinned without asserting the
	// whole result.
	CTEs map[string]UnitTestCTEExpected `json:"ctes,omitempty" yaml:"ctes,omitempty" mapstructure:"ctes"`
}

// UnitTestCTEExpected asserts the rows produced by one named CTE in the asset's
// query. Same row/count/match/order semantics as the top-level expectation.
type UnitTestCTEExpected struct {
	Rows  []map[string]interface{} `json:"rows,omitempty" yaml:"rows,omitempty" mapstructure:"rows"`
	Count *int64                   `json:"count,omitempty" yaml:"count,omitempty" mapstructure:"count"`
	Match string                   `json:"match,omitempty" yaml:"match,omitempty" mapstructure:"match"`
	Order string                   `json:"order,omitempty" yaml:"order,omitempty" mapstructure:"order"`
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
	isAsset := u.Type == "" || u.Type == selectorAssetDependencyType
	if u.Mode == UpstreamModeFull && isAsset {
		return u.Value, nil
	}

	val := map[string]any{}
	id := "uri"
	if isAsset {
		id = selectorAssetDependencyType
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

type RoutingConfig struct {
	EgressGateway string `json:"egress_gateway,omitempty" yaml:"egress_gateway,omitempty" mapstructure:"egress_gateway"`
}

func (r *RoutingConfig) IsZero() bool {
	return r == nil || r.EgressGateway == ""
}

// Clone returns a deep copy of r. The current implementation is a struct
// value copy, which is correct as long as all fields are value types. If a
// pointer/slice/map field is added to RoutingConfig this method must be
// updated to copy those fields explicitly.
func (r *RoutingConfig) Clone() *RoutingConfig {
	if r == nil {
		return nil
	}

	clone := *r
	return &clone
}

// Asset is the in-memory representation of one asset within a pipeline. When
// adding a new exported string-valued field, extend renderAssetStrings in
// pkg/pipeline/variant.go so variant materialization renders it. Asset bodies
// (ExecutableFile.Content) are intentionally left for run-time Jinja and are
// excluded from the visitor — see the allowlist in TestVariantVisitorCoversStringFields.
type Asset struct { //nolint:recvcheck
	ID                string             `json:"id" yaml:"-" mapstructure:"-"`
	URI               string             `json:"uri" yaml:"uri,omitempty" mapstructure:"uri"`
	Name              string             `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	Type              AssetType          `json:"type" yaml:"type,omitempty" mapstructure:"type"`
	Enabled           *TemplatedBool     `json:"enabled,omitempty" yaml:"enabled,omitempty" mapstructure:"enabled"`
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
	Parameters        ParameterMap       `json:"parameters" yaml:"parameters,omitempty" mapstructure:"parameters"`
	Secrets           []SecretMapping    `json:"secrets" yaml:"secrets,omitempty" mapstructure:"secrets"`
	Extends           []string           `json:"extends" yaml:"extends,omitempty" mapstructure:"extends"`
	Columns           []Column           `json:"columns" yaml:"columns,omitempty" mapstructure:"columns"`
	CustomChecks      []CustomCheck      `json:"custom_checks" yaml:"custom_checks,omitempty" mapstructure:"custom_checks"`
	UnitTests         []UnitTest         `json:"unit_tests,omitempty" yaml:"unit_tests,omitempty" mapstructure:"unit_tests"`
	Hooks             Hooks              `json:"hooks,omitempty" yaml:"hooks,omitempty" mapstructure:"hooks"`
	Metadata          EmptyStringMap     `json:"metadata" yaml:"metadata,omitempty" mapstructure:"metadata"`
	Snowflake         SnowflakeConfig    `json:"snowflake" yaml:"snowflake,omitempty" mapstructure:"snowflake"`
	Athena            AthenaConfig       `json:"athena" yaml:"athena,omitempty" mapstructure:"athena"`
	Routing           *RoutingConfig     `json:"routing,omitempty" yaml:"routing,omitempty" mapstructure:"routing"`
	IntervalModifiers IntervalModifiers  `json:"interval_modifiers" yaml:"interval_modifiers,omitempty" mapstructure:"interval_modifiers"`
	RerunCooldown     *int               `json:"rerun_cooldown,omitempty" yaml:"rerun_cooldown,omitempty" mapstructure:"rerun_cooldown"`
	Retries           *int               `json:"retries" yaml:"retries,omitempty" mapstructure:"retries"`
	RetriesDelay      *int               `json:"retries_delay,omitempty" yaml:"-" mapstructure:"-"`
	RefreshRestricted *bool              `json:"refresh_restricted,omitempty" yaml:"refresh_restricted,omitempty" mapstructure:"refresh_restricted"`
	Notifications     *Notifications     `json:"notifications,omitempty" yaml:"notifications,omitempty" mapstructure:"notifications"`

	upstream   []*Asset
	downstream []*Asset
}

// IsEnabled returns the asset's resolved enabled value. It panics if enabled
// still contains an unresolved template; use EnabledValue to handle that error.
func (a *Asset) IsEnabled() bool {
	enabled, err := a.EnabledValue()
	if err != nil {
		panic(err)
	}
	return enabled
}

func (a *Asset) EnabledValue() (bool, error) {
	if a == nil || a.Enabled == nil {
		return true, nil
	}
	return a.Enabled.Bool()
}

type Hook struct {
	Query string `json:"query" yaml:"query" mapstructure:"query"`
}

type Hooks struct {
	ApplicableTypes []string `json:"applicable_type,omitempty" yaml:"applicable_type,omitempty" mapstructure:"applicable_type"`
	Pre             []Hook   `json:"pre,omitempty" yaml:"pre,omitempty" mapstructure:"pre"`
	Post            []Hook   `json:"post,omitempty" yaml:"post,omitempty" mapstructure:"post"`
}

func (h Hooks) IsZero() bool {
	return len(h.Pre) == 0 && len(h.Post) == 0 && len(h.ApplicableTypes) == 0
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
		Type:  selectorAssetDependencyType,
		Value: asset.Name,
		Mode:  UpstreamModeFull,
	})
}

// prefixSchemaComponent applies the dev-environment schema prefix to the schema
// component of a (possibly multi-part) table name. The schema is always the
// component immediately before the table, so for `schema.table` it prefixes the
// first component and for `catalog.schema.table` it prefixes the middle one,
// leaving the catalog/database untouched. This matches the dev-environment query
// rewriter (see pkg/devenv). Single-component names have no schema to prefix and
// are returned unchanged.
func prefixSchemaComponent(name, prefix string) string {
	nameParts := strings.Split(name, ".")
	if len(nameParts) < 2 {
		return name
	}
	schemaIdx := len(nameParts) - 2
	nameParts[schemaIdx] = prefix + nameParts[schemaIdx]
	return strings.Join(nameParts, ".")
}

func (a *Asset) PrefixSchema(prefix string) {
	if prefix == "" {
		return
	}

	a.Name = prefixSchemaComponent(a.Name, prefix)
}

func (a *Asset) PrefixUpstreams(prefix string) {
	if prefix == "" {
		return
	}

	for i, u := range a.Upstreams {
		if u.Type != selectorAssetDependencyType {
			continue
		}

		a.Upstreams[i].Value = prefixSchemaComponent(u.Value, prefix)
	}
}

// removeRedundanciesBeforePersisting aims to remove unnecessary configuration from the asset.
// This is particularly useful when we save a formatted version of the asset itself.
func (a *Asset) removeRedundanciesBeforePersisting() {
	a.clearDuplicateUpstreams()
	a.clearDuplicateTags()
	a.removeExtraSpacesAtLineEndingsInTextContent()

	// python assets don't require a type anymore
	if a.Type == AssetTypePython && strings.HasSuffix(a.ExecutableFile.Path, ".py") {
		a.Type = ""
	}

	if a.Routing.IsZero() {
		a.Routing = nil
	}
}

func deduplicateTags(tags EmptyStringArray) EmptyStringArray {
	if len(tags) == 0 {
		return tags
	}

	seen := make(map[string]bool, len(tags))
	unique := make(EmptyStringArray, 0, len(tags))
	for _, tag := range tags {
		lower := strings.ToLower(tag)
		if seen[lower] {
			continue
		}
		seen[lower] = true
		unique = append(unique, tag)
	}

	return unique
}

func (a *Asset) clearDuplicateTags() {
	a.Tags = deduplicateTags(a.Tags)
	for i := range a.Columns {
		a.Columns[i].Tags = deduplicateTags(a.Columns[i].Tags)
	}
}

// clearDuplicateUpstreams removes duplicate upstream dependencies from the asset.
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
		filteredParams := ParameterMap{}
		for key, value := range a.Parameters {
			// Only keep parameters that are NOT in defaults or have different values
			if defaultValue, existsInDefaults := pipeline[0].DefaultValues.Parameters[key]; !existsInDefaults || !reflect.DeepEqual(defaultValue, value) {
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

	keysToAddSpace := []string{"custom_checks", "depends", "columns", "materialization", "routing", "secrets", "parameters", "hooks"}
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

func (m EmptyStringMap) MarshalJSON() ([]byte, error) { //nolint: staticcheck
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

// ParameterMap stores asset parameters with arbitrary value types, allowing
// parameters to hold strings, numbers, booleans, or nested structures.
type ParameterMap map[string]interface{} //nolint:recvcheck

func (m ParameterMap) MarshalJSON() ([]byte, error) { //nolint:recvcheck
	if m == nil {
		return []byte{'{', '}'}, nil
	}
	return json.Marshal(map[string]interface{}(m))
}

func (m *ParameterMap) UnmarshalJSON(data []byte) error {
	if data == nil {
		return nil
	}
	var v map[string]interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if len(v) == 0 {
		return nil
	}
	*m = v
	return nil
}

// GetString returns the value for key as a string. Scalar primitives are
// coerced to their string representations so that YAML configs with unquoted
// values continue to work. Structured values intentionally do not stringify.
// Returns ("", false) if the key is absent or the value is nil.
func (m ParameterMap) GetString(key string) (string, bool) {
	v, ok := m[key]
	if !ok || v == nil {
		return "", false
	}
	switch val := v.(type) {
	case string:
		return val, true
	case int8:
		return strconv.FormatInt(int64(val), 10), true
	case int16:
		return strconv.FormatInt(int64(val), 10), true
	case int32:
		return strconv.FormatInt(int64(val), 10), true
	case int:
		return strconv.Itoa(val), true
	case int64:
		return strconv.FormatInt(val, 10), true
	case uint:
		return strconv.FormatUint(uint64(val), 10), true
	case uint8:
		return strconv.FormatUint(uint64(val), 10), true
	case uint16:
		return strconv.FormatUint(uint64(val), 10), true
	case uint32:
		return strconv.FormatUint(uint64(val), 10), true
	case uint64:
		return strconv.FormatUint(val, 10), true
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32), true
	case float64:
		const maxInt64Float = float64(1 << 63)
		if val >= -maxInt64Float && val < maxInt64Float && val == math.Trunc(val) {
			return strconv.FormatInt(int64(val), 10), true
		}
		return strconv.FormatFloat(val, 'f', -1, 64), true
	case bool:
		return strconv.FormatBool(val), true
	default:
		return "", false
	}
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

type CatchupMode string

const (
	CatchupNone   CatchupMode = ""
	CatchupActive CatchupMode = "active"
	CatchupAll    CatchupMode = "all"
)

func (c *CatchupMode) UnmarshalYAML(node *yaml.Node) error {
	var b bool
	if err := node.Decode(&b); err == nil {
		*c = boolToCatchup(b)
		return nil
	}
	var s string
	if err := node.Decode(&s); err == nil {
		*c = normalizeCatchupString(s)
		return nil
	}
	*c = CatchupNone
	return nil
}

func (c *CatchupMode) UnmarshalJSON(data []byte) error {
	var b bool
	if err := json.Unmarshal(data, &b); err == nil {
		*c = boolToCatchup(b)
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		*c = normalizeCatchupString(s)
		return nil
	}
	*c = CatchupNone
	return nil
}

func boolToCatchup(b bool) CatchupMode {
	if b {
		return CatchupActive
	}
	return CatchupNone
}

func normalizeCatchupString(s string) CatchupMode {
	switch CatchupMode(s) {
	case CatchupActive, CatchupAll:
		return CatchupMode(s)
	default:
		return CatchupNone
	}
}

// Pipeline is the in-memory representation of a pipeline.yml plus its loaded
// assets. When adding a new exported field that may legitimately contain Jinja
// (`{{ var.X }}`) — typically string-valued metadata fields — extend the
// renderPipelineStrings visitor in pkg/pipeline/variant.go so variant
// materialization picks it up. The TestVariantVisitorCoversStringFields test
// guards against silently regressing this.
type Pipeline struct {
	LegacyID           string                 `json:"legacy_id" yaml:"id,omitempty" mapstructure:"id"`
	Name               string                 `json:"name" yaml:"name,omitempty" mapstructure:"name"`
	Tags               EmptyStringArray       `json:"tags" yaml:"tags,omitempty" mapstructure:"tags"`
	Domains            EmptyStringArray       `json:"domains" yaml:"domains,omitempty" mapstructure:"domains"`
	Meta               EmptyStringMap         `json:"meta" yaml:"meta,omitempty" mapstructure:"meta"`
	Owner              string                 `json:"owner" yaml:"owner,omitempty" mapstructure:"owner"`
	Schedule           Schedule               `json:"schedule" yaml:"schedule,omitempty" mapstructure:"schedule"`
	StartDate          string                 `json:"start_date" yaml:"start_date,omitempty" mapstructure:"start_date"`
	DefinitionFile     DefinitionFile         `json:"definition_file" yaml:"-"`
	DefaultConnections EmptyStringMap         `json:"default_connections" yaml:"default_connections,omitempty" mapstructure:"default_connections"`
	Assets             []*Asset               `json:"assets" yaml:"assets,omitempty"`
	Notifications      Notifications          `json:"notifications" yaml:"notifications,omitempty" mapstructure:"notifications"`
	Catchup            CatchupMode            `json:"catchup" yaml:"catchup,omitempty" mapstructure:"catchup"`
	MetadataPush       MetadataPush           `json:"metadata_push" yaml:"metadata_push,omitempty" mapstructure:"metadata_push"`
	Retries            *int                   `json:"retries" yaml:"retries,omitempty" mapstructure:"retries"`
	RetriesDelay       *int                   `json:"retries_delay,omitempty" yaml:"-" mapstructure:"-"`
	Concurrency        int                    `json:"concurrency" yaml:"concurrency,omitempty" mapstructure:"concurrency"`
	MaxActiveSteps     *int                   `json:"max_active_steps" yaml:"max_active_steps,omitempty" mapstructure:"max_active_steps"`
	DefaultValues      *DefaultValues         `json:"default,omitempty" yaml:"default,omitempty" mapstructure:"default,omitempty"`
	Commit             string                 `json:"commit" yaml:"commit,omitempty"`
	Snapshot           string                 `json:"snapshot" yaml:"snapshot,omitempty"`
	Agent              bool                   `json:"agent" yaml:"agent,omitempty" mapstructure:"agent"`
	Variables          Variables              `json:"variables" yaml:"variables,omitempty" mapstructure:"variables"`
	Variants           VariantSet             `json:"variants,omitempty" yaml:"variants,omitempty" mapstructure:"variants"`
	SelectedVariant    string                 `json:"selected_variant" yaml:"-"`
	TasksByType        map[AssetType][]*Asset `json:"-" yaml:"-"`
	tasksByName        map[string]*Asset      `yaml:"-"`
	MacrosPath         string                 `json:"-" yaml:"-"`
	Macros             []Macro                `json:"macros" yaml:"macros,omitempty" mapstructure:"macros"`
	Fixtures           []Fixture              `json:"fixtures,omitempty" yaml:"fixtures,omitempty" mapstructure:"fixtures"`
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

	if err := p.Variants.Validate(p.Variables); err != nil {
		return err
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

	if err := p.Variants.Validate(p.Variables); err != nil {
		return err
	}

	return nil
}

type DefaultValues struct {
	Type              string                 `json:"type" yaml:"type" mapstructure:"type"`
	Description       string                 `json:"description,omitempty" yaml:"description,omitempty" mapstructure:"description"`
	StartDate         string                 `json:"start_date,omitempty" yaml:"start_date,omitempty" mapstructure:"start_date"`
	Connection        string                 `json:"connection,omitempty" yaml:"connection,omitempty" mapstructure:"connection"`
	Tags              EmptyStringArray       `json:"tags,omitempty" yaml:"tags,omitempty" mapstructure:"tags"`
	Domains           EmptyStringArray       `json:"domains,omitempty" yaml:"domains,omitempty" mapstructure:"domains"`
	Meta              EmptyStringMap         `json:"meta,omitempty" yaml:"meta,omitempty" mapstructure:"meta"`
	Materialization   Materialization        `json:"materialization,omitempty" yaml:"materialization,omitempty" mapstructure:"materialization"`
	Upstreams         []Upstream             `json:"upstreams,omitempty" yaml:"depends,omitempty" mapstructure:"depends"`
	Image             string                 `json:"image,omitempty" yaml:"image,omitempty" mapstructure:"image"`
	Instance          string                 `json:"instance,omitempty" yaml:"instance,omitempty" mapstructure:"instance"`
	Owner             string                 `json:"owner,omitempty" yaml:"owner,omitempty" mapstructure:"owner"`
	Tier              int                    `json:"tier,omitempty" yaml:"tier,omitempty" mapstructure:"tier"`
	Parameters        map[string]interface{} `json:"parameters" yaml:"parameters" mapstructure:"parameters"`
	Secrets           []secretMapping        `json:"secrets" yaml:"secrets" mapstructure:"secrets"`
	Extends           []string               `json:"extends,omitempty" yaml:"extends,omitempty" mapstructure:"extends"`
	Columns           []Column               `json:"columns,omitempty" yaml:"columns,omitempty" mapstructure:"columns"`
	CustomChecks      []CustomCheck          `json:"custom_checks,omitempty" yaml:"custom_checks,omitempty" mapstructure:"custom_checks"`
	Hooks             Hooks                  `json:"hooks" yaml:"hooks" mapstructure:"hooks"`
	Metadata          EmptyStringMap         `json:"metadata,omitempty" yaml:"metadata,omitempty" mapstructure:"metadata"`
	Snowflake         SnowflakeConfig        `json:"snowflake,omitempty" yaml:"snowflake,omitempty" mapstructure:"snowflake"`
	Athena            AthenaConfig           `json:"athena,omitempty" yaml:"athena,omitempty" mapstructure:"athena"`
	Routing           *RoutingConfig         `json:"routing,omitempty" yaml:"routing,omitempty" mapstructure:"routing"`
	IntervalModifiers IntervalModifiers      `json:"interval_modifiers" yaml:"interval_modifiers" mapstructure:"interval_modifiers"`
	RerunCooldown     *int                   `json:"rerun_cooldown,omitempty" yaml:"rerun_cooldown,omitempty" mapstructure:"rerun_cooldown"`
	Retries           *int                   `json:"retries,omitempty" yaml:"retries,omitempty" mapstructure:"retries"`
	RefreshRestricted *bool                  `json:"refresh_restricted,omitempty" yaml:"refresh_restricted,omitempty" mapstructure:"refresh_restricted"`
	Notifications     *Notifications         `json:"notifications,omitempty" yaml:"notifications,omitempty" mapstructure:"notifications"`
}

func (d *DefaultValues) UnmarshalYAML(value *yaml.Node) error {
	var definition taskDefinition
	if err := value.Decode(&definition); err != nil {
		return err
	}

	asset, err := taskDefinitionToAsset(definition)
	if err != nil {
		return err
	}

	*d = DefaultValues{
		Type:              string(asset.Type),
		Description:       asset.Description,
		StartDate:         asset.StartDate,
		Connection:        asset.Connection,
		Tags:              asset.Tags,
		Domains:           asset.Domains,
		Meta:              asset.Meta,
		Materialization:   asset.Materialization,
		Upstreams:         asset.Upstreams,
		Image:             asset.Image,
		Instance:          asset.Instance,
		Owner:             asset.Owner,
		Tier:              asset.Tier,
		Parameters:        asset.Parameters,
		Secrets:           definition.Secrets,
		Extends:           asset.Extends,
		Columns:           asset.Columns,
		CustomChecks:      asset.CustomChecks,
		Hooks:             asset.Hooks,
		Metadata:          asset.Metadata,
		Snowflake:         asset.Snowflake,
		Athena:            asset.Athena,
		Routing:           asset.Routing,
		IntervalModifiers: asset.IntervalModifiers,
		RerunCooldown:     asset.RerunCooldown,
		Retries:           asset.Retries,
		RefreshRestricted: asset.RefreshRestricted,
		Notifications:     asset.Notifications,
	}

	return nil
}

func (p *Pipeline) GetCompatibilityHash() string {
	parts := make([]string, 0, len(p.Assets)+1)
	parts = append(parts, p.Name)
	for _, asset := range p.Assets {
		var assetBuilder strings.Builder
		fmt.Fprintf(&assetBuilder, ":%s{", asset.Name)
		if enabled, err := asset.EnabledValue(); err == nil && !enabled {
			assetBuilder.WriteString(":enabled:false")
		} else if err != nil && asset.Enabled != nil && asset.Enabled.Template != "" {
			fmt.Fprintf(&assetBuilder, ":enabled:%s", asset.Enabled.Template)
		}
		for _, upstream := range asset.Upstreams {
			fmt.Fprintf(&assetBuilder, ":%s:%s:", upstream.Value, upstream.Type)
		}
		assetBuilder.WriteByte('}')
		parts = append(parts, assetBuilder.String())
	}
	parts = append(parts, ":")
	hash := sha256.New()
	hash.Write([]byte(strings.Join(parts, "")))
	return hex.EncodeToString(hash.Sum(nil))
}

func (p *Pipeline) GetAllConnectionNamesForAsset(asset *Asset) ([]string, error) {
	assetType := asset.Type
	if assetType == AssetTypePython { //nolint
		connectionNames := assetSecretConnectionNames(asset)
		if asset.Connection != "" {
			connectionNames = append(connectionNames, asset.Connection)
		} else if asset.Materialization.Type != "" {
			conn, err := p.GetConnectionNameForAsset(asset)
			if err != nil {
				return []string{}, err
			}
			connectionNames = append(connectionNames, conn)
		}
		return connectionNames, nil
	} else if assetType == AssetTypeR {
		connectionNames := assetSecretConnectionNames(asset)
		if asset.Connection != "" {
			connectionNames = append(connectionNames, asset.Connection)
		}
		return connectionNames, nil
	} else if assetType == AssetTypeIngestr {
		ingestrSource, ok := asset.Parameters.GetString("source_connection")
		if !ok {
			return []string{}, errors.Errorf("No source connection in asset")
		}

		ingestrDestination, err := p.GetConnectionNameForAsset(asset)
		if err != nil {
			return []string{}, err
		}

		return []string{ingestrDestination, ingestrSource}, nil
	} else if assetMainTaskIsConnectionless(assetType) {
		return nil, nil
	} else {
		conn, err := p.GetConnectionNameForAsset(asset)
		if err != nil {
			return []string{}, err
		}

		return []string{conn}, nil
	}
}

func assetMainTaskIsConnectionless(assetType AssetType) bool {
	switch assetType {
	case AssetTypeAthenaSource,
		AssetTypeBigquerySource,
		AssetTypeClickHouseSource,
		AssetTypeDatabricksSource,
		AssetTypeDuckDBSource,
		AssetTypeMongoSource,
		AssetTypeMsSQLSource,
		AssetTypeOracleSource,
		AssetTypePostgresSource,
		AssetTypeRedshiftSource,
		AssetTypeSnowflakeSource,
		AssetTypeSynapseSource,
		AssetTypeVerticaSource,
		AssetTypeEmpty,
		AssetTypeAgentClaudeCode,
		AssetTypeDomo,
		AssetTypeGoodData,
		AssetTypeGrafana,
		AssetTypeLooker,
		AssetTypeLookerStudio,
		AssetTypeMetabase,
		AssetTypeModeBI,
		AssetTypePowerBI,
		AssetTypeQlikSense,
		AssetTypeQlikView,
		AssetTypeQuicksight,
		AssetTypeRedash,
		AssetTypeSisense,
		AssetTypeSuperset,
		AssetTypeTableauDashboard,
		AssetTypeTableauWorksheet,
		AssetType("appsflyer.export.bq"),
		AssetType("dbt"),
		AssetType("dbt.test"),
		AssetType("gcs.sensor.object"),
		AssetType("gcs.sensor.object_sensor_with_prefix"),
		AssetType("python.beta"),
		AssetType("python.legacy"):
		return true
	default:
		return false
	}
}

func assetSecretConnectionNames(asset *Asset) []string {
	secretKeys := make([]string, 0, len(asset.Secrets))
	for _, secret := range asset.Secrets {
		secretKeys = append(secretKeys, secret.SecretKey)
	}
	return secretKeys
}

func (p *Pipeline) GetConnectionNameForAsset(asset *Asset) (string, error) {
	if asset.Connection != "" {
		return asset.Connection, nil
	}

	assetType := asset.Type
	var ok bool
	switch assetType {
	case AssetTypeIngestr:
		if conn, ok := asset.Parameters.GetString("destination_connection"); ok && conn != "" {
			return conn, nil
		}

		ingestrDest, _ := asset.Parameters.GetString("destination")
		assetType, ok = IngestrTypeConnectionMapping[ingestrDest]
		if !ok {
			return "", errors.Errorf("connection type could not be inferred for destination '%s', please specify a `connection` key in the asset", ingestrDest)
		}
	case AssetTypePython, AssetTypeEmpty:
		assetType = p.GetMajorityAssetTypesFromSQLAssets(AssetTypeBigqueryQuery)
	default:
		// For all other asset types, use the asset type directly for connection mapping lookup.
	}

	mapping, ok := AssetTypeConnectionMapping[assetType]
	if !ok {
		return "", errors.Errorf("no connection mapping found for asset type '%s'", assetType)
	}

	// Some asset types can be served by more than one connection platform (e.g. a
	// mongo.source asset accepts either a "mongo" or a "mongo_atlas" connection), so
	// check every candidate platform's explicit default connection before falling
	// back to the magic default name.
	for _, platform := range connectionPlatformsForAssetType(assetType, mapping) {
		if conn, ok := p.DefaultConnections[platform]; ok {
			return conn, nil
		}
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
		AssetTypeBigqueryQuery:     0,
		AssetTypeSnowflakeQuery:    0,
		AssetTypePostgresQuery:     0,
		AssetTypeMsSQLQuery:        0,
		AssetTypeVerticaQuery:      0,
		AssetTypeDatabricksQuery:   0,
		AssetTypeRedshiftQuery:     0,
		AssetTypeSynapseQuery:      0,
		AssetTypeFabricQuery:       0,
		AssetTypeFabricQueryLegacy: 0,
		AssetTypeAthenaQuery:       0,
		AssetTypeDuckDBQuery:       0,
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
			ingestrDestination, ok := asset.Parameters.GetString("destination")
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
	variantRenderer    VariantRendererFactory

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

func NewBuilder(config BuilderConfig, yamlTaskCreator TaskCreator, commentTaskCreator TaskCreator, fs afero.Fs, gr glossaryReader, variantRenderer VariantRendererFactory) *Builder {
	b := &Builder{
		config:             config,
		yamlTaskCreator:    yamlTaskCreator,
		commentTaskCreator: commentTaskCreator,
		fs:                 fs,
		GlossaryReader:     gr,
		variantRenderer:    variantRenderer,
	}

	b.assetMutators = []AssetMutator{
		b.fillGlossaryStuff,
		b.SetupDefaultsFromPipeline,
		b.InjectConnectionAsSecret,
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
	variantName      string
	skipVariantCheck bool
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

// WithVariant materializes the named variant during pipeline construction.
// The Builder must have been constructed with a non-nil VariantRendererFactory;
// otherwise the call errors. Passing an empty name is equivalent to omitting
// the option.
func WithVariant(name string) CreatePipelineOption {
	return func(o *createPipelineConfig) {
		o.variantName = name
	}
}

// withSkipVariantCheck is an internal option used by CreatePipelinesFromPath
// to probe a path for variants without triggering the "variant required" guard.
// Not exported because external callers should use WithOnlyPipeline (which also
// skips the check) for the same effect.
func withSkipVariantCheck() CreatePipelineOption {
	return func(o *createPipelineConfig) {
		o.skipVariantCheck = true
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

	// Variant guard: when the pipeline declares variants, callers MUST pick
	// one (via WithVariant) or use CreatePipelinesFromPath. WithOnlyPipeline
	// is exempted because it explicitly asks for the un-materialized template
	// view (used by `bruin internal list-variants` and the plural builder's
	// own probe step).
	if !config.onlyPipeline && !config.skipVariantCheck && len(pipeline.Variants) > 0 && config.variantName == "" {
		return nil, fmt.Errorf("pipeline %q declares variants %v; pass WithVariant(name) or use CreatePipelinesFromPath", pipeline.Name, pipeline.Variants.Names())
	}

	// Apply the variant's variable overrides BEFORE asset construction so any
	// asset-level mutator (e.g. parameter rendering) sees variant values.
	if config.variantName != "" && len(pipeline.Variants) > 0 {
		if err := pipeline.ApplyVariantVariables(config.variantName); err != nil {
			return nil, err
		}
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

	taskResults := b.createAssetsFromFiles(taskFiles, pipeline)
	for _, result := range taskResults {
		if result.err != nil {
			return nil, result.err
		}

		if result.task == nil {
			continue
		}

		task := result.task
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

			if upstream.Type != selectorAssetDependencyType {
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

	// Final step for variant pipelines: render identity/structure string fields
	// (asset names, dependencies, schedule, …) so the materialized pipeline has
	// stable values. Variables.Merge ran earlier; the asset *Asset pointer graph
	// stays valid because rendering only mutates values, not pointers.
	if config.variantName != "" && len(pipeline.Variants) > 0 {
		if b.variantRenderer == nil {
			return nil, fmt.Errorf("WithVariant(%q) requires a renderer; pass a VariantRendererFactory to NewBuilder", config.variantName)
		}
		variantName := config.variantName
		if err := pipeline.RenderTemplatedFields(func(vars map[string]any) RenderFunc {
			return b.variantRenderer(vars, variantName)
		}); err != nil {
			return nil, err
		}
	}

	return pipeline, nil
}

type assetFromFileResult struct {
	task *Asset
	err  error
}

func (b *Builder) createAssetsFromFiles(taskFiles []string, foundPipeline *Pipeline) []assetFromFileResult {
	results := make([]assetFromFileResult, len(taskFiles))
	if len(taskFiles) == 0 {
		return results
	}

	workerCount := min(runtime.GOMAXPROCS(0), len(taskFiles))
	if workerCount <= 1 {
		for i, file := range taskFiles {
			task, err := b.CreateAssetFromFile(file, foundPipeline)
			results[i] = assetFromFileResult{task: task, err: err}
		}
		return results
	}

	jobs := make(chan int, workerCount)
	var wg sync.WaitGroup
	wg.Add(workerCount)
	for range workerCount {
		go func() {
			defer wg.Done()
			for index := range jobs {
				task, err := b.CreateAssetFromFile(taskFiles[index], foundPipeline)
				results[index] = assetFromFileResult{task: task, err: err}
			}
		}()
	}

	for index := range taskFiles {
		jobs <- index
	}
	close(jobs)
	wg.Wait()

	return results
}

// CreatePipelinesFromPath returns one *Pipeline per declared variant when the
// pipeline at pathToPipeline is a variant pipeline, or a single-element slice
// otherwise. Each returned Pipeline is fully materialized (variant variables
// merged, identity fields rendered, assets loaded) and ready for downstream use.
//
// This is the entry point external services (e.g. asset-service) should prefer:
// it makes the variant fan-out invisible to callers that don't care.
func (b *Builder) CreatePipelinesFromPath(ctx context.Context, pathToPipeline string, opts ...CreatePipelineOption) ([]*Pipeline, error) {
	cfg := createPipelineConfig{}
	for _, o := range opts {
		o(&cfg)
	}

	// Honour an explicit WithVariant by delegating to the singular form.
	if cfg.variantName != "" {
		pl, err := b.CreatePipelineFromPath(ctx, pathToPipeline, opts...)
		if err != nil {
			return nil, err
		}
		return []*Pipeline{pl}, nil
	}

	// Probe the un-materialized form to discover declared variants without
	// loading assets.
	probe, err := b.CreatePipelineFromPath(ctx, pathToPipeline, append(opts, WithOnlyPipeline(), withSkipVariantCheck())...)
	if err != nil {
		return nil, err
	}
	if len(probe.Variants) == 0 {
		pl, err := b.CreatePipelineFromPath(ctx, pathToPipeline, opts...)
		if err != nil {
			return nil, err
		}
		return []*Pipeline{pl}, nil
	}

	out := make([]*Pipeline, 0, len(probe.Variants))
	for _, name := range probe.Variants.Names() {
		pl, err := b.CreatePipelineFromPath(ctx, pathToPipeline, append(opts, WithVariant(name))...)
		if err != nil {
			return nil, fmt.Errorf("variant %q: %w", name, err)
		}
		out = append(out, pl)
	}
	return out, nil
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

	absPath, absErr := filepath.Abs(filePath)
	if absErr != nil {
		absPath = filePath
	}
	task.DefinitionFile.Name = filepath.Base(absPath)
	task.DefinitionFile.Path = absPath
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

	defaults := foundPipeline.DefaultValues
	if defaults == nil {
		return asset, nil
	}

	if asset == nil {
		return asset, nil
	}

	applyStringDefault(&asset.Description, defaults.Description)
	applyStringDefault(&asset.StartDate, defaults.StartDate)
	applyStringDefault(&asset.Connection, defaults.Connection)
	applyStringDefault(&asset.Image, defaults.Image)
	applyStringDefault(&asset.Instance, defaults.Instance)
	applyStringDefault(&asset.Owner, defaults.Owner)
	if asset.Tier == 0 && defaults.Tier != 0 {
		asset.Tier = defaults.Tier
	}
	if len(asset.Type) == 0 && len(defaults.Type) > 0 {
		asset.Type = AssetType(defaults.Type)
	}

	// merge parameters from the default values to asset parameters
	if len(asset.Parameters) == 0 {
		asset.Parameters = ParameterMap{}
	}

	for key, value := range defaults.Parameters {
		if _, exists := asset.Parameters[key]; !exists {
			asset.Parameters[key] = value
		}
	}

	mergeEmptyStringMapDefaults(&asset.Meta, defaults.Meta)
	mergeEmptyStringMapDefaults(&asset.Metadata, defaults.Metadata)

	appendMissingStringValues(&asset.Tags, defaults.Tags)
	appendMissingStringValues(&asset.Domains, defaults.Domains)
	appendMissingStringValues(&asset.Extends, defaults.Extends)
	mergeMaterializationDefaults(&asset.Materialization, defaults.Materialization)
	asset.Upstreams = appendMissingUpstreams(asset.Upstreams, defaults.Upstreams)
	mergeColumnDefaults(asset, defaults.Columns)
	mergeCustomCheckDefaults(asset, defaults.CustomChecks)

	// merge secrets from the default values to asset secrets
	existingSecrets := make(map[string]bool)
	for _, secret := range asset.Secrets {
		existingSecrets[secret.SecretKey] = true
	}
	for _, secret := range defaults.Secrets {
		secretMap := SecretMapping(secret)
		if !existingSecrets[secretMap.SecretKey] {
			asset.Secrets = append(asset.Secrets, secretMap)
		}
	}
	if (asset.IntervalModifiers.Start == TimeModifier{}) {
		asset.IntervalModifiers.Start = defaults.IntervalModifiers.Start
	}
	if (asset.IntervalModifiers.End == TimeModifier{}) {
		asset.IntervalModifiers.End = defaults.IntervalModifiers.End
	}
	acceptsDefaultHooks := assetAcceptsDefaultHooks(asset, defaults.Hooks)
	if acceptsDefaultHooks && len(asset.Hooks.Pre) == 0 {
		asset.Hooks.Pre = append([]Hook(nil), defaults.Hooks.Pre...)
	}
	if acceptsDefaultHooks && len(asset.Hooks.Post) == 0 {
		asset.Hooks.Post = append([]Hook(nil), defaults.Hooks.Post...)
	}
	if asset.Snowflake.Warehouse == "" {
		asset.Snowflake.Warehouse = defaults.Snowflake.Warehouse
	}
	if asset.Athena.Location == "" {
		asset.Athena.Location = defaults.Athena.Location
	}
	if !defaults.Routing.IsZero() {
		if asset.Routing == nil {
			asset.Routing = defaults.Routing.Clone()
		} else if asset.Routing.EgressGateway == "" {
			asset.Routing.EgressGateway = defaults.Routing.EgressGateway
		}
	}
	if asset.RerunCooldown == nil && defaults.RerunCooldown != nil {
		asset.RerunCooldown = cloneIntPtr(defaults.RerunCooldown)
	}
	if asset.Retries == nil && defaults.Retries != nil {
		asset.Retries = cloneIntPtr(defaults.Retries)
	}
	if asset.RefreshRestricted == nil && defaults.RefreshRestricted != nil {
		asset.RefreshRestricted = cloneBoolPtr(defaults.RefreshRestricted)
	}
	asset.Notifications = mergeNotificationDefaults(asset.Notifications, defaults.Notifications)

	return asset, nil
}

func applyStringDefault(target *string, defaultValue string) {
	if *target == "" && defaultValue != "" {
		*target = defaultValue
	}
}

func mergeEmptyStringMapDefaults(target *EmptyStringMap, defaults EmptyStringMap) {
	if len(defaults) == 0 {
		return
	}
	if *target == nil {
		*target = EmptyStringMap{}
	}
	for key, value := range defaults {
		if _, exists := (*target)[key]; !exists {
			(*target)[key] = value
		}
	}
}

func appendMissingStringValues[S ~[]E, E ~string](target *S, defaults S) {
	if len(defaults) == 0 {
		return
	}
	merged := append(S(nil), (*target)...)
	existing := make(map[E]bool, len(merged))
	for _, value := range merged {
		existing[value] = true
	}
	for _, value := range defaults {
		if !existing[value] {
			merged = append(merged, value)
			existing[value] = true
		}
	}
	*target = merged
}

func mergeMaterializationDefaults(target *Materialization, defaults Materialization) {
	if target.Type == "" {
		target.Type = defaults.Type
	}
	if target.Strategy == "" {
		target.Strategy = defaults.Strategy
	}
	if target.PartitionBy == "" {
		target.PartitionBy = defaults.PartitionBy
	}
	if len(target.ClusterBy) == 0 && len(defaults.ClusterBy) > 0 {
		target.ClusterBy = append([]string(nil), defaults.ClusterBy...)
	}
	if target.IncrementalKey == "" {
		target.IncrementalKey = defaults.IncrementalKey
	}
	if target.TimeGranularity == "" {
		target.TimeGranularity = defaults.TimeGranularity
	}
}

func appendMissingUpstreams(target []Upstream, defaults []Upstream) []Upstream {
	if len(defaults) == 0 {
		return target
	}
	merged := append([]Upstream(nil), target...)
	existing := make(map[string]bool, len(merged))
	for _, upstream := range merged {
		existing[upstreamIdentity(upstream)] = true
	}
	for _, upstream := range defaults {
		key := upstreamIdentity(upstream)
		if !existing[key] {
			merged = append(merged, cloneUpstream(upstream))
			existing[key] = true
		}
	}
	return merged
}

func upstreamIdentity(upstream Upstream) string {
	upstreamType := upstream.Type
	if upstreamType == "" {
		upstreamType = selectorAssetDependencyType
	}
	return upstreamType + "\x00" + upstream.Value
}

func cloneUpstream(upstream Upstream) Upstream {
	clone := upstream
	clone.Metadata = cloneEmptyStringMap(upstream.Metadata)
	clone.Columns = append([]DependsColumn(nil), upstream.Columns...)
	return clone
}

func mergeColumnDefaults(asset *Asset, defaults []Column) {
	if len(defaults) == 0 {
		return
	}
	for _, defaultColumn := range defaults {
		index := findColumnIndex(asset.Columns, defaultColumn.Name)
		if index == -1 {
			asset.Columns = append(asset.Columns, cloneColumnForAsset(defaultColumn, asset.Name))
			continue
		}
		mergeColumnDefault(&asset.Columns[index], defaultColumn, asset.Name)
	}
}

func findColumnIndex(columns []Column, name string) int {
	for index, column := range columns {
		if strings.EqualFold(column.Name, name) {
			return index
		}
	}
	return -1
}

func mergeColumnDefault(target *Column, defaults Column, assetName string) {
	if target.EntityAttribute == nil {
		target.EntityAttribute = cloneEntityAttribute(defaults.EntityAttribute)
	}
	applyStringDefault(&target.Name, defaults.Name)
	applyStringDefault(&target.SourceColumn, defaults.SourceColumn)
	applyStringDefault(&target.Type, defaults.Type)
	applyStringDefault(&target.Mask, defaults.Mask)
	applyStringDefault(&target.Description, defaults.Description)
	appendMissingStringValues(&target.Tags, defaults.Tags)
	if !target.PrimaryKey && defaults.PrimaryKey {
		target.PrimaryKey = true
	}
	if !target.UpdateOnMerge && defaults.UpdateOnMerge {
		target.UpdateOnMerge = true
	}
	applyStringDefault(&target.MergeSQL, defaults.MergeSQL)
	if target.Nullable.Value == nil && defaults.Nullable.Value != nil {
		target.Nullable = DefaultTrueBool{Value: cloneBoolPtr(defaults.Nullable.Value)}
	}
	applyStringDefault(&target.Default, defaults.Default)
	if target.Precision == nil {
		target.Precision = cloneIntPtr(defaults.Precision)
	}
	if target.Scale == nil {
		target.Scale = cloneIntPtr(defaults.Scale)
	}
	if target.Length == nil {
		target.Length = cloneIntPtr(defaults.Length)
	}
	applyStringDefault(&target.Collation, defaults.Collation)
	if target.ForeignKey == nil {
		target.ForeignKey = cloneColumnReference(defaults.ForeignKey)
	}
	applyStringDefault(&target.Owner, defaults.Owner)
	appendMissingStringValues(&target.Domains, defaults.Domains)
	mergeEmptyStringMapDefaults(&target.Meta, defaults.Meta)
	applyStringDefault(&target.Extends, defaults.Extends)
	target.Checks = mergeColumnCheckDefaults(target.Checks, defaults.Checks, assetName, target.Name)
	target.Upstreams = appendMissingColumnUpstreams(target.Upstreams, defaults.Upstreams)
}

func cloneColumnForAsset(column Column, assetName string) Column {
	clone := column
	clone.EntityAttribute = cloneEntityAttribute(column.EntityAttribute)
	clone.Tags = append(EmptyStringArray(nil), column.Tags...)
	clone.Nullable = cloneDefaultTrueBool(column.Nullable)
	clone.Precision = cloneIntPtr(column.Precision)
	clone.Scale = cloneIntPtr(column.Scale)
	clone.Length = cloneIntPtr(column.Length)
	clone.ForeignKey = cloneColumnReference(column.ForeignKey)
	clone.Domains = append(EmptyStringArray(nil), column.Domains...)
	clone.Meta = cloneEmptyStringMap(column.Meta)
	clone.Checks = cloneColumnChecksForAsset(column.Checks, assetName, column.Name)
	clone.Upstreams = cloneColumnUpstreams(column.Upstreams)
	return clone
}

func cloneEntityAttribute(value *EntityAttribute) *EntityAttribute {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneColumnReference(value *ColumnReference) *ColumnReference {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func appendMissingColumnUpstreams(target []*UpstreamColumn, defaults []*UpstreamColumn) []*UpstreamColumn {
	if len(defaults) == 0 {
		return target
	}
	merged := cloneColumnUpstreams(target)
	existing := make(map[string]bool, len(merged))
	for _, upstream := range merged {
		existing[columnUpstreamIdentity(upstream)] = true
	}
	for _, upstream := range defaults {
		key := columnUpstreamIdentity(upstream)
		if !existing[key] {
			merged = append(merged, cloneColumnUpstream(upstream))
			existing[key] = true
		}
	}
	return merged
}

func columnUpstreamIdentity(upstream *UpstreamColumn) string {
	if upstream == nil {
		return ""
	}
	return upstream.Table + "\x00" + upstream.Column
}

func cloneColumnUpstreams(upstreams []*UpstreamColumn) []*UpstreamColumn {
	if len(upstreams) == 0 {
		return upstreams
	}
	clone := make([]*UpstreamColumn, 0, len(upstreams))
	for _, upstream := range upstreams {
		clone = append(clone, cloneColumnUpstream(upstream))
	}
	return clone
}

func cloneColumnUpstream(upstream *UpstreamColumn) *UpstreamColumn {
	if upstream == nil {
		return nil
	}
	clone := *upstream
	return &clone
}

func mergeColumnCheckDefaults(target []ColumnCheck, defaults []ColumnCheck, assetName, columnName string) []ColumnCheck {
	if len(defaults) == 0 {
		return target
	}
	merged := append([]ColumnCheck(nil), target...)
	for _, defaultCheck := range defaults {
		index := findColumnCheckIndex(merged, defaultCheck.Name)
		if index == -1 {
			merged = append(merged, cloneColumnCheckForAsset(defaultCheck, assetName, columnName))
			continue
		}
		mergeColumnCheckDefault(&merged[index], defaultCheck)
	}
	return merged
}

func findColumnCheckIndex(checks []ColumnCheck, name string) int {
	for index, check := range checks {
		if strings.EqualFold(check.Name, name) {
			return index
		}
	}
	return -1
}

func mergeColumnCheckDefault(target *ColumnCheck, defaults ColumnCheck) {
	applyStringDefault(&target.Name, defaults.Name)
	if isColumnCheckValueZero(target.Value) && !isColumnCheckValueZero(defaults.Value) {
		target.Value = cloneColumnCheckValue(defaults.Value)
	}
	if target.Blocking.Value == nil && defaults.Blocking.Value != nil {
		target.Blocking = cloneDefaultTrueBool(defaults.Blocking)
	}
	applyStringDefault(&target.Description, defaults.Description)
	if target.Retries == nil {
		target.Retries = cloneIntPtr(defaults.Retries)
	}
	if target.Notifications == nil {
		target.Notifications = cloneNotifications(defaults.Notifications)
	}
}

func cloneColumnChecksForAsset(checks []ColumnCheck, assetName, columnName string) []ColumnCheck {
	if len(checks) == 0 {
		return checks
	}
	clone := make([]ColumnCheck, 0, len(checks))
	for _, check := range checks {
		clone = append(clone, cloneColumnCheckForAsset(check, assetName, columnName))
	}
	return clone
}

func cloneColumnCheckForAsset(check ColumnCheck, assetName, columnName string) ColumnCheck {
	clone := check
	clone.ID = hash(fmt.Sprintf("%s-%s-%s", assetName, columnName, check.Name))
	clone.Value = cloneColumnCheckValue(check.Value)
	clone.Blocking = cloneDefaultTrueBool(check.Blocking)
	clone.Retries = cloneIntPtr(check.Retries)
	clone.Notifications = cloneNotifications(check.Notifications)
	return clone
}

func mergeCustomCheckDefaults(asset *Asset, defaults []CustomCheck) {
	if len(defaults) == 0 {
		return
	}
	for _, defaultCheck := range defaults {
		index := findCustomCheckIndex(asset.CustomChecks, defaultCheck.Name)
		if index == -1 {
			asset.CustomChecks = append(asset.CustomChecks, cloneCustomCheckForAsset(defaultCheck, asset.Name))
			continue
		}
		mergeCustomCheckDefault(&asset.CustomChecks[index], defaultCheck)
	}
}

func findCustomCheckIndex(checks []CustomCheck, name string) int {
	for index, check := range checks {
		if strings.EqualFold(check.Name, name) {
			return index
		}
	}
	return -1
}

func mergeCustomCheckDefault(target *CustomCheck, defaults CustomCheck) {
	applyStringDefault(&target.Name, defaults.Name)
	applyStringDefault(&target.Description, defaults.Description)
	if target.Value == 0 && defaults.Value != 0 {
		target.Value = defaults.Value
	}
	if target.Count == nil {
		target.Count = cloneInt64Ptr(defaults.Count)
	}
	if target.Blocking.Value == nil && defaults.Blocking.Value != nil {
		target.Blocking = cloneDefaultTrueBool(defaults.Blocking)
	}
	applyStringDefault(&target.Query, defaults.Query)
	if target.Retries == nil {
		target.Retries = cloneIntPtr(defaults.Retries)
	}
	if target.Notifications == nil {
		target.Notifications = cloneNotifications(defaults.Notifications)
	}
}

func cloneCustomCheckForAsset(check CustomCheck, assetName string) CustomCheck {
	clone := check
	clone.ID = hash(fmt.Sprintf("%s-%s", assetName, check.Name))
	clone.Count = cloneInt64Ptr(check.Count)
	clone.Blocking = cloneDefaultTrueBool(check.Blocking)
	clone.Retries = cloneIntPtr(check.Retries)
	clone.Notifications = cloneNotifications(check.Notifications)
	return clone
}

func mergeNotificationDefaults(target *Notifications, defaults *Notifications) *Notifications {
	if defaults == nil {
		return target
	}
	if target == nil {
		return cloneNotifications(defaults)
	}
	merged := cloneNotifications(target)
	merged.Slack = appendMissingSlackNotifications(merged.Slack, defaults.Slack)
	merged.MSTeams = appendMissingMSTeamsNotifications(merged.MSTeams, defaults.MSTeams)
	merged.Discord = appendMissingDiscordNotifications(merged.Discord, defaults.Discord)
	merged.Webhook = appendMissingWebhookNotifications(merged.Webhook, defaults.Webhook)
	return merged
}

func appendMissingSlackNotifications(target []SlackNotification, defaults []SlackNotification) []SlackNotification {
	merged := append([]SlackNotification(nil), target...)
	existing := make(map[string]bool, len(merged))
	for _, notification := range merged {
		existing[notification.Channel] = true
	}
	for _, notification := range defaults {
		if !existing[notification.Channel] {
			merged = append(merged, cloneSlackNotification(notification))
			existing[notification.Channel] = true
		}
	}
	return merged
}

func appendMissingMSTeamsNotifications(target []MSTeamsNotification, defaults []MSTeamsNotification) []MSTeamsNotification {
	merged := append([]MSTeamsNotification(nil), target...)
	existing := make(map[string]bool, len(merged))
	for _, notification := range merged {
		existing[notification.Connection] = true
	}
	for _, notification := range defaults {
		if !existing[notification.Connection] {
			merged = append(merged, cloneMSTeamsNotification(notification))
			existing[notification.Connection] = true
		}
	}
	return merged
}

func appendMissingDiscordNotifications(target []DiscordNotification, defaults []DiscordNotification) []DiscordNotification {
	merged := append([]DiscordNotification(nil), target...)
	existing := make(map[string]bool, len(merged))
	for _, notification := range merged {
		existing[notification.Connection] = true
	}
	for _, notification := range defaults {
		if !existing[notification.Connection] {
			merged = append(merged, cloneDiscordNotification(notification))
			existing[notification.Connection] = true
		}
	}
	return merged
}

func appendMissingWebhookNotifications(target []WebhookNotification, defaults []WebhookNotification) []WebhookNotification {
	merged := append([]WebhookNotification(nil), target...)
	existing := make(map[string]bool, len(merged))
	for _, notification := range merged {
		existing[notification.Connection] = true
	}
	for _, notification := range defaults {
		if !existing[notification.Connection] {
			merged = append(merged, cloneWebhookNotification(notification))
			existing[notification.Connection] = true
		}
	}
	return merged
}

func cloneEmptyStringMap(value EmptyStringMap) EmptyStringMap {
	if value == nil {
		return nil
	}
	clone := make(EmptyStringMap, len(value))
	for key, mapValue := range value {
		clone[key] = mapValue
	}
	return clone
}

func cloneDefaultTrueBool(value DefaultTrueBool) DefaultTrueBool {
	return DefaultTrueBool{Value: cloneBoolPtr(value.Value)}
}

func cloneColumnCheckValue(value ColumnCheckValue) ColumnCheckValue {
	clone := ColumnCheckValue{}
	if value.IntArray != nil {
		clone.IntArray = ptrTo(append([]int(nil), (*value.IntArray)...))
	}
	if value.Int != nil {
		clone.Int = cloneIntPtr(value.Int)
	}
	if value.Float != nil {
		floatValue := *value.Float
		clone.Float = &floatValue
	}
	if value.StringArray != nil {
		clone.StringArray = ptrTo(append([]string(nil), (*value.StringArray)...))
	}
	if value.String != nil {
		stringValue := *value.String
		clone.String = &stringValue
	}
	if value.Bool != nil {
		clone.Bool = cloneBoolPtr(value.Bool)
	}
	return clone
}

func isColumnCheckValueZero(value ColumnCheckValue) bool {
	return value.IntArray == nil && value.Int == nil && value.Float == nil && value.StringArray == nil && value.String == nil && value.Bool == nil
}

func ptrTo[T any](value T) *T {
	return &value
}

func cloneIntPtr(value *int) *int {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneInt64Ptr(value *int64) *int64 {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneNotifications(value *Notifications) *Notifications {
	if value == nil {
		return nil
	}
	clone := &Notifications{
		Slack:   make([]SlackNotification, 0, len(value.Slack)),
		MSTeams: make([]MSTeamsNotification, 0, len(value.MSTeams)),
		Discord: make([]DiscordNotification, 0, len(value.Discord)),
		Webhook: make([]WebhookNotification, 0, len(value.Webhook)),
	}
	for _, notification := range value.Slack {
		clone.Slack = append(clone.Slack, cloneSlackNotification(notification))
	}
	for _, notification := range value.MSTeams {
		clone.MSTeams = append(clone.MSTeams, cloneMSTeamsNotification(notification))
	}
	for _, notification := range value.Discord {
		clone.Discord = append(clone.Discord, cloneDiscordNotification(notification))
	}
	for _, notification := range value.Webhook {
		clone.Webhook = append(clone.Webhook, cloneWebhookNotification(notification))
	}
	return clone
}

func cloneSlackNotification(value SlackNotification) SlackNotification {
	value.Success = cloneDefaultTrueBool(value.Success)
	value.Failure = cloneDefaultTrueBool(value.Failure)
	return value
}

func cloneMSTeamsNotification(value MSTeamsNotification) MSTeamsNotification {
	value.Success = cloneDefaultTrueBool(value.Success)
	value.Failure = cloneDefaultTrueBool(value.Failure)
	return value
}

func cloneDiscordNotification(value DiscordNotification) DiscordNotification {
	value.Success = cloneDefaultTrueBool(value.Success)
	value.Failure = cloneDefaultTrueBool(value.Failure)
	return value
}

func cloneWebhookNotification(value WebhookNotification) WebhookNotification {
	value.Success = cloneDefaultTrueBool(value.Success)
	value.Failure = cloneDefaultTrueBool(value.Failure)
	return value
}

func assetAcceptsDefaultHooks(asset *Asset, defaults Hooks) bool {
	if !asset.IsSQLAsset() {
		return false
	}
	// When no applicable_type filter is set, every SQL asset inherits the defaults.
	if len(defaults.ApplicableTypes) == 0 {
		return true
	}
	return slices.Contains(defaults.ApplicableTypes, string(asset.Type))
}

func (b *Builder) InjectConnectionAsSecret(ctx context.Context, asset *Asset, foundPipeline *Pipeline) (*Asset, error) {
	if asset == nil || asset.Connection == "" {
		return asset, nil
	}

	for _, secret := range asset.Secrets {
		if secret.SecretKey == asset.Connection {
			return asset, nil
		}
	}

	asset.Secrets = append(asset.Secrets, SecretMapping{
		SecretKey:   asset.Connection,
		InjectedKey: asset.Connection,
	})

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
					Checks:    make([]ColumnCheck, 0),
					Upstreams: make([]*UpstreamColumn, 0),
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
	return IsSQLAssetType(a.Type)
}

func IsSQLAssetType(t AssetType) bool {
	switch t {
	case AssetTypeBigqueryQuery,
		AssetTypeSnowflakeQuery,
		AssetTypePostgresQuery,
		AssetTypeRedshiftQuery,
		AssetTypeMsSQLQuery,
		AssetTypeVerticaQuery,
		AssetTypeDatabricksQuery,
		AssetTypeSynapseQuery,
		AssetTypeFabricQuery,
		AssetTypeFabricQueryLegacy,
		AssetTypeAthenaQuery,
		AssetTypeDuckDBQuery,
		AssetTypeClickHouse,
		AssetTypeTrinoQuery,
		AssetTypeDremioQuery,
		AssetTypeSailQuery,
		AssetTypeOracleQuery:
		return true
	default:
		return false
	}
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
