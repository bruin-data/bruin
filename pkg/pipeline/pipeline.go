package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

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
	AssetTypeSnowflakeQuerySensor = AssetType("sf.sensor.query")
	AssetTypeBigqueryQuery        = AssetType("bq.sql")
	AssetTypeBigqueryTableSensor  = AssetType("bq.sensor.table")
	AssetTypeBigqueryQuerySensor  = AssetType("bq.sensor.query")
	AssetTypeEmpty                = AssetType("empty")
	AssetTypePostgresQuery        = AssetType("pg.sql")
	AssetTypeRedshiftQuery        = AssetType("rs.sql")
	AssetTypeAthenaQuery          = AssetType("athena.sql")
	AssetTypeMsSQLQuery           = AssetType("ms.sql")
	AssetTypeDatabricksQuery      = AssetType("databricks.sql")
	AssetTypeSynapseQuery         = AssetType("synapse.sql")
	AssetTypeIngestr              = AssetType("ingestr")

	RunConfigFullRefresh = RunConfig("full-refresh")
	RunConfigStartDate   = RunConfig("start-date")
	RunConfigEndDate     = RunConfig("end-date")
)

var supportedFileSuffixes = []string{".yml", ".yaml", ".sql", ".py"}

type (
	schedule           string
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
	Slack   []SlackNotification   `yaml:"slack" json:"slack"`
	MSTeams []MSTeamsNotification `yaml:"ms_teams" json:"ms_teams"`
}

type DefaultTrueBool struct {
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
	if b.Value == nil {
		return nil, nil
	}

	return *b.Value, nil
}

type NotificationCommon struct {
	Success DefaultTrueBool `yaml:"success" json:"success"`
	Failure DefaultTrueBool `yaml:"failure" json:"failure"`
}

type SlackNotification struct {
	Channel            string `json:"channel"`
	NotificationCommon `yaml:",inline" json:",inline"`
}

type MSTeamsNotification struct {
	Connection         string `yaml:"connection" json:"connection"`
	NotificationCommon `yaml:",inline" json:",inline"`
}

func (n Notifications) MarshalJSON() ([]byte, error) {
	slack := make([]SlackNotification, 0, len(n.Slack))
	MSTeams := make([]MSTeamsNotification, 0, len(n.MSTeams))
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

	return json.Marshal(struct {
		Slack   []SlackNotification   `json:"slack"`
		MSTeams []MSTeamsNotification `json:"ms_teams"`
	}{
		Slack:   slack,
		MSTeams: MSTeams,
	})
}

type MaterializationType string

const (
	MaterializationTypeNone  MaterializationType = ""
	MaterializationTypeView  MaterializationType = "view"
	MaterializationTypeTable MaterializationType = "table"
)

type MaterializationStrategy string

const (
	MaterializationStrategyNone          MaterializationStrategy = ""
	MaterializationStrategyCreateReplace MaterializationStrategy = "create+replace"
	MaterializationStrategyDeleteInsert  MaterializationStrategy = "delete+insert"
	MaterializationStrategyAppend        MaterializationStrategy = "append"
	MaterializationStrategyMerge         MaterializationStrategy = "merge"
)

var AllAvailableMaterializationStrategies = []MaterializationStrategy{
	MaterializationStrategyCreateReplace,
	MaterializationStrategyDeleteInsert,
	MaterializationStrategyAppend,
	MaterializationStrategyMerge,
}

type Materialization struct {
	Type           MaterializationType     `json:"type" yaml:"type,omitempty"`
	Strategy       MaterializationStrategy `json:"strategy" yaml:"strategy,omitempty"`
	PartitionBy    string                  `json:"partition_by" yaml:"partition_by,omitempty"`
	ClusterBy      []string                `json:"cluster_by" yaml:"cluster_by,omitempty"`
	IncrementalKey string                  `json:"incremental_key" yaml:"incremental_key,omitempty"`
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
	if ccv.IntArray != nil {
		return json.Marshal(ccv.IntArray)
	}
	if ccv.Int != nil {
		return json.Marshal(ccv.Int)
	}
	if ccv.Float != nil {
		return json.Marshal(ccv.Float)
	}
	if ccv.StringArray != nil {
		return json.Marshal(ccv.StringArray)
	}
	if ccv.String != nil {
		return json.Marshal(ccv.String)
	}
	if ccv.Bool != nil {
		return json.Marshal(ccv.Bool)
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
	ID       string           `json:"id" yaml:"-"`
	Name     string           `json:"name" yaml:"name,omitempty"`
	Value    ColumnCheckValue `json:"value" yaml:"value,omitempty"`
	Blocking DefaultTrueBool  `json:"blocking" yaml:"blocking,omitempty"`
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

type Column struct {
	EntityAttribute *EntityAttribute `json:"entity_attribute" yaml:"-"`
	Name            string           `json:"name" yaml:"name,omitempty"`
	Type            string           `json:"type" yaml:"type,omitempty"`
	Description     string           `json:"description" yaml:"description,omitempty"`
	Checks          []ColumnCheck    `json:"checks" yaml:"checks,omitempty"`
	PrimaryKey      bool             `json:"primary_key" yaml:"primary_key,omitempty"`
	UpdateOnMerge   bool             `json:"update_on_merge" yaml:"update_on_merge,omitempty"`
	Extends         string           `json:"-" yaml:"extends,omitempty"`
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
	AssetTypeSnowflakeQuery:       "snowflake",
	AssetTypeSnowflakeQuerySensor: "snowflake",
	AssetTypePostgresQuery:        "postgres",
	AssetTypeRedshiftQuery:        "redshift",
	AssetTypeMsSQLQuery:           "mssql",
	AssetTypeDatabricksQuery:      "databricks",
	AssetTypeSynapseQuery:         "synapse",
	AssetTypeAthenaQuery:          "athena",
}

var IngestrTypeConnectionMapping = map[string]AssetType{
	"bigquery":   AssetTypeBigqueryQuery,
	"snowflake":  AssetTypeSnowflakeQuery,
	"postgres":   AssetTypePostgresQuery,
	"redshift":   AssetTypeRedshiftQuery,
	"mssql":      AssetTypeMsSQLQuery,
	"databricks": AssetTypeDatabricksQuery,
	"synapse":    AssetTypeSynapseQuery,
}

type SecretMapping struct {
	SecretKey   string `json:"secret_key"`
	InjectedKey string `json:"injected_key"`
}

type CustomCheck struct {
	ID          string          `json:"id" yaml:"-"`
	Name        string          `json:"name" yaml:"name"`
	Description string          `json:"description" yaml:"description,omitempty"`
	Query       string          `json:"query" yaml:"query"`
	Value       int64           `json:"value" yaml:"value,omitempty"`
	Blocking    DefaultTrueBool `json:"blocking" yaml:"blocking,omitempty"`
}

type Upstream struct {
	Type     string         `json:"type" yaml:"type"`
	Value    string         `json:"value" yaml:"value"`
	Metadata EmptyStringMap `json:"metadata,omitempty" yaml:"metadata,omitempty"`
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

type AthenaConfig struct {
	Location string `json:"location"`
}

func (s SnowflakeConfig) MarshalJSON() ([]byte, error) {
	if s.Warehouse == "" {
		return []byte("null"), nil
	}

	return json.Marshal(s)
}

func (s AthenaConfig) MarshalJSON() ([]byte, error) {
	if s.Location == "" {
		return []byte("null"), nil
	}

	return json.Marshal(s)
}

type Asset struct {
	ID              string             `json:"id" yaml:"-"`
	URI             string             `json:"uri" yaml:"uri,omitempty"`
	Name            string             `json:"name" yaml:"name,omitempty"`
	Description     string             `json:"description" yaml:"description,omitempty"`
	Type            AssetType          `json:"type" yaml:"type,omitempty"`
	ExecutableFile  ExecutableFile     `json:"executable_file" yaml:"-"`
	DefinitionFile  TaskDefinitionFile `json:"definition_file" yaml:"-"`
	Parameters      EmptyStringMap     `json:"parameters" yaml:"parameters,omitempty"`
	Connection      string             `json:"connection" yaml:"connection,omitempty"`
	Secrets         []SecretMapping    `json:"secrets" yaml:"secrets,omitempty"`
	Materialization Materialization    `json:"materialization" yaml:"materialization,omitempty"`
	Columns         []Column           `json:"columns" yaml:"columns,omitempty"`
	CustomChecks    []CustomCheck      `json:"custom_checks" yaml:"custom_checks,omitempty"`
	Image           string             `json:"image" yaml:"image,omitempty"`
	Instance        string             `json:"instance" yaml:"instance,omitempty"`
	Owner           string             `json:"owner" yaml:"owner,omitempty"`
	Metadata        EmptyStringMap     `json:"metadata" yaml:"metadata,omitempty"`
	Tags            EmptyStringArray   `json:"tags" yaml:"tags,omitempty"`
	Snowflake       SnowflakeConfig    `json:"snowflake" yaml:"snowflake,omitempty"`
	Athena          AthenaConfig       `json:"athena" yaml:"athena,omitempty"`

	upstream   []*Asset
	downstream []*Asset
	Upstreams  []Upstream `json:"upstreams" yaml:"depends,omitempty"`
}

func (a *Asset) AddUpstream(asset *Asset) {
	a.upstream = append(a.upstream, asset)
}

// removeRedundanciesBeforePersisting aims to remove unnecessary configuration from the asset.
// This is particularly useful when we save a formatted version of the asset itself.
func (a *Asset) removeRedundanciesBeforePersisting() {
	a.clearDuplicateUpstreams()
	if a.Type == AssetTypePython {
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
		if c.Name == name {
			return &c
		}
	}

	return nil
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

	a.removeRedundanciesBeforePersisting()

	yamlConfig, err := yaml.Marshal(a)
	if err != nil {
		return err
	}

	keysToAddSpace := []string{"custom_checks", "depends", "columns", "materialization"}
	for _, key := range keysToAddSpace {
		yamlConfig = bytes.ReplaceAll(yamlConfig, []byte("\n"+key+":"), []byte("\n\n"+key+":"))
	}

	filePath := a.ExecutableFile.Path
	beginning := ""
	end := ""
	executableContent := ""

	if strings.HasSuffix(a.ExecutableFile.Path, ".sql") {
		// we are dealing with a SQL asset with an embedded YAML block, treat accordingly.
		beginning = "/* " + configMarkerString + "\n\n"
		end = "\n" + configMarkerString + " */" + "\n\n"
		executableContent = a.ExecutableFile.Content
	}

	if strings.HasSuffix(a.ExecutableFile.Path, ".py") {
		// we are dealing with a Python asset with an embedded YAML block, treat accordingly.
		beginning = `""" ` + configMarkerString + "\n\n"
		end = "\n" + configMarkerString + ` """` + "\n\n"
		executableContent = a.ExecutableFile.Content
	}

	stringVersion := beginning + string(yamlConfig) + end + executableContent
	if !strings.HasSuffix(stringVersion, "\n") {
		stringVersion += "\n"
	}

	return afero.WriteFile(fs, filePath, []byte(stringVersion), 0o644)
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
	return &pl, nil
}

type Pipeline struct {
	LegacyID           string         `yaml:"id" json:"legacy_id"`
	Name               string         `yaml:"name" json:"name"`
	Schedule           schedule       `yaml:"schedule" json:"schedule"`
	StartDate          string         `yaml:"start_date" json:"start_date"`
	DefinitionFile     DefinitionFile `json:"definition_file"`
	DefaultParameters  EmptyStringMap `yaml:"default_parameters" json:"default_parameters"`
	DefaultConnections EmptyStringMap `yaml:"default_connections" json:"default_connections"`
	Assets             []*Asset       `json:"assets"`
	Notifications      Notifications  `yaml:"notifications" json:"notifications"`
	Catchup            bool           `yaml:"catchup" json:"catchup"`
	Retries            int            `yaml:"retries" json:"retries"`

	TasksByType map[AssetType][]*Asset `json:"-"`
	tasksByName map[string]*Asset
}

func (p *Pipeline) GetConnectionNameForAsset(asset *Asset) (string, error) {
	if asset.Connection != "" {
		return asset.Connection, nil
	}

	assetType := asset.Type
	if assetType == AssetTypeIngestr {
		assetType = IngestrTypeConnectionMapping[asset.Parameters["destination"]]
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

	switch mapping {
	case "aws":
		return "aws-default", nil
	case "athena":
		return "athena-default", nil
	case "gcp":
		return "gcp-default", nil
	case "google_cloud_platform":
		return "gcp-default", nil
	case "snowflake":
		return "snowflake-default", nil
	case "postgres":
		return "postgres-default", nil
	case "redshift":
		return "redshift-default", nil
	case "mssql":
		return "mssql-default", nil
	case "databricks":
		return "databricks-default", nil
	case "synapse":
		return "synapse-default", nil
	case "mongo":
		return "mongo-default", nil
	case "mysql":
		return "mysql-default", nil
	case "notion":
		return "notion-default", nil
	case "hana":
		return "hana-default", nil
	default:
		return "", errors.Errorf("no default connection found for type '%s'", assetType)
	}
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
	PipelineFileName    string
	TasksDirectoryName  string
	TasksDirectoryNames []string
	TasksFileSuffixes   []string
}

type glossaryReader interface {
	GetEntities(pathToPipeline string) ([]*glossary.Entity, error)
}

type Builder struct {
	config             BuilderConfig
	yamlTaskCreator    TaskCreator
	commentTaskCreator TaskCreator
	fs                 afero.Fs

	GlossaryReader glossaryReader
}

func (b *Builder) SetGlossaryReader(reader glossaryReader) {
	b.GlossaryReader = reader
}

type ParseError struct {
	Msg string
}

func (e ParseError) Error() string {
	return e.Msg
}

func NewBuilder(config BuilderConfig, yamlTaskCreator TaskCreator, commentTaskCreator TaskCreator, fs afero.Fs, gr glossaryReader) *Builder {
	return &Builder{
		config:             config,
		yamlTaskCreator:    yamlTaskCreator,
		commentTaskCreator: commentTaskCreator,
		fs:                 fs,
		GlossaryReader:     gr,
	}
}

func (b *Builder) CreatePipelineFromPath(pathToPipeline string) (*Pipeline, error) {
	pipelineFilePath := pathToPipeline
	if !strings.HasSuffix(pipelineFilePath, b.config.PipelineFileName) {
		pipelineFilePath = filepath.Join(pathToPipeline, b.config.PipelineFileName)
	} else {
		pathToPipeline = filepath.Dir(pathToPipeline)
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
		files, err := path.GetAllFilesRecursive(tasksPath, supportedFileSuffixes)
		if err != nil {
			continue
		}

		taskFiles = append(taskFiles, files...)
	}

	for _, file := range taskFiles {
		task, err := b.CreateAssetFromFile(file)
		if err != nil {
			return nil, err
		}

		if task == nil {
			continue
		}

		// if the definition comes from a Python file the asset is always a Python asset, so force it
		// at least that's the hypothesis for now
		if strings.HasSuffix(task.ExecutableFile.Path, ".py") && task.Type == "" {
			task.Type = AssetTypePython
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

func (b *Builder) CreateAssetFromFile(path string) (*Asset, error) {
	isSeparateDefinitionFile := false
	creator := b.commentTaskCreator

	if fileHasSuffix(b.config.TasksFileSuffixes, path) {
		creator = b.yamlTaskCreator
		isSeparateDefinitionFile = true
	}

	task, err := creator(path)
	if err != nil {
		if errors.As(err, &ParseError{}) {
			return nil, err
		}

		return nil, errors.Wrapf(err, "error creating task from file '%s'", path)
	}

	if task == nil {
		return nil, nil
	}

	task.DefinitionFile.Name = filepath.Base(path)
	task.DefinitionFile.Path = path
	task.DefinitionFile.Type = CommentTask
	if isSeparateDefinitionFile {
		task.DefinitionFile.Type = YamlTask
	}

	return task, nil
}
