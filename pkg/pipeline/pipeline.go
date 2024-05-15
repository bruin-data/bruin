package pipeline

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

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
	AssetTypeMsSQLQuery           = AssetType("ms.sql")
	AssetTypeSynapseQuery         = AssetType("synapse.sql")
	AssetTypeIngestr              = AssetType("ingestr")
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
	Slack []SlackNotification `json:"slack"`
}

type SlackNotification struct {
	Channel string `json:"channel"`
}

func (n Notifications) MarshalJSON() ([]byte, error) {
	slack := make([]SlackNotification, 0, len(n.Slack))
	for _, s := range n.Slack {
		if !reflect.ValueOf(s).IsZero() {
			slack = append(slack, s)
		}
	}

	return json.Marshal(struct {
		Slack []SlackNotification `json:"slack"`
	}{
		Slack: slack,
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
	Type           MaterializationType     `json:"type"`
	Strategy       MaterializationStrategy `json:"strategy"`
	PartitionBy    string                  `json:"partition_by"`
	ClusterBy      []string                `json:"cluster_by"`
	IncrementalKey string                  `json:"incremental_key"`
}

func (m Materialization) MarshalJSON() ([]byte, error) {
	if m.Type == "" && m.Strategy == "" && m.PartitionBy == "" && len(m.ClusterBy) == 0 && m.IncrementalKey == "" {
		return json.Marshal(nil)
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

	return json.Marshal(nil)
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
	ID       string           `json:"id"`
	Name     string           `json:"name"`
	Value    ColumnCheckValue `json:"value"`
	Blocking bool             `json:"blocking"`
}

func NewColumnCheck(assetName, columnName, name string, value ColumnCheckValue, blocking bool) ColumnCheck {
	return ColumnCheck{
		ID:       hash(fmt.Sprintf("%s-%s-%s", assetName, columnName, name)),
		Name:     strings.TrimSpace(name),
		Value:    value,
		Blocking: blocking,
	}
}

type Column struct {
	Name          string        `json:"name"`
	Type          string        `json:"type"`
	Description   string        `json:"description"`
	Checks        []ColumnCheck `json:"checks"`
	PrimaryKey    bool          `json:"primary_key"`
	UpdateOnMerge bool          `json:"update_on_merge"`
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
	AssetTypeSynapseQuery:         "synapse",
}

var IngestrTypeConnectionMapping = map[string]AssetType{
	"bigquery":  AssetTypeBigqueryQuery,
	"snowflake": AssetTypeSnowflakeQuery,
	"postgres":  AssetTypePostgresQuery,
	"redshift":  AssetTypeRedshiftQuery,
	"mssql":     AssetTypeMsSQLQuery,
	"synapse":   AssetTypeSynapseQuery,
}

type SecretMapping struct {
	SecretKey   string `json:"secret_key"`
	InjectedKey string `json:"injected_key"`
}

type CustomCheck struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Query    string `json:"query"`
	Value    int64  `json:"value"`
	Blocking bool   `json:"blocking"`
}

type Asset struct {
	ID              string             `json:"id"`
	Name            string             `json:"name"`
	Description     string             `json:"description"`
	Type            AssetType          `json:"type"`
	ExecutableFile  ExecutableFile     `json:"executable_file"`
	DefinitionFile  TaskDefinitionFile `json:"definition_file"`
	Parameters      EmptyStringMap     `json:"parameters"`
	Connection      string             `json:"connection"`
	Secrets         []SecretMapping    `json:"secrets"`
	DependsOn       []string           `json:"upstream"`
	Materialization Materialization    `json:"materialization"`
	Columns         []Column           `json:"columns"`
	CustomChecks    []CustomCheck      `json:"custom_checks"`
	Image           string             `json:"image"`
	Instance        string             `json:"instance"`
	Owner           string             `json:"owner"`
	Metadata        EmptyStringMap     `json:"metadata"`

	Pipeline *Pipeline `json:"-"`

	upstream   []*Asset
	downstream []*Asset
}

func (a *Asset) AddUpstream(asset *Asset) {
	a.upstream = append(a.upstream, asset)
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
		return json.Marshal(map[string]string{})
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

type AssetCollection []*Asset

func (ac AssetCollection) MarshalJSON() ([]byte, error) {
	if ac == nil {
		return []byte("[]"), nil
	}

	return json.Marshal([]*Asset(ac))
}

type Pipeline struct {
	LegacyID           string          `yaml:"id" json:"legacy_id"`
	Name               string          `yaml:"name" json:"name"`
	Schedule           schedule        `yaml:"schedule" json:"schedule"`
	StartDate          string          `yaml:"start_date" json:"start_date"`
	DefinitionFile     DefinitionFile  `json:"definition_file"`
	DefaultParameters  EmptyStringMap  `yaml:"default_parameters" json:"default_parameters"`
	DefaultConnections EmptyStringMap  `yaml:"default_connections" json:"default_connections"`
	Assets             AssetCollection `json:"assets"`
	Notifications      Notifications   `yaml:"notifications" json:"notifications"`
	Catchup            bool            `yaml:"catchup" json:"catchup"`
	Retries            int             `yaml:"retries" json:"retries"`

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

func (p *Pipeline) GetMajorityAssetTypesFromSQLAssets(defaultIfNone AssetType) AssetType {
	taskTypeCounts := map[AssetType]int{
		AssetTypeBigqueryQuery:  0,
		AssetTypeSnowflakeQuery: 0,
		AssetTypePostgresQuery:  0,
		AssetTypeMsSQLQuery:     0,
		AssetTypeRedshiftQuery:  0,
		AssetTypeSynapseQuery:   0,
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

type TaskCreator func(path string) (*Asset, error)

type BuilderConfig struct {
	PipelineFileName    string
	TasksDirectoryName  string
	TasksDirectoryNames []string
	TasksFileSuffixes   []string
}

type builder struct {
	config             BuilderConfig
	yamlTaskCreator    TaskCreator
	commentTaskCreator TaskCreator
	fs                 afero.Fs
}

type ParseError struct {
	Msg string
}

func (e *ParseError) Error() string {
	return e.Msg
}

func NewBuilder(config BuilderConfig, yamlTaskCreator TaskCreator, commentTaskCreator TaskCreator, fs afero.Fs) *builder {
	return &builder{
		config:             config,
		yamlTaskCreator:    yamlTaskCreator,
		commentTaskCreator: commentTaskCreator,
		fs:                 fs,
	}
}

func (b *builder) CreatePipelineFromPath(pathToPipeline string) (*Pipeline, error) {
	pipelineFilePath := pathToPipeline
	if !strings.HasSuffix(pipelineFilePath, b.config.PipelineFileName) {
		pipelineFilePath = filepath.Join(pathToPipeline, b.config.PipelineFileName)
	} else {
		pathToPipeline = filepath.Dir(pathToPipeline)
	}
	yamlError := new(path.YamlParseError)
	var pipeline Pipeline
	err := path.ReadYaml(b.fs, pipelineFilePath, &pipeline)
	if err != nil && errors.As(err, &yamlError) {
		return nil, &ParseError{Msg: fmt.Sprintf("error parsing pipeline file at '%s':%s", pipelineFilePath, err.Error())}
	}
	if err != nil {
		return nil, errors.Wrapf(err, "error reading pipeline file at '%s'", pipelineFilePath)
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

	for _, asset := range pipeline.Assets {
		for _, upstream := range asset.DependsOn {
			u, ok := pipeline.tasksByName[upstream]
			if !ok {
				continue
			}

			asset.AddUpstream(u)
			u.AddDownstream(asset)
		}
	}

	return &pipeline, nil
}

func fileHasSuffix(arr []string, str string) bool {
	for _, a := range arr {
		if strings.HasSuffix(str, a) {
			return true
		}
	}
	return false
}

func (b *builder) CreateAssetFromFile(path string) (*Asset, error) {
	isSeparateDefinitionFile := false
	creator := b.commentTaskCreator

	if fileHasSuffix(b.config.TasksFileSuffixes, path) {
		creator = b.yamlTaskCreator
		isSeparateDefinitionFile = true
	}

	task, err := creator(path)
	if err != nil {
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
