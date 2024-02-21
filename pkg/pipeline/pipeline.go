package pipeline

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"reflect"
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
	AssetTypeEmpty                = AssetType("empty")
	AssetTypePostgresQuery        = AssetType("pg.sql")
	AssetTypeRedshiftQuery        = AssetType("rs.sql")
	AssetTypeMsSQLQuery           = AssetType("ms.sql")
	AssetTypeSynapseQuery         = AssetType("synapse.sql")
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

type ColumnCheck struct {
	ID    string           `json:"id"`
	Name  string           `json:"name"`
	Value ColumnCheckValue `json:"value"`
}

func NewColumnCheck(assetName, columnName, name string, value ColumnCheckValue) ColumnCheck {
	return ColumnCheck{
		ID:    hash(fmt.Sprintf("%s-%s-%s", assetName, columnName, name)),
		Name:  strings.TrimSpace(name),
		Value: value,
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

type AssetType string

var assetTypeConnectionMapping = map[AssetType][]string{
	AssetTypeBigqueryQuery:        {"google_cloud_platform", "gcp"},
	AssetTypeSnowflakeQuery:       {"snowflake", "sf"},
	AssetTypeSnowflakeQuerySensor: {"snowflake", "sf"},
	AssetTypePostgresQuery:        {"postgres", "pg"},
	AssetTypeRedshiftQuery:        {"redshift", "rs"},
	AssetTypeMsSQLQuery:           {"mssql", "ms"},
	AssetTypeSynapseQuery:         {"synapse", "sy"},
}

type SecretMapping struct {
	SecretKey   string `json:"secret_key"`
	InjectedKey string `json:"injected_key"`
}

type CustomCheck struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Query string `json:"query"`
	Value int64  `json:"value"`
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

func (p *Pipeline) GetConnectionNameForAsset(asset *Asset) string {
	if asset.Connection != "" {
		return asset.Connection
	}

	mappings := assetTypeConnectionMapping[asset.Type]
	if mappings == nil {
		return ""
	}

	for _, mapping := range mappings {
		if p.DefaultConnections[mapping] != "" {
			return p.DefaultConnections[mapping]
		}
	}

	return ""
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

	var pipeline Pipeline
	err := path.ReadYaml(b.fs, pipelineFilePath, &pipeline)
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
