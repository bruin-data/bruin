package pipeline

import (
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

const (
	CommentTask TaskDefinitionType = "comment"
	YamlTask    TaskDefinitionType = "yaml"

	AssetTypePython         = AssetType("python")
	AssetTypeSnowflakeQuery = AssetType("sf.sql")
	AssetTypeBigqueryQuery  = AssetType("bq.sql")
	AssetTypeEmpty          = AssetType("empty")
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
	Name       string `json:"name"`
	Connection string `json:"connection"`
	Success    string `json:"success"`
	Failure    string `json:"failure"`
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
)

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

func (v *ColumnCheckValue) MarshalJSON() ([]byte, error) {
	if v.IntArray != nil {
		return json.Marshal(v.IntArray)
	}
	if v.Int != nil {
		return json.Marshal(v.Int)
	}
	if v.Float != nil {
		return json.Marshal(v.Float)
	}
	if v.StringArray != nil {
		return json.Marshal(v.StringArray)
	}
	if v.String != nil {
		return json.Marshal(v.String)
	}
	if v.Bool != nil {
		return json.Marshal(v.Bool)
	}

	return json.Marshal(nil)
}

type ColumnCheck struct {
	Name  string           `json:"name"`
	Value ColumnCheckValue `json:"value"`
}

type Column struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Checks      []ColumnCheck `json:"checks"`
}

type AssetType string

var assetTypeConnectionMapping = map[AssetType][]string{
	AssetType("bq.sql"): {"google_cloud_platform", "gcp"},
	AssetType("sf.sql"): {"snowflake", "sf"},
}

type SecretMapping struct {
	SecretKey   string `json:"secret_key"`
	InjectedKey string `json:"injected_key"`
}

type CustomCheck struct {
	Name  string           `json:"name"`
	Query string           `json:"query"`
	Value ColumnCheckValue `json:"value"`
}

type Asset struct {
	Name            string             `json:"name"`
	Description     string             `json:"description"`
	Type            AssetType          `json:"type"`
	ExecutableFile  ExecutableFile     `json:"executable_file"`
	DefinitionFile  TaskDefinitionFile `json:"definition_file"`
	Parameters      map[string]string  `json:"parameters"`
	Connection      string             `json:"connection"`
	Secrets         []SecretMapping    `json:"secrets"`
	DependsOn       []string           `json:"upstream"`
	Materialization Materialization    `json:"materialization"`
	Columns         []Column           `json:"columns"`
	CustomChecks    []CustomCheck      `json:"custom_checks"`
	Image           string             `json:"image"`

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

type Pipeline struct {
	LegacyID           string            `yaml:"id" json:"legacy_id"`
	Name               string            `yaml:"name" json:"name"`
	Schedule           schedule          `yaml:"schedule" json:"schedule"`
	StartDate          string            `yaml:"start_date" json:"start_date"`
	DefinitionFile     DefinitionFile    `json:"definition_file"`
	DefaultParameters  map[string]string `yaml:"default_parameters" json:"default_parameters"`
	DefaultConnections map[string]string `yaml:"default_connections" json:"default_connections"`
	Assets             []*Asset          `json:"assets"`
	Notifications      Notifications     `yaml:"notifications" json:"notifications"`

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
		task, err := b.CreateTaskFromFile(file)
		if err != nil {
			return nil, err
		}

		if task == nil {
			continue
		}

		// if the definition comes from a Python file the asset is always a Python asset, so force it
		// at least that's the hypothesis for now
		if strings.HasSuffix(task.ExecutableFile.Path, ".py") {
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

func (b *builder) CreateTaskFromFile(path string) (*Asset, error) {
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
