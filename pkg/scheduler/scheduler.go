package scheduler

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/version"
	"github.com/google/uuid"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type TaskInstanceStatus int

func (s TaskInstanceStatus) String() string {
	switch s {
	case Pending:
		return "pending"
	case Queued:
		return "queued"
	case Running:
		return "running"
	case Failed:
		return "failed"
	case UpstreamFailed:
		return "upstream_failed"
	case Succeeded:
		return "succeeded"
	case Skipped:
		return "skipped"
	}
	return "unknown"
}

func StatusFromString(status string) TaskInstanceStatus {
	switch strings.ToLower(status) {
	case "pending":
		return Pending
	case "queued":
		return Queued
	case "running":
		return Running
	case "failed":
		return Failed
	case "upstream_failed":
		return UpstreamFailed
	case "succeeded":
		return Succeeded
	case "skipped":
		return Skipped
	default:
		return -1
	}
}

type TaskInstanceType int

func (s TaskInstanceType) String() string {
	switch s {
	case TaskInstanceTypeMain:
		return "main"
	case TaskInstanceTypeColumnCheck:
		return "column_test"
	case TaskInstanceTypeCustomCheck:
		return "custom_test"
	case TaskInstanceTypeMetadataPush:
		return "metadata_push"
	}
	return "unknown"
}

const (
	Pending TaskInstanceStatus = iota
	Queued
	Running
	Failed
	UpstreamFailed
	Succeeded
	Skipped
)

const (
	TaskInstanceTypeMain TaskInstanceType = iota
	TaskInstanceTypeColumnCheck
	TaskInstanceTypeCustomCheck
	TaskInstanceTypeMetadataPush
)

type TaskInstance interface {
	GetPipeline() *pipeline.Pipeline
	GetAsset() *pipeline.Asset
	GetType() TaskInstanceType
	GetHumanID() string
	GetHumanReadableDescription() string

	GetStatus() TaskInstanceStatus
	MarkAs(status TaskInstanceStatus)
	Completed() bool
	Blocking() bool

	GetUpstream() []TaskInstance
	GetDownstream() []TaskInstance
	AddUpstream(t TaskInstance)
	AddDownstream(t TaskInstance)
}

type PipelineState struct {
	Parameters        RunConfig             `json:"parameters"`
	Metadata          Metadata              `json:"metadata"`
	State             []*PipelineAssetState `json:"state"`
	Version           string                `json:"version"`
	TimeStamp         time.Time             `json:"timestamp"`
	RunID             string                `json:"run_id"`
	CompatibilityHash string                `json:"compatibility_hash"`
}

type RunConfig struct {
	Downstream             bool     `json:"downstream"`
	StartDate              string   `json:"startDate"`
	EndDate                string   `json:"endDate"`
	Workers                int      `json:"workers"`
	Environment            string   `json:"environment"`
	Force                  bool     `json:"force"`
	PushMetadata           bool     `json:"pushMetadata"`
	NoLogFile              bool     `json:"noLogFile"`
	FullRefresh            bool     `json:"fullRefresh"`
	UsePip                 bool     `json:"useUV"`
	Tag                    string   `json:"tag"`
	ExcludeTag             string   `json:"excludeTag"`
	Only                   []string `json:"only"`
	Output                 string   `json:"output"`
	ExpUseWingetForUv      bool     `json:"expUseWingetForUv"`
	ConfigFilePath         string   `json:"configFilePath"`
	SensorMode             string   `json:"sensorMode"`
	ApplyIntervalModifiers bool     `json:"applyIntervalModifiers"`
}

type PipelineAssetState struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type Metadata struct {
	Version string `json:"version"`
	OS      string `json:"os"`
}

type AssetInstance struct {
	ID       string
	HumanID  string
	Pipeline *pipeline.Pipeline
	Asset    *pipeline.Asset

	status     TaskInstanceStatus
	upstream   []TaskInstance
	downstream []TaskInstance
}

func (t *AssetInstance) GetHumanID() string {
	return t.HumanID
}

func (t *AssetInstance) GetHumanReadableDescription() string {
	return t.Asset.Name
}

func (t *AssetInstance) GetStatus() TaskInstanceStatus {
	return t.status
}

func (t *AssetInstance) Completed() bool {
	return t.status == Failed || t.status == Succeeded || t.status == UpstreamFailed || t.status == Skipped
}

func (t *AssetInstance) Blocking() bool {
	return true
}

func (t *AssetInstance) MarkAs(status TaskInstanceStatus) {
	t.status = status
}

func (t *AssetInstance) GetPipeline() *pipeline.Pipeline {
	return t.Pipeline
}

func (t *AssetInstance) GetAsset() *pipeline.Asset {
	return t.Asset
}

func (t *AssetInstance) GetType() TaskInstanceType {
	return TaskInstanceTypeMain
}

func (t *AssetInstance) GetUpstream() []TaskInstance {
	return t.upstream
}

func (t *AssetInstance) GetDownstream() []TaskInstance {
	return t.downstream
}

func (t *AssetInstance) AddUpstream(task TaskInstance) {
	t.upstream = append(t.upstream, task)
}

func (t *AssetInstance) AddDownstream(task TaskInstance) {
	t.downstream = append(t.downstream, task)
}

type ColumnCheckInstance struct {
	*AssetInstance

	parentID string
	Column   *pipeline.Column
	Check    *pipeline.ColumnCheck
}

func (t *ColumnCheckInstance) GetType() TaskInstanceType {
	return TaskInstanceTypeColumnCheck
}

func (t *ColumnCheckInstance) GetHumanReadableDescription() string {
	return fmt.Sprintf("%s - Column '%s' / Check '%s'", t.Asset.Name, t.Column.Name, t.Check.Name)
}

func (t *ColumnCheckInstance) Blocking() bool {
	return t.Check.Blocking.Bool()
}

type CustomCheckInstance struct {
	*AssetInstance

	Check *pipeline.CustomCheck
}

func (t *CustomCheckInstance) GetType() TaskInstanceType {
	return TaskInstanceTypeCustomCheck
}

func (t *CustomCheckInstance) GetHumanReadableDescription() string {
	return fmt.Sprintf("%s - Custom Check '%s'", t.Asset.Name, t.Check.Name)
}

func (t *CustomCheckInstance) Blocking() bool {
	return t.Check.Blocking.Bool()
}

type MetadataPushInstance struct {
	*AssetInstance
}

func (t *MetadataPushInstance) GetType() TaskInstanceType {
	return TaskInstanceTypeMetadataPush
}

func (t *MetadataPushInstance) GetHumanReadableDescription() string {
	return t.Asset.Name + " - Metadata Push"
}

func (t *MetadataPushInstance) Blocking() bool {
	return false
}

type TaskExecutionResult struct {
	Instance TaskInstance
	Error    error
}

type InstancesByType map[TaskInstanceType][]TaskInstance

func (i InstancesByType) AddUpstreamByType(instanceType TaskInstanceType, upstream TaskInstance) {
	foundInstances := i[instanceType]
	for _, instance := range foundInstances {
		instance.AddUpstream(upstream)
		upstream.AddDownstream(instance)
	}
}

func (i InstancesByType) AddDownstreamByType(instanceType TaskInstanceType, downstream TaskInstance) {
	foundInstances := i[instanceType]
	for _, instance := range foundInstances {
		instance.AddDownstream(downstream)
		downstream.AddUpstream(instance)
	}
}

type Scheduler struct {
	logger           logger.Logger
	taskScheduleLock sync.Mutex
	pipeline         *pipeline.Pipeline

	taskInstances []TaskInstance
	taskNameMap   map[string]InstancesByType

	WorkQueue chan TaskInstance
	Results   chan *TaskExecutionResult

	runID string
}

func (s *Scheduler) InstanceCount() int {
	return len(s.taskInstances)
}

func (s *Scheduler) InstanceCountByStatus(status TaskInstanceStatus) int {
	count := 0
	for _, i := range s.taskInstances {
		if i.GetStatus() == status {
			count++
		}
	}

	return count
}

func (s *Scheduler) MarkAll(status TaskInstanceStatus) {
	for _, instance := range s.taskInstances {
		instance.MarkAs(status)
	}
}

func (s *Scheduler) MarkAsset(task *pipeline.Asset, status TaskInstanceStatus, downstream bool) {
	instancesByType := s.taskNameMap[task.Name]
	for _, instance := range instancesByType {
		for _, i := range instance {
			s.MarkTaskInstance(i, status, downstream)
		}
	}
}

func (s *Scheduler) MarkPendingInstancesByType(instanceType TaskInstanceType, status TaskInstanceStatus) {
	for _, instance := range s.taskInstances {
		if instance.GetStatus() != Pending {
			continue
		}

		if instance.GetType() != instanceType {
			continue
		}

		s.MarkTaskInstance(instance, status, false)
	}
}

func (s *Scheduler) MarkCheckInstancesByID(checkID string, status TaskInstanceStatus) error {
	for _, instance := range s.taskInstances {
		columnCheck, ok := instance.(*ColumnCheckInstance)
		if ok {
			if columnCheck.Check.ID == checkID {
				s.MarkTaskInstance(instance, status, false)
				return nil
			}
		}
		customCheck, ok := instance.(*CustomCheckInstance)

		if ok {
			if customCheck.Check.ID == checkID {
				s.MarkTaskInstance(instance, status, false)
				return nil
			}
		}
	}
	return errors.New("cannot find check with the given ID")
}

func (s *Scheduler) MarkByTag(tag string, status TaskInstanceStatus, downstream bool) {
	for _, instance := range s.taskInstances {
		asset := instance.GetAsset()
		if len(asset.Tags) == 0 {
			continue
		}

		if !slices.Contains(asset.Tags, tag) {
			continue
		}

		s.MarkTaskInstance(instance, status, downstream)
	}
}

func (s *Scheduler) MarkTaskInstance(instance TaskInstance, status TaskInstanceStatus, downstream bool) {
	instance.MarkAs(status)
	if !downstream {
		return
	}

	downstreams := instance.GetDownstream()
	if len(downstreams) == 0 {
		return
	}

	for _, d := range downstreams {
		s.MarkTaskInstance(d, status, downstream)
	}
}

func (s *Scheduler) MarkTaskInstanceIfNotSkipped(instance TaskInstance, status TaskInstanceStatus, markDownstream bool) {
	if instance.GetStatus() == Skipped {
		return
	}
	instance.MarkAs(status)
	if !markDownstream {
		return
	}

	downstreams := instance.GetDownstream()
	if len(downstreams) == 0 {
		return
	}

	for _, d := range downstreams {
		s.MarkTaskInstanceIfNotSkipped(d, status, markDownstream)
	}
}

func (s *Scheduler) markTaskInstanceFailedWithDownstream(instance TaskInstance) {
	s.MarkTaskInstanceIfNotSkipped(instance, UpstreamFailed, true)
	s.MarkTaskInstanceIfNotSkipped(instance, Failed, false)
}

func (s *Scheduler) GetTaskInstancesByStatus(status TaskInstanceStatus) []TaskInstance {
	instances := make([]TaskInstance, 0)
	for _, i := range s.taskInstances {
		if i.GetStatus() != status {
			continue
		}

		instances = append(instances, i)
	}

	return instances
}

func (s *Scheduler) WillRunTaskOfType(taskType pipeline.AssetType) bool {
	instances := s.GetTaskInstancesByStatus(Pending)
	for _, instance := range instances {
		asset := instance.GetAsset()
		assetType := asset.Type
		if assetType == pipeline.AssetTypeIngestr && taskType != pipeline.AssetTypeIngestr {
			assetType, _ = helpers.GetIngestrDestinationType(asset)
		}
		if assetType == taskType {
			return true
		}
	}

	return false
}

func (s *Scheduler) FindMajorityOfTypes(defaultIfNone pipeline.AssetType) pipeline.AssetType {
	return s.pipeline.GetMajorityAssetTypesFromSQLAssets(defaultIfNone)
}

func NewScheduler(logger logger.Logger, p *pipeline.Pipeline, runID string) *Scheduler {
	instances := make([]TaskInstance, 0)
	for _, task := range p.Assets {
		parentID := uuid.New().String()
		instance := &AssetInstance{
			ID:         parentID,
			HumanID:    task.Name,
			Pipeline:   p,
			Asset:      task,
			status:     Pending,
			upstream:   make([]TaskInstance, 0),
			downstream: make([]TaskInstance, 0),
		}
		instances = append(instances, instance)

		for _, col := range task.Columns {
			for _, t := range col.Checks {
				testInstance := &ColumnCheckInstance{
					AssetInstance: &AssetInstance{
						ID:         uuid.New().String(),
						HumanID:    fmt.Sprintf("%s:%s:%s", task.Name, col.Name, t.Name),
						Pipeline:   p,
						Asset:      task,
						status:     Pending,
						upstream:   make([]TaskInstance, 0),
						downstream: make([]TaskInstance, 0),
					},
					parentID: parentID,
					Column:   &col,
					Check:    &t,
				}
				instances = append(instances, testInstance)
			}
		}

		for _, c := range task.CustomChecks {
			humanIDName := strings.ReplaceAll(strings.ToLower(c.Name), " ", "_")
			testInstance := &CustomCheckInstance{
				AssetInstance: &AssetInstance{
					ID:         uuid.New().String(),
					HumanID:    fmt.Sprintf("%s:custom-check:%s", task.Name, humanIDName),
					Pipeline:   p,
					Asset:      task,
					status:     Pending,
					upstream:   make([]TaskInstance, 0),
					downstream: make([]TaskInstance, 0),
				},
				Check: &c,
			}
			instances = append(instances, testInstance)
		}

		if p.MetadataPush.HasAnyEnabled() {
			instances = append(instances, &MetadataPushInstance{
				AssetInstance: &AssetInstance{
					ID:         uuid.New().String(),
					HumanID:    task.Name + ":metadata-push",
					Pipeline:   p,
					Asset:      task,
					status:     Pending,
					upstream:   make([]TaskInstance, 0),
					downstream: make([]TaskInstance, 0),
				},
			})
		}
	}

	s := &Scheduler{
		logger:           logger,
		pipeline:         p,
		taskInstances:    instances,
		taskScheduleLock: sync.Mutex{},
		WorkQueue:        make(chan TaskInstance, 100),
		Results:          make(chan *TaskExecutionResult),
		runID:            runID,
	}
	s.initialize()

	return s
}

func (s *Scheduler) initialize() {
	s.constructTaskNameMap()
	s.constructInstanceRelationships()
}

// useful for debugging sessions.
// func (s *Scheduler) printTaskInstances() {
// 	fmt.Println("\n\nTask Instances")
// 	for _, types := range s.taskNameMap {
// 		for _, instances := range types {
// 			if len(instances) == 0 {
// 				continue
// 			}
// 			for _, instance := range instances {
// 				upstream := instance.GetUpstream()
// 				upstreamNames := make([]string, 0)
// 				for _, u := range upstream {
// 					upstreamNames = append(upstreamNames, u.GetHumanID())
// 				}
//
// 				downstreamNames := make([]string, 0)
// 				downstream := instance.GetDownstream()
// 				for _, d := range downstream {
// 					downstreamNames = append(downstreamNames, d.GetHumanID())
// 				}
//
// 				fmt.Println(instance.GetAsset().Name, instance.GetType(), upstreamNames, downstreamNames, instance.GetStatus())
// 			}
//
// 		}
// 	}
//
// 	fmt.Println("=================")
// }

func (s *Scheduler) constructTaskNameMap() {
	s.taskNameMap = make(map[string]InstancesByType)
	for _, ti := range s.taskInstances {
		assetName := ti.GetAsset().Name
		if _, ok := s.taskNameMap[assetName]; !ok {
			s.taskNameMap[assetName] = InstancesByType{}
		}

		s.taskNameMap[assetName][ti.GetType()] = append(s.taskNameMap[assetName][ti.GetType()], ti)
	}
}

func (s *Scheduler) constructInstanceRelationships() {
	for _, ti := range s.taskInstances {
		if ti.GetType() != TaskInstanceTypeMain {
			continue
		}

		assetName := ti.GetAsset().Name

		// add the upstream-downstream relationships for the main task to its quality checks
		s.taskNameMap[assetName].AddUpstreamByType(TaskInstanceTypeColumnCheck, ti)
		s.taskNameMap[assetName].AddUpstreamByType(TaskInstanceTypeCustomCheck, ti)
		s.taskNameMap[assetName].AddUpstreamByType(TaskInstanceTypeMetadataPush, ti)

		for _, dep := range ti.GetAsset().Upstreams {
			if dep.Mode == pipeline.UpstreamModeSymbolic {
				continue
			}

			if dep.Type != "asset" {
				continue
			}

			upstreamInstances, ok := s.taskNameMap[dep.Value]
			if !ok {
				continue
			}

			for _, instances := range upstreamInstances {
				for _, upstream := range instances {
					if !upstream.Blocking() {
						continue
					}

					ti.AddUpstream(upstream)
					upstream.AddDownstream(ti)
				}
			}
		}
	}
}

func (s *Scheduler) Run(ctx context.Context) []*TaskExecutionResult {
	results := make([]*TaskExecutionResult, 0)
	if len(s.GetTaskInstancesByStatus(Pending)) == 0 {
		s.logger.Debug("no tasks to run, finishing the scheduler loop")
		return nil
	}

	go s.Kickstart()

	s.logger.Debug("started the scheduler loop")
	for {
		select {
		case <-ctx.Done():
			close(s.WorkQueue)
			return results
		case result := <-s.Results:
			s.logger.Debug("received task result: ", result.Instance.GetAsset().Name)
			results = append(results, result)
			finished := s.Tick(result)
			if finished {
				s.logger.Debug("pipeline has completed, finishing the scheduler loop")
				return results
			}
		}
	}
}

// Tick marks an iteration of the scheduler loop. It is called when a result is received.
// The results are mainly fed from a channel, but Tick allows implementing additional methods of passing
// Asset results and simulating scheduler loops, e.g. time travel. It is also useful for testing purposes.
func (s *Scheduler) Tick(result *TaskExecutionResult) bool {
	s.taskScheduleLock.Lock()
	defer s.taskScheduleLock.Unlock()
	if result.Instance.GetStatus() != Skipped {
		s.MarkTaskInstance(result.Instance, Succeeded, false)
	}
	if result.Error != nil {
		s.markTaskInstanceFailedWithDownstream(result.Instance)
	}

	if s.hasPipelineFinished() {
		close(s.WorkQueue)
		return true
	}

	tasks := s.getScheduleableTasks()
	if len(tasks) == 0 {
		return false
	}

	for _, task := range tasks {
		task.MarkAs(Queued)
		s.WorkQueue <- task
	}

	return false
}

// Kickstart initiates the scheduler process by sending a "start" task for the processing.
func (s *Scheduler) Kickstart() {
	s.Tick(&TaskExecutionResult{
		Instance: &AssetInstance{
			Asset: &pipeline.Asset{
				Name: "start",
			},
			status: Succeeded,
		},
	})
}

func (s *Scheduler) getScheduleableTasks() []TaskInstance {
	tasks := make([]TaskInstance, 0)
	for _, task := range s.taskInstances {
		if task.GetStatus() != Pending {
			continue
		}

		if !s.allDependenciesSucceededForTask(task) {
			continue
		}

		tasks = append(tasks, task)
	}

	return tasks
}

func (s *Scheduler) allDependenciesSucceededForTask(t TaskInstance) bool {
	if len(t.GetUpstream()) == 0 {
		return true
	}

	for _, upstream := range t.GetUpstream() {
		status := upstream.GetStatus()
		if status == Pending || status == Queued || status == Running {
			return false
		}
	}

	return true
}

func (s *Scheduler) hasPipelineFinished() bool {
	for _, task := range s.taskInstances {
		if !task.Completed() {
			return false
		}
	}

	return true
}

func (s *Scheduler) SavePipelineState(fs afero.Fs, param *RunConfig, runID, statePath string) error {
	state := make([]*PipelineAssetState, 0)
	dict := make(map[string][]TaskInstanceStatus)
	for _, task := range s.taskInstances {
		dict[task.GetAsset().Name] = append(dict[task.GetAsset().Name], task.GetStatus())
	}

	for key, status := range dict {
		result := GetStatusForTask(status)
		state = append(state, &PipelineAssetState{
			Name:   key,
			Status: result.String(),
		})
	}

	pipelineState := &PipelineState{
		Parameters: *param,
		Metadata: Metadata{
			Version: version.Version,
			OS:      runtime.GOOS,
		},
		State:             state,
		Version:           "1.0.0",
		TimeStamp:         time.Now(),
		RunID:             runID,
		CompatibilityHash: s.pipeline.GetCompatibilityHash(),
	}
	file := filepath.Join(statePath, runID+".json")
	if err := helpers.WriteJSONToFile(fs, pipelineState, file); err != nil {
		s.logger.Error("failed to write pipeline state to file", zap.Error(err))
		return err
	}
	return nil
}

func (s *Scheduler) RestoreState(state *PipelineState) error {
	if s.pipeline.GetCompatibilityHash() != state.CompatibilityHash {
		return errors.New("the pipeline has changed since the last run; please rerun the pipeline")
	}
	stateMap := make(map[string]string)
	for _, state := range state.State {
		stateMap[state.Name] = state.Status
	}

	for _, task := range s.taskInstances {
		taskName := task.GetAsset().Name
		if status, exists := stateMap[taskName]; exists {
			switch status {
			case Failed.String(), UpstreamFailed.String(), Running.String(), Queued.String():
				task.MarkAs(Pending)
			case Skipped.String(), Succeeded.String():
				task.MarkAs(Skipped)
			default:
				return fmt.Errorf("unknown status: %s. Please report this issue at https://github.com/bruin-data/bruin/issues/new", status)
			}
		}
	}

	return nil
}

func GetStatusForTask(tasks []TaskInstanceStatus) TaskInstanceStatus {
	dict := make(map[TaskInstanceStatus]bool)
	for _, status := range tasks {
		if _, ok := dict[status]; !ok {
			dict[status] = true
			continue
		}
	}
	if dict[Failed] || dict[UpstreamFailed] {
		return Failed
	}

	if dict[Skipped] && len(dict) == 1 {
		return Skipped
	}

	if dict[Succeeded] && !dict[Running] {
		return Succeeded
	}

	return Pending
}

func ReadState(fs afero.Fs, statePath string) (*PipelineState, error) {
	latestRunID, err := helpers.GetLatestFileInDir(fs, statePath)
	if err != nil {
		return nil, err
	}
	pipelineState := &PipelineState{}
	if err := helpers.ReadJSONToFile(fs, latestRunID, pipelineState); err != nil {
		return nil, err
	}

	return pipelineState, nil
}
