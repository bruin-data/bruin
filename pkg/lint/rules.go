package lint

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/robfig/cron/v3"
	"github.com/spf13/afero"
	"github.com/yourbasic/graph"
)

const (
	validIDRegex = `^[\w.-]+$`

	taskNameMustExist          = `A task must have a name`
	taskNameMustBeAlphanumeric = `A task name must be made of alphanumeric characters, dashes, dots and underscores`
	taskTypeMustExist          = `A task must have a type, e.g. 'bq.sql' for a GoogleCloudPlatform SQL task`

	executableFileCannotBeEmpty   = `The 'run' option cannot be empty, make sure you have defined a file to run`
	executableFileDoesNotExist    = `The executable file does not exist`
	executableFileIsADirectory    = `The executable file is a directory, must be a file`
	executableFileIsEmpty         = `The executable file is empty`
	executableFileIsNotExecutable = "Executable file is not executable, give it the '644' or '755' permissions"

	pipelineNameCannotBeEmpty      = "The pipeline name cannot be empty, it must be a valid name made of alphanumeric characters, dashes, dots and underscores"
	pipelineNameMustBeAlphanumeric = "The pipeline name must be made of alphanumeric characters, dashes, dots and underscores"

	pipelineContainsCycle = "The pipeline has a cycle with dependencies, make sure there are no cyclic dependencies"

	pipelineSlackFieldEmptyChannel     = "Slack notifications must have a `channel` attribute"
	pipelineSlackChannelFieldNotUnique = "The `channel` attribute under the Slack notifications must be unique"

	materializationStrategyIsNotSupportedForViews     = "Materialization strategy is not supported for views"
	materializationPartitionByNotSupportedForViews    = "Materialization partition by is not supported for views because views cannot be partitioned"
	materializationIncrementalKeyNotSupportedForViews = "Materialization incremental key is not supported for views because views cannot be updated incrementally"
	materializationClusterByNotSupportedForViews      = "Materialization cluster by is not supported for views because views cannot be clustered"
	TableNamespaceSizeBigQuery                        = 3
)

var validIDRegexCompiled = regexp.MustCompile(validIDRegex)

func EnsureTaskNameIsValidForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	if asset.Name == "" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: taskNameMustExist,
		})

		return issues, nil
	}

	if match := validIDRegexCompiled.MatchString(asset.Name); !match {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: taskNameMustBeAlphanumeric,
		})
	}

	return issues, nil
}

func EnsureTaskNameIsUnique(p *pipeline.Pipeline) ([]*Issue, error) {
	nameFileMapping := make(map[string][]*pipeline.Asset)
	for _, task := range p.Assets {
		if task.Name == "" {
			continue
		}

		if _, ok := nameFileMapping[task.Name]; !ok {
			nameFileMapping[task.Name] = make([]*pipeline.Asset, 0)
		}

		nameFileMapping[task.Name] = append(nameFileMapping[task.Name], task)
	}

	issues := make([]*Issue, 0)
	for name, files := range nameFileMapping {
		if len(files) == 1 {
			continue
		}

		taskPaths := make([]string, 0)
		for _, task := range files {
			taskPaths = append(taskPaths, task.DefinitionFile.Path)
		}

		issues = append(issues, &Issue{
			Task:        files[0],
			Description: fmt.Sprintf("Asset name '%s' is not unique, please make sure all the task names are unique", name),
			Context:     taskPaths,
		})
	}

	return issues, nil
}

func EnsureTaskNameIsUniqueForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Name == "" {
		return issues, nil
	}

	taskPaths := []string{asset.DefinitionFile.Path}

	for _, a := range p.Assets {
		if a.Name == "" {
			continue
		}

		if a.Name == asset.Name && a.DefinitionFile.Path != asset.DefinitionFile.Path {
			taskPaths = append(taskPaths, a.DefinitionFile.Path)
		}
	}

	if len(taskPaths) == 1 {
		return issues, nil
	}

	slices.Sort(taskPaths)
	issues = append(issues, &Issue{
		Task:        asset,
		Description: fmt.Sprintf("Asset name '%s' is not unique, please make sure all the task names are unique", asset.Name),
		Context:     taskPaths,
	})

	return issues, nil
}

func EnsureExecutableFileIsValidForASingleAsset(fs afero.Fs) AssetValidator {
	return func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		issues := make([]*Issue, 0)
		if asset.DefinitionFile.Type == pipeline.CommentTask {
			return issues, nil
		}

		if asset.ExecutableFile.Path == "" {
			if asset.Type == pipeline.AssetTypePython {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: executableFileCannotBeEmpty,
				})
			}
			return issues, nil
		}

		fileInfo, err := fs.Stat(asset.ExecutableFile.Path)
		if errors.Is(err, os.ErrNotExist) {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: executableFileDoesNotExist,
			})
			return issues, nil
		}

		if fileInfo.IsDir() {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: executableFileIsADirectory,
			})
			return issues, nil
		}

		if fileInfo.Size() == 0 {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: executableFileIsEmpty,
			})
		}

		if isFileExecutable(fileInfo.Mode()) {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: executableFileIsNotExecutable,
			})
		}

		return issues, nil
	}
}

func EnsurePipelineNameIsValid(pipeline *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if pipeline.Name == "" {
		issues = append(issues, &Issue{
			Description: pipelineNameCannotBeEmpty,
		})

		return issues, nil
	}

	if match := validIDRegexCompiled.MatchString(pipeline.Name); !match {
		issues = append(issues, &Issue{
			Description: pipelineNameMustBeAlphanumeric,
		})
	}

	return issues, nil
}

func CallFuncForEveryAsset(callable AssetValidator) func(pipeline *pipeline.Pipeline) ([]*Issue, error) {
	return func(pipeline *pipeline.Pipeline) ([]*Issue, error) {
		issues := make([]*Issue, 0)
		for _, task := range pipeline.Assets {
			assetIssues, err := callable(context.TODO(), pipeline, task)
			if err != nil {
				return issues, err
			}

			issues = append(issues, assetIssues...)
		}

		return issues, nil
	}
}

func EnsureIngestrAssetIsValidForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type != pipeline.AssetTypeIngestr {
		return issues, nil
	}

	requiredKeys := []string{"source_connection", "source_table", "destination"}
	for _, key := range requiredKeys {
		if asset.Parameters == nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Ingestr assets require the following parameters: " + strings.Join(requiredKeys, ", "),
			})

			return issues, nil
		}

		value, exists := asset.Parameters[key]
		if !exists || value == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Ingestr assets require the following parameters: " + strings.Join(requiredKeys, ", "),
			})

			return issues, nil
		}
	}

	updateOnMergeKeys := asset.ColumnNamesWithUpdateOnMerge()
	if len(updateOnMergeKeys) > 0 {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Ingestr assets do not support the 'update_on_merge' field, the strategy used decide the update behavior",
		})
	}

	return issues, nil
}

func isFileExecutable(mode os.FileMode) bool {
	return mode&0o111 != 0
}

func EnsureDependencyExistsForASingleAsset(ctx context.Context, p *pipeline.Pipeline, task *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	for _, dep := range task.DependsOn {
		if dep == "" {
			issues = append(issues, &Issue{
				Task:        task,
				Description: "Assets cannot have empty dependencies",
			})
		}

		upstream := p.GetAssetByName(dep)
		if upstream == nil {
			issues = append(issues, &Issue{
				Task:        task,
				Description: fmt.Sprintf("Dependency '%s' does not exist", dep),
			})
		}
	}

	return issues, nil
}

func EnsurePipelineScheduleIsValidCron(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if p.Schedule == "" {
		return issues, nil
	}

	schedule := p.Schedule
	if schedule == "daily" || schedule == "hourly" || schedule == "weekly" || schedule == "monthly" {
		schedule = "@" + schedule
	}

	_, err := cron.ParseStandard(string(schedule))
	if err != nil {
		issues = append(issues, &Issue{
			Description: fmt.Sprintf("Invalid cron schedule '%s'", p.Schedule),
		})
	}

	return issues, nil
}

func EnsureTypeIsCorrectForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type == "" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: taskTypeMustExist,
		})
		return issues, nil
	}

	if _, ok := executor.DefaultExecutorsV2[asset.Type]; !ok {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Invalid asset type '%s'", asset.Type),
		})
	}

	return issues, nil
}

// EnsurePipelineHasNoCycles ensures that the pipeline is a DAG, and contains no cycles.
// Since the pipelines are directed graphs, strongly connected components mean cycles, therefore
// they would be considered invalid for our pipelines.
// Strong connectivity wouldn't work for tasks that depend on themselves, therefore there's a specific check for that.
func EnsurePipelineHasNoCycles(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	for _, task := range p.Assets {
		for _, dep := range task.DependsOn {
			if task.Name == dep {
				issues = append(issues, &Issue{
					Description: pipelineContainsCycle,
					Context:     []string{fmt.Sprintf("Asset `%s` depends on itself", task.Name)},
				})
			}
		}
	}

	taskNameToIndex := make(map[string]int, len(p.Assets))
	for i, task := range p.Assets {
		taskNameToIndex[task.Name] = i
	}

	g := graph.New(len(p.Assets))
	for _, task := range p.Assets {
		for _, dep := range task.DependsOn {
			g.Add(taskNameToIndex[task.Name], taskNameToIndex[dep])
		}
	}

	cycles := graph.StrongComponents(g)
	for _, cycle := range cycles {
		cycleLength := len(cycle)
		if cycleLength == 1 {
			continue
		}

		tasksInCycle := make(map[string]bool, cycleLength)
		for _, taskIndex := range cycle {
			tasksInCycle[p.Assets[taskIndex].Name] = true
		}

		context := make([]string, 0, cycleLength)
		for _, taskIndex := range cycle {
			task := p.Assets[taskIndex]
			for _, dep := range task.DependsOn {
				if _, ok := tasksInCycle[dep]; !ok {
					continue
				}

				context = append(context, fmt.Sprintf("%s ➜ %s", task.Name, dep))
			}
		}

		issues = append(issues, &Issue{
			Description: pipelineContainsCycle,
			Context:     context,
		})
	}

	return issues, nil
}

func isStringInArray(arr []string, str string) bool {
	for _, a := range arr {
		if str == a {
			return true
		}
	}
	return false
}

func EnsureSlackFieldInPipelineIsValid(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	slackChannels := make([]string, 0, len(p.Notifications.Slack))
	for _, slack := range p.Notifications.Slack {
		channelWithoutHash := strings.TrimPrefix(slack.Channel, "#")
		if channelWithoutHash == "" {
			issues = append(issues, &Issue{
				Description: pipelineSlackFieldEmptyChannel,
			})
			continue
		}

		if isStringInArray(slackChannels, channelWithoutHash) {
			issues = append(issues, &Issue{
				Description: pipelineSlackChannelFieldNotUnique,
			})
		}

		slackChannels = append(slackChannels, channelWithoutHash)
	}

	return issues, nil
}

func EnsureMaterializationValuesAreValidForSingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	switch asset.Materialization.Type {
	case pipeline.MaterializationTypeNone:
		return issues, nil
	case pipeline.MaterializationTypeView:
		if asset.Materialization.Strategy != pipeline.MaterializationStrategyNone {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: materializationStrategyIsNotSupportedForViews,
			})
		}

		if asset.Materialization.IncrementalKey != "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: materializationIncrementalKeyNotSupportedForViews,
			})
		}

		if asset.Materialization.ClusterBy != nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: materializationClusterByNotSupportedForViews,
			})
		}

		if asset.Materialization.PartitionBy != "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: materializationPartitionByNotSupportedForViews,
			})
		}

	case pipeline.MaterializationTypeTable:
		if asset.Materialization.Strategy == pipeline.MaterializationStrategyNone {
			return issues, nil
		}

		switch asset.Materialization.Strategy {
		case pipeline.MaterializationStrategyNone:
		case pipeline.MaterializationStrategyCreateReplace:
		case pipeline.MaterializationStrategyAppend:
			return issues, nil
		case pipeline.MaterializationStrategyDeleteInsert:
			if asset.Materialization.IncrementalKey == "" {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Materialization strategy 'delete+insert' requires the 'incremental_key' field to be set",
				})
			}
		case pipeline.MaterializationStrategyMerge:
			if len(asset.Columns) == 0 {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Materialization strategy 'merge' requires the 'columns' field to be set with actual columns",
				})
			}

			primaryKeys := asset.ColumnNamesWithPrimaryKey()
			if len(primaryKeys) == 0 {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Materialization strategy 'merge' requires the 'primary_key' field to be set on at least one column",
				})
			}

		default:
			issues = append(issues, &Issue{
				Task: asset,
				Description: fmt.Sprintf(
					"Materialization strategy '%s' is not supported, available strategies are: %v",
					asset.Materialization.Strategy,
					pipeline.AllAvailableMaterializationStrategies,
				),
			})
		}
	default:
		issues = append(issues, &Issue{
			Task: asset,
			Description: fmt.Sprintf(
				"Materialization type '%s' is not supported, available types are: %v",
				asset.Materialization.Type,
				[]pipeline.MaterializationType{
					pipeline.MaterializationTypeView,
					pipeline.MaterializationTypeTable,
				},
			),
		})
	}

	return issues, nil
}

func EnsureSnowflakeSensorHasQueryParameterForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type != pipeline.AssetTypeSnowflakeQuerySensor {
		return issues, nil
	}

	query, ok := asset.Parameters["query"]
	if !ok {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Snowflake query sensor requires a `query` parameter",
		})
		return issues, nil
	}

	if query == "" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Snowflake query sensor requires a `query` parameter that is not empty",
		})
	}

	return issues, nil
}

func EnsureBigQueryTableSensorHasTableParameterForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type != pipeline.AssetTypeBigqueryTableSensor {
		return issues, nil
	}

	table, ok := asset.Parameters["table"]
	if !ok {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "BigQuery table sensor requires a `table` parameter",
		})
		return issues, nil
	}
	tableItems := strings.Split(table, ".")

	if len(tableItems) != TableNamespaceSizeBigQuery {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "BigQuery table sensor `table` parameter must be in the format `project.dataset.table`",
		})
	}

	return issues, nil
}

func EnsureBigQueryQuerySensorHasTableParameterForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type != pipeline.AssetTypeBigqueryQuerySensor {
		return issues, nil
	}

	query, ok := asset.Parameters["query"]
	if !ok {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "BigQuery query sensor requires a `query` parameter",
		})
		return issues, nil
	}

	if query == "" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "BigQuery query sensor requires a `query` parameter that is not empty",
		})
	}

	return issues, nil
}
