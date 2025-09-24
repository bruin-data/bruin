package lint

import (
	"context"
	"encoding/csv"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/sqlparser"
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

	pipelineMSTeamsConnectionFieldNotUnique = "The `connection` attribute under the MS Teams notifications must be unique"
	pipelineMSTeamsConnectionFieldEmpty     = "MS Teams notifications `connection` attribute must not be empty"

	pipelineConcurrencyMustBePositive = "Pipeline concurrency must be 1 or greater"
	assetTierMustBeBetweenOneAndFive  = "Asset tier must be between 1 and 5"
	secretMappingKeyMustExist         = "Secrets must have a `key` attribute"

	materializationStrategyIsNotSupportedForViews     = "Materialization strategy is not supported for views"
	materializationPartitionByNotSupportedForViews    = "Materialization partition by is not supported for views because views cannot be partitioned"
	materializationIncrementalKeyNotSupportedForViews = "Materialization incremental key is not supported for views because views cannot be updated incrementally"
	materializationClusterByNotSupportedForViews      = "Materialization cluster by is not supported for views because views cannot be clustered"
)

var validIDRegexCompiled = regexp.MustCompile(validIDRegex)

type ValidatorSeverity int

const (
	ValidatorSeverityWarning ValidatorSeverity = iota
	ValidatorSeverityCritical
)

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

func EnsureTaskNameIsUnique(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
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

func EnsurePipelineNameIsValid(ctx context.Context, pipeline *pipeline.Pipeline) ([]*Issue, error) {
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

func CallFuncForEveryAsset(callable AssetValidator) func(ctx context.Context, pipeline *pipeline.Pipeline) ([]*Issue, error) {
	return func(ctx context.Context, pipeline *pipeline.Pipeline) ([]*Issue, error) {
		issues := make([]*Issue, 0)
		excludeTag, ok := ctx.Value(excludeTagKey).(string)
		if !ok {
			excludeTag = ""
		}
		for _, task := range pipeline.Assets {
			if ContainsTag(task.Tags, excludeTag) {
				continue
			}
			assetIssues, err := callable(ctx, pipeline, task)
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
	if value, exists := asset.Parameters["incremental_strategy"]; exists && value == "merge" {
		primaryKeys := asset.ColumnNamesWithPrimaryKey()
		if len(primaryKeys) == 0 {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Materialization strategy 'merge' requires the 'primary_key' field to be set on at least one column",
			})
		}
	}

	return issues, nil
}

func isFileExecutable(mode os.FileMode) bool {
	return mode&0o111 != 0
}

func EnsureDependencyExistsForASingleAsset(ctx context.Context, p *pipeline.Pipeline, task *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	for _, dep := range task.Upstreams {
		if dep.Value == "" {
			issues = append(issues, &Issue{
				Task:        task,
				Description: "Assets cannot have empty dependencies",
			})
		}

		if dep.Type == "uri" {
			continue
		}

		upstream := p.GetAssetByName(dep.Value)
		if upstream == nil {
			issues = append(issues, &Issue{
				Task:        task,
				Description: fmt.Sprintf("Dependency '%s' does not exist", dep.Value),
			})
		}
	}

	return issues, nil
}

func EnsurePipelineScheduleIsValidCron(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if p.Schedule == "" {
		return issues, nil
	}

	if p.Schedule == "continuous" || p.Schedule == "@continuous" {
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

type WarnRegularYamlFiles struct {
	fs afero.Fs
}

func (w *WarnRegularYamlFiles) WarnRegularYamlFilesInRepo(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	if p.DefinitionFile.Path == "" {
		return issues, nil
	}

	// Get the directory containing the pipeline file
	pipelineDir := filepath.Dir(p.DefinitionFile.Path)

	assetsDir := filepath.Join(pipelineDir, "assets")
	exists, err := afero.DirExists(w.fs, assetsDir)
	if err != nil || !exists {
		return issues, nil //nolint:all
	}

	foundFiles := make([]string, 0)

	// Walk through the assets directory to find .yml files
	err = afero.Walk(w.fs, assetsDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil //nolint:all
		}

		if info.IsDir() {
			return nil
		}

		isYaml := strings.HasSuffix(path, ".yml") || strings.HasSuffix(path, ".yaml")
		if !isYaml {
			return nil
		}

		isAssetYaml := strings.HasSuffix(path, ".asset.yml") || strings.HasSuffix(path, ".asset.yaml")
		if isAssetYaml {
			return nil
		}

		foundFiles = append(foundFiles, path)
		return nil
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to walk through assets directory")
	}

	if len(foundFiles) > 0 {
		issueContext := make([]string, len(foundFiles))
		for i, file := range foundFiles {
			pathToAppend := file
			relPath, err := filepath.Rel(filepath.Dir(p.DefinitionFile.Path), file)
			if err == nil {
				pathToAppend = relPath
			}

			issueContext[i] = pathToAppend
		}

		issues = append(issues, &Issue{
			Description: "Regular YAML files are not treated as assets, please rename them to `.asset.yml` if you intended to create assets.",
			Context:     issueContext,
		})
	}

	return issues, nil
}

func EnsurePipelineStartDateIsValid(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if p.StartDate == "" {
		return issues, nil
	}

	_, err := time.Parse("2006-01-02", p.StartDate)
	if err != nil {
		issues = append(issues, &Issue{
			Description: fmt.Sprintf("start_date must be in the format of YYYY-MM-DD in the pipeline definition, '%s' given", p.StartDate),
		})
	}

	return issues, nil
}

// ValidateCustomCheckQueryExists checks for duplicate column checks within a single column.
// It returns a slice of Issues, each representing a duplicate column check found.
func ValidateCustomCheckQueryExists(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	var issues []*Issue
	for _, check := range asset.CustomChecks {
		if check.Query == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Custom check '%s' query cannot be empty", check.Name),
			})
		}
	}
	return issues, nil
}

func ValidatePythonAssetMaterialization(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type != pipeline.AssetTypePython {
		return issues, nil
	}
	if asset.Materialization.Type != pipeline.MaterializationTypeTable {
		return issues, nil
	}

	if len(asset.Connection) == 0 {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "A task with materialization must have a connection defined",
		})
	}

	return issues, nil
}

func ValidateAssetSeedValidation(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if strings.HasSuffix(string(asset.Type), ".seed") {
		if asset.Materialization.Type != pipeline.MaterializationTypeNone {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Materialization is not allowed on a seed asset",
			})
		}
		if asset.Parameters["path"] == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Seed file path is required",
			})
			return issues, nil
		}

		seedFilePath := filepath.Join(filepath.Dir(asset.DefinitionFile.Path), asset.Parameters["path"])
		_, err := os.Stat(seedFilePath)
		if os.IsNotExist(err) {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Seed file does not exist or cannot be found",
			})
			return issues, nil
		}

		file, err := os.Open(seedFilePath)
		if err != nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Failed to open seed file",
			})
			return issues, nil
		}
		columnMap := make(map[string]bool)
		defer file.Close()
		if file != nil {
			reader := csv.NewReader(file)
			headers, err := reader.Read()
			if err != nil {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "CSV file cannot be parsed",
				})
				return issues, nil
			}
			for _, header := range headers {
				columnMap[strings.ToLower(header)] = true
			}
		}

		for _, column := range asset.Columns {
			if !columnMap[strings.ToLower(column.Name)] {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: fmt.Sprintf("Column '%s' is defined in the asset but does not exist in the CSV", column.Name),
				})
			}
		}
	}
	return issues, nil
}

var arnPattern = regexp.MustCompile(`^arn:[^:\n]*:[^:\n]*:[^:\n]*:[^:\n]*:(?:[^:\/\n]*[:\/])?.*$`)

func ValidateEMRServerlessAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	supportedTypes := []pipeline.AssetType{
		pipeline.AssetTypeEMRServerlessPyspark,
		pipeline.AssetTypeEMRServerlessSpark,
	}
	if !slices.Contains(supportedTypes, asset.Type) {
		return issues, nil
	}

	extension := filepath.Ext(asset.ExecutableFile.Path)
	if asset.Type == pipeline.AssetTypeEMRServerlessPyspark && extension != ".py" {
		issues = append(issues, &Issue{
			Task: asset,
			Description: fmt.Sprintf(
				"%s assets must be a Python file",
				pipeline.AssetTypeEMRServerlessPyspark,
			),
		})
	}
	if asset.Type == pipeline.AssetTypeEMRServerlessSpark && !slices.Contains([]string{".yaml", ".yml"}, extension) {
		issues = append(issues, &Issue{
			Task: asset,
			Description: fmt.Sprintf(
				"%s assets must be a YAML file",
				pipeline.AssetTypeEMRServerlessSpark,
			),
		})
	}

	required := []string{}
	prohibited := []string{}
	if asset.Type == pipeline.AssetTypeEMRServerlessSpark {
		required = append(required, "entrypoint")
	} else {
		prohibited = append(prohibited, "entrypoint")
	}

	for _, key := range required {
		value := strings.TrimSpace(asset.Parameters[key])
		if value == "" {
			issues = append(issues, &Issue{
				Task: asset,
				Description: fmt.Sprintf( //nolint
					"missing required field parameters.%s", key,
				),
			})
		}
	}
	for _, key := range prohibited {
		if _, exists := asset.Parameters[key]; exists {
			issues = append(issues, &Issue{
				Task: asset,
				Description: fmt.Sprintf( //nolint
					"prohibited field parameters.%s", key,
				),
			})
		}
	}

	timeoutSpec := strings.TrimSpace(asset.Parameters["timeout"])
	if timeoutSpec != "" {
		timeout, err := time.ParseDuration(timeoutSpec)
		if err != nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "parameters.timeout is not a valid duration",
			})
		}
		if timeout != 0 && timeout < (5*time.Minute) {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "parameters.timeout must be atleast 5m or zero",
			})
		}
	}

	executionRole := strings.TrimSpace(asset.Parameters["execution_role"])
	if executionRole != "" && !arnPattern.MatchString(executionRole) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "parameters.execution_role must be an Amazon Resource Name (ARN)",
		})
	}

	logLocation := strings.TrimSpace(asset.Parameters["logs"])
	if logLocation != "" {
		logURI, err := url.Parse(logLocation)
		if err != nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "parameters.logs must be a valid URI",
			})
		} else if logURI.Scheme != "s3" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "parameters.logs must be a valid S3 URI",
			})
		}
	}
	return issues, nil
}

// ValidateDuplicateColumnNames checks for duplicate column names within a single asset.
// It returns a slice of Issues, each representing a duplicate column name found.
//
// The function performs a case-insensitive comparison of column names.
//
// Parameters:
//   - ctx: The context for the validation operation
//   - p: A pointer to the pipeline.Pipeline struct
//   - asset: The pipeline.Asset to be validated for duplicate column names.
//
// Returns:
//   - A slice of *Issue, each describing a duplicate column name found.
//   - An error, which is always nil in this implementation.
func ValidateDuplicateColumnNames(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	var issues []*Issue

	columnNames := make(map[string]bool)
	for _, column := range asset.Columns {
		lowercaseName := strings.ToLower(column.Name)
		if columnNames[lowercaseName] {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Duplicate column name '%s' found ", column.Name),
			})
		} else {
			columnNames[lowercaseName] = true
		}
	}
	return issues, nil
}

// ValidateDuplicateTags checks for duplicate tags within an asset and its columns.
// It performs case-insensitive comparisons to find duplicates and returns issues
// for any repeated tags found either on the asset itself or within individual
// columns.
func ValidateDuplicateTags(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	var issues []*Issue

	tagSet := make(map[string]bool)
	for _, tag := range asset.Tags {
		key := strings.ToLower(tag)
		if tagSet[key] {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Duplicate asset tag '%s' found", tag),
			})
		} else {
			tagSet[key] = true
		}
	}

	for _, column := range asset.Columns {
		columnTagSet := make(map[string]bool)
		for _, tag := range column.Tags {
			key := strings.ToLower(tag)
			if columnTagSet[key] {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: fmt.Sprintf("Duplicate tag '%s' found in column '%s'", tag, column.Name),
				})
			} else {
				columnTagSet[key] = true
			}
		}
	}

	return issues, nil
}

func ValidateAssetDirectoryExist(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	var issues []*Issue

	parentDir := filepath.Dir(p.DefinitionFile.Path)

	if _, err := os.Stat(parentDir + "/assets"); os.IsNotExist(err) {
		issues = append(issues, &Issue{
			Task:        &pipeline.Asset{},
			Description: fmt.Sprintf("Assets directory does not exist at '%s'", parentDir),
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
func EnsurePipelineHasNoCycles(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	for _, task := range p.Assets {
		for _, dep := range task.Upstreams {
			if dep.Type == "uri" {
				continue
			}
			if task.Name == dep.Value {
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
		for _, dep := range task.Upstreams {
			if dep.Type == "uri" {
				continue
			}
			g.Add(taskNameToIndex[task.Name], taskNameToIndex[dep.Value])
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
			for _, dep := range task.Upstreams {
				if dep.Type == "uri" {
					continue
				}
				if _, ok := tasksInCycle[dep.Value]; !ok {
					continue
				}

				context = append(context, fmt.Sprintf("%s ➜ %s", task.Name, dep.Value))
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

func EnsureSlackFieldInPipelineIsValid(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
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

func EnsureMSTeamsFieldInPipelineIsValid(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	MSTeamsConnections := make([]string, 0, len(p.Notifications.MSTeams))
	for _, notification := range p.Notifications.MSTeams {
		if notification.Connection == "" {
			issues = append(issues, &Issue{
				Description: pipelineMSTeamsConnectionFieldEmpty,
			})
			continue
		}

		if isStringInArray(MSTeamsConnections, notification.Connection) {
			issues = append(issues, &Issue{
				Description: pipelineMSTeamsConnectionFieldNotUnique,
			})
		}

		MSTeamsConnections = append(MSTeamsConnections, notification.Connection)
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

		if asset.Materialization.IncrementalKey != "" &&
			asset.Materialization.Strategy != pipeline.MaterializationStrategyDeleteInsert && asset.Materialization.Strategy != pipeline.MaterializationStrategyTimeInterval && asset.Materialization.Strategy != pipeline.MaterializationStrategySCD2ByTime {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Incremental key is only supported with 'delete+insert', 'time_interval' and 'scd2_by_time' strategies.",
			})
		}

		switch asset.Materialization.Strategy {
		case pipeline.MaterializationStrategyNone:
		case pipeline.MaterializationStrategyDDL:
			if asset.Materialization.Type == pipeline.MaterializationTypeView {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "DDL strategy is not allowed on a view",
				})
			}
			if asset.ExecutableFile.Content != "" {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "DDL strategy builds the table, from bruin metadata and does not accept a custom query",
				})
			}
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
		case pipeline.MaterializationStrategyTruncateInsert:
			// truncate+insert doesn't require any special fields
			return issues, nil
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
		case pipeline.MaterializationStrategySCD2ByColumn:
			primaryKeys := asset.ColumnNamesWithPrimaryKey()
			if len(primaryKeys) == 0 {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Materialization strategy 'scd2_by_column' requires the 'primary_key' field to be set on at least one column",
				})
			}
		case pipeline.MaterializationStrategySCD2ByTime:
			if asset.Materialization.IncrementalKey == "" {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Materialization strategy 'scd2_by_type' requires the 'incremental_key' field to be set",
				})
			}
			primaryKeys := asset.ColumnNamesWithPrimaryKey()
			if len(primaryKeys) == 0 {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Materialization strategy 'scd2_by_type' requires the 'primary_key' field to be set on at least one column",
				})
			}

		case pipeline.MaterializationStrategyTimeInterval:
			if asset.Materialization.IncrementalKey == "" {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Materialization strategy 'time_interval' requires the 'incremental_key' field to be set",
				})
			}
			if asset.Materialization.TimeGranularity == "" {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Materialization strategy 'time_interval' requires the 'time_granularity' field to be set",
				})
			}
			if asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityDate && asset.Materialization.TimeGranularity != pipeline.MaterializationTimeGranularityTimestamp {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "'time_granularity' can be either 'date' or 'timestamp'.",
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

var TableSensorAllowedAssetTypes = map[pipeline.AssetType]bool{
	pipeline.AssetTypeBigqueryTableSensor:   true,
	pipeline.AssetTypeSnowflakeTableSensor:  true,
	pipeline.AssetTypeAthenaTableSensor:     true,
	pipeline.AssetTypeRedshiftTableSensor:   true,
	pipeline.AssetTypeDatabricksTableSensor: true,
	pipeline.AssetTypeClickHouseTableSensor: true,
	pipeline.AssetTypeMsSQLTableSensor:      true,
	pipeline.AssetTypePostgresTableSensor:   true,
	pipeline.AssetTypeSynapseTableSensor:    true,
}

var platformNames = map[pipeline.AssetType]string{
	pipeline.AssetTypeBigqueryTableSensor:   "BigQuery",
	pipeline.AssetTypeSnowflakeTableSensor:  "Snowflake",
	pipeline.AssetTypeDatabricksTableSensor: "Databricks",
	pipeline.AssetTypeAthenaTableSensor:     "Athena",
	pipeline.AssetTypePostgresTableSensor:   "PostgreSQL",
	pipeline.AssetTypeRedshiftTableSensor:   "Redshift",
	pipeline.AssetTypeMsSQLTableSensor:      "MS SQL",
	pipeline.AssetTypeClickHouseTableSensor: "ClickHouse",
	pipeline.AssetTypeSynapseTableSensor:    "Synapse",
}

func ValidateTableSensorTableParameter(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if !TableSensorAllowedAssetTypes[asset.Type] {
		return issues, nil
	}

	table, ok := asset.Parameters["table"]
	if !ok {
		platformName := platformNames[asset.Type]
		issues = append(issues, &Issue{
			Task:        asset,
			Description: platformName + " table sensor requires a `table` parameter",
		})
		return issues, nil
	}

	// Validate table name format based on database type
	validationError := validateTableNameFormat(asset.Type, table)
	if validationError != "" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: validationError,
		})
	}
	return issues, nil
}

type TableNameValidationRule struct {
	MinComponents int
	MaxComponents int
	FormatDesc    string
}

var tableNameValidationRules = map[string]TableNameValidationRule{
	"BigQuery": {
		MinComponents: 2,
		MaxComponents: 3,
		FormatDesc:    "`dataset.table` or `project.dataset.table`",
	},
	"Snowflake": {
		MinComponents: 2,
		MaxComponents: 3,
		FormatDesc:    "`schema.table` or `database.schema.table`",
	},
	"Databricks": {
		MinComponents: 2,
		MaxComponents: 2,
		FormatDesc:    "`schema.table`",
	},
	"Athena": {
		MinComponents: 1,
		MaxComponents: 1,
		FormatDesc:    "`table`",
	},
	"PostgreSQL": {
		MinComponents: 1,
		MaxComponents: 2,
		FormatDesc:    "`table` or `schema.table`",
	},
	"Redshift": {
		MinComponents: 1,
		MaxComponents: 2,
		FormatDesc:    "`table` or `schema.table`",
	},
	"MS SQL": {
		MinComponents: 1,
		MaxComponents: 2,
		FormatDesc:    "`table` or `schema.table`",
	},
	"ClickHouse": {
		MinComponents: 1,
		MaxComponents: 2,
		FormatDesc:    "`table` or `schema.table`",
	},
	"Synapse": {
		MinComponents: 1,
		MaxComponents: 2,
		FormatDesc:    "`table` or `schema.table`",
	},
}

func validateTableNameFormat(assetType pipeline.AssetType, tableName string) string {
	tableItems := strings.Split(tableName, ".")
	platformName := platformNames[assetType]

	for _, component := range tableItems {
		if component == "" {
			return fmt.Sprintf("%s table sensor `table` parameter contains empty components, '%s' given", platformName, tableName)
		}
	}

	rule, exists := tableNameValidationRules[platformName]
	if !exists {
		return fmt.Sprintf("Table sensor is not supported for this asset type %s", assetType)
	}

	componentCount := len(tableItems)
	if componentCount < rule.MinComponents || componentCount > rule.MaxComponents {
		return fmt.Sprintf("%s table sensor `table` parameter must be in format %s, '%s' given", platformName, rule.FormatDesc, tableName)
	}

	return ""
}

func EnsureBigQueryQuerySensorHasQueryParameterForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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

// ValidateCustomCheckQueryDryRun validates CustomCheck.Query using a dry-run against the DB.
func ValidateCustomCheckQueryDryRun(connections connectionManager, renderer jinja.RendererInterface) AssetValidator {
	return func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		var issues []*Issue

		if len(asset.CustomChecks) == 0 {
			return issues, nil
		}

		connName, err := p.GetConnectionNameForAsset(asset)
		if err != nil { //nolint
			return issues, nil
		}

		validator := connections.GetConnection(connName)
		if validator == nil {
			return issues, nil
		}

		validatorInstance, ok := validator.(queryValidator)
		if !ok { //nolint
			return issues, nil
		}

		assetRenderer, err := renderer.CloneForAsset(ctx, p, asset)
		if err != nil {
			return nil, err
		}

		for _, check := range asset.CustomChecks {
			if strings.TrimSpace(check.Query) == "" {
				continue
			}

			renderedQuery, err := assetRenderer.Render(check.Query)
			if err != nil {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: fmt.Sprintf("Failed to render custom check query '%s': %s", check.Name, err),
					Context:     []string{check.Query},
				})
				continue
			}

			q := &query.Query{Query: renderedQuery}
			valid, err := validatorInstance.IsValid(ctx, q)
			if err != nil {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: fmt.Sprintf("Failed to validate custom check query '%s': %s", check.Name, err),
					Context:     []string{renderedQuery},
				})
			} else if !valid {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Custom check query is invalid:" + renderedQuery,
					Context:     []string{renderedQuery},
				})
			}
		}
		return issues, nil
	}
}

type GlossaryChecker struct {
	gr                 *glossary.GlossaryReader
	foundGlossary      *glossary.Glossary
	cacheFoundGlossary bool
}

func (g *GlossaryChecker) EnsureAssetEntitiesExistInGlossary(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Columns == nil {
		return issues, nil
	}
	var err error

	foundGlossary := g.foundGlossary
	if g.foundGlossary == nil {
		foundGlossary, err = g.gr.GetGlossary(p.DefinitionFile.Path)
		if err != nil {
			g.foundGlossary = &glossary.Glossary{Entities: make([]*glossary.Entity, 0)}
			return issues, err
		}

		if foundGlossary != nil && g.cacheFoundGlossary {
			g.foundGlossary = foundGlossary
		}
	}

	for _, column := range asset.Columns {
		if column.EntityAttribute == nil {
			continue
		}

		if column.EntityAttribute.Entity == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Entity name cannot be empty",
			})
			continue
		}

		if column.EntityAttribute.Attribute == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Attribute name cannot be empty",
			})
			continue
		}

		entity := foundGlossary.GetEntity(column.EntityAttribute.Entity)
		if entity == nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Entity '%s' does not exist in the glossary", column.EntityAttribute.Entity),
			})
			continue
		}

		attribute := entity.GetAttribute(column.EntityAttribute.Attribute)
		if attribute == nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Attribute '%s' does not exist in the entity '%s'", column.EntityAttribute.Attribute, column.EntityAttribute.Entity),
			})
		}
	}

	return issues, nil
}

func (g *GlossaryChecker) EnsureParentDomainsExistInGlossary(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	var err error

	foundGlossary := g.foundGlossary
	if g.foundGlossary == nil {
		foundGlossary, err = g.gr.GetGlossary(p.DefinitionFile.Path)
		if err != nil {
			g.foundGlossary = &glossary.Glossary{Entities: make([]*glossary.Entity, 0)}
			return issues, err
		}

		if foundGlossary != nil && g.cacheFoundGlossary {
			g.foundGlossary = foundGlossary
		}
	}

	for _, domain := range foundGlossary.Domains {
		if domain.ParentDomain == "" {
			continue
		}

		parentDomain := foundGlossary.GetDomain(domain.ParentDomain)
		if parentDomain == nil {
			issues = append(issues, &Issue{
				Description: fmt.Sprintf("Parent domain '%s' for domain '%s' does not exist in the glossary", domain.ParentDomain, domain.Name),
			})
		}
	}

	return issues, nil
}

type sqlParser interface {
	UsedTables(sql, dialect string) ([]string, error)
	GetMissingDependenciesForAsset(asset *pipeline.Asset, pipeline *pipeline.Pipeline, renderer jinja.RendererInterface) ([]string, error)
	ColumnLineage(sql, dialect string, schema sqlparser.Schema) (*sqlparser.Lineage, error)
}

type UsedTableValidatorRule struct {
	renderer jinja.RendererInterface
	parser   sqlParser
}

func (u UsedTableValidatorRule) Name() string {
	return "used-tables"
}

func (u UsedTableValidatorRule) IsFast() bool {
	return true
}

func (u UsedTableValidatorRule) GetApplicableLevels() []Level {
	return []Level{LevelPipeline, LevelAsset}
}

func (u UsedTableValidatorRule) GetSeverity() ValidatorSeverity {
	return ValidatorSeverityWarning
}

func (u UsedTableValidatorRule) Validate(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	return CallFuncForEveryAsset(u.ValidateAsset)(ctx, p)
}

func (u UsedTableValidatorRule) ValidateAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	assetRenderer, err := u.renderer.CloneForAsset(ctx, p, asset)
	if err != nil {
		return nil, err
	}

	missingDeps, err := u.parser.GetMissingDependenciesForAsset(asset, p, assetRenderer)
	if err != nil {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "failed to get missing dependencies: " + err.Error(),
		})
		return issues, nil //nolint
	}

	if len(missingDeps) == 0 {
		return issues, nil
	}

	issues = append(issues, &Issue{
		Task:        asset,
		Description: "There are some tables that are referenced in the query but not included in the 'depends' list.",
		Context:     missingDeps,
	})

	return issues, nil
}

func (u UsedTableValidatorRule) ValidateCrossPipeline(ctx context.Context, pipelines []*pipeline.Pipeline) ([]*Issue, error) {
	// This rule doesn't need cross-pipeline validation
	return []*Issue{}, nil
}

func ValidateVariables(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	if p.Variables == nil {
		return issues, nil
	}

	err := p.Variables.Validate()
	if err != nil {
		issues = append(issues, &Issue{
			Description: err.Error(),
		})
	}

	return issues, nil
}

func EnsurePipelineConcurrencyIsValid(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if p.Concurrency <= 0 {
		issues = append(issues, &Issue{
			Description: pipelineConcurrencyMustBePositive,
		})
	}

	return issues, nil
}

func EnsureAssetTierIsValidForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	if asset.Tier != 0 && (asset.Tier < 1 || asset.Tier > 5) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: assetTierMustBeBetweenOneAndFive,
		})
	}

	return issues, nil
}

func EnsureSecretMappingsHaveKeyForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	for _, m := range asset.Secrets {
		if strings.TrimSpace(m.SecretKey) == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: secretMappingKeyMustExist,
			})
		}
	}
	return issues, nil
}

// ValidateCrossPipelineURIDependencies validates all URI dependencies across all pipelines
// and returns warnings for any URI dependencies that cannot be resolved or duplicate URIs.
func ValidateCrossPipelineURIDependencies(ctx context.Context, pipelines []*pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	// Create a map of all available URIs across all pipelines and track duplicates
	availableURIs := make(map[string]*pipeline.Asset)
	uriToAssets := make(map[string][]*pipeline.Asset)

	for _, pl := range pipelines {
		for _, asset := range pl.Assets {
			if asset.URI != "" {
				availableURIs[asset.URI] = asset
				uriToAssets[asset.URI] = append(uriToAssets[asset.URI], asset)
			}
		}
	}

	// Check for duplicate URIs
	for uri, assets := range uriToAssets {
		if len(assets) > 1 {
			// Report duplicate URI issue for the first asset
			assetNames := make([]string, len(assets))
			for i, asset := range assets {
				assetNames[i] = asset.Name
			}
			issues = append(issues, &Issue{
				Task:        assets[0],
				Description: fmt.Sprintf("Duplicate URI '%s' found across multiple assets: %s", uri, strings.Join(assetNames, ", ")),
			})
		}
	}

	// Check each asset in all pipelines for URI dependencies
	for _, pl := range pipelines {
		for _, asset := range pl.Assets {
			for _, dep := range asset.Upstreams {
				if dep.Type != "uri" {
					continue
				}

				if dep.Value == "" {
					issues = append(issues, &Issue{
						Task:        asset,
						Description: "URI dependency cannot be empty",
					})
					continue
				}

				// Check if the URI exists in any of the available pipelines
				if _, exists := availableURIs[dep.Value]; !exists {
					issues = append(issues, &Issue{
						Task:        asset,
						Description: fmt.Sprintf("Cross-pipeline URI dependency '%s' not found in any available pipeline", dep.Value),
					})
				}
			}
		}
	}

	return issues, nil
}

// EnsureValidTimeWindow checks that the start date is before the end date
func EnsureValidTimeWindow(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	// Get start and end dates from context
	startDate, ok := ctx.Value(pipeline.RunConfigStartDate).(time.Time)
	if !ok {
		// If no start date in context, skip validation
		return issues, nil
	}

	endDate, ok := ctx.Value(pipeline.RunConfigEndDate).(time.Time)
	if !ok {
		// If no end date in context, skip validation
		return issues, nil
	}

	// Check if start date is after end date
	if startDate.After(endDate) {
		issues = append(issues, &Issue{
			Description: fmt.Sprintf("Start date (%s) must be before end date (%s)",
				startDate.Format("2006-01-02 15:04:05"),
				endDate.Format("2006-01-02 15:04:05")),
		})
	}

	return issues, nil
}
