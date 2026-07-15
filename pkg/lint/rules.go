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
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/bruin-data/bruin/pkg/tablename"
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

	slackChannelEmpty          = "Slack notifications must have a `channel` attribute"
	slackChannelNotUnique      = "The `channel` attribute under the Slack notifications must be unique"
	msTeamsConnectionEmpty     = "MS Teams notifications `connection` attribute must not be empty"
	msTeamsConnectionNotUnique = "The `connection` attribute under the MS Teams notifications must be unique"
	discordConnectionEmpty     = "Discord notifications `connection` attribute must not be empty"
	discordConnectionNotUnique = "The `connection` attribute under the Discord notifications must be unique"
	emailRecipientsEmpty       = "Email notifications must have at least one recipient"
	emailRecipientEmpty        = "Email notification recipients must not be empty"
	emailRecipientsNotUnique   = "The `recipients` attribute under the email notifications must be unique"

	pipelineConcurrencyMustBePositive    = "Pipeline concurrency must be 1 or greater"
	pipelineMaxActiveStepsMustBePositive = "Pipeline max_active_steps must be a positive number"
	assetTierMustBeBetweenOneAndFive     = "Asset tier must be between 1 and 5"
	secretMappingKeyMustExist            = "Secrets must have a `key` attribute"

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
	issues := make([]*Issue, 0, 1)
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

		value, exists := asset.Parameters.GetString(key)
		if !exists || value == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Ingestr assets require the following parameters: " + strings.Join(requiredKeys, ", "),
			})

			return issues, nil
		}
	}

	effectiveStrategy := ""
	if value, exists := asset.Parameters.GetString("incremental_strategy"); exists && value != "" {
		if !python.IsIngestrStrategySupported(value) {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Incremental strategy '%s' is not supported for ingestr assets. Supported strategies are: %s", value, python.GetSupportedIngestrStrategiesString()),
			})
		} else {
			effectiveStrategy = value
		}
	}

	materializationIssues, materializationStrategy := validateIngestrMaterialization(asset, effectiveStrategy)
	issues = append(issues, materializationIssues...)
	if materializationStrategy != "" {
		effectiveStrategy = materializationStrategy
	}
	if cdcVal, _ := asset.Parameters.GetString("cdc"); cdcVal == "true" && effectiveStrategy != "" && effectiveStrategy != "merge" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "CDC ingestr assets require incremental strategy 'merge'",
		})
	}

	updateOnMergeKeys := asset.ColumnNamesWithUpdateOnMerge()
	if len(updateOnMergeKeys) > 0 {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Ingestr assets do not support the 'update_on_merge' field, the strategy used decide the update behavior",
		})
	}
	if mode, exists := asset.Parameters.GetString("cdc_mode"); exists && mode != "stream" && mode != "batch" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Invalid 'cdc_mode' value: must be 'stream' or 'batch'",
		})
	}
	if v, exists := asset.Parameters.GetString("version"); exists && v != "" && !ingestrVersionPattern.MatchString(v) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Invalid 'version' value %q: must be 'vMAJOR' or fully-qualified 'vMAJOR.MINOR.PATCH'", v),
		})
	}
	if effectiveStrategy == "merge" {
		// Skip PK validation for CDC mode - PKs are determined by the source
		if cdcVal, _ := asset.Parameters.GetString("cdc"); cdcVal != "true" {
			primaryKeys := asset.ColumnNamesWithPrimaryKey()
			if len(primaryKeys) == 0 {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Materialization strategy 'merge' requires the 'primary_key' field to be set on at least one column",
				})
			}
		}
	}

	return issues, nil
}

// WarnIngestrCDCStreamParameter nudges CDC assets towards a single streaming
// switch. On a CDC asset, streaming is controlled by cdc_mode: stream, which
// enables ingestr's --stream flag on its own; setting the generic stream
// parameter as well is redundant (and stream: true alongside cdc_mode: batch is
// contradictory).
func WarnIngestrCDCStreamParameter(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type != pipeline.AssetTypeIngestr || asset.Parameters == nil {
		return issues, nil
	}

	if cdc, _ := asset.Parameters.GetString("cdc"); cdc != "true" {
		return issues, nil
	}

	if _, exists := asset.Parameters.GetString("stream"); !exists {
		return issues, nil
	}

	issues = append(issues, &Issue{
		Task:        asset,
		Description: "The 'stream' parameter is redundant on a CDC asset; use 'cdc_mode: stream' to stream (or 'cdc_mode: batch' for a bounded run)",
	})

	return issues, nil
}

func validateIngestrMaterialization(asset *pipeline.Asset, effectiveStrategy string) ([]*Issue, string) {
	issues := make([]*Issue, 0)
	mat := asset.Materialization
	if mat.Type == pipeline.MaterializationTypeNone {
		return issues, ""
	}

	if mat.Type != pipeline.MaterializationTypeTable {
		return []*Issue{{
			Task:        asset,
			Description: "Ingestr assets only support materialization type 'table'",
		}}, ""
	}

	materializationStrategy := ""
	if mat.Strategy != pipeline.MaterializationStrategyNone {
		strategy, ok := python.TranslateBruinMaterializationStrategyToIngestr(mat.Strategy)
		if !ok {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Materialization strategy '%s' is not supported for ingestr assets. Supported strategies are: %s", mat.Strategy, python.GetSupportedIngestrMaterializationStrategiesString()),
			})
		} else {
			materializationStrategy = strategy
			effectiveStrategy = strategy
			issues = append(issues, validateIngestrMaterializationConflict(asset, "incremental_strategy", strategy, "materialization.strategy")...)
		}
	}

	if hasIngestrMaterializationIncrementalKey(asset, mat) && !python.IsIngestrIncrementalKeyStrategy(effectiveStrategy) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Materialization incremental key is only supported for append, merge, and delete+insert strategies on ingestr assets",
		})
	}

	issues = append(issues, validateIngestrMaterializationConflict(asset, "incremental_key", mat.IncrementalKey, "materialization.incremental_key")...)
	issues = append(issues, validateIngestrMaterializationConflict(asset, "partition_by", mat.PartitionBy, "materialization.partition_by")...)
	issues = append(issues, validateIngestrMaterializationConflict(asset, "cluster_by", strings.Join(mat.ClusterBy, ","), "materialization.cluster_by")...)

	return issues, materializationStrategy
}

func hasIngestrMaterializationIncrementalKey(asset *pipeline.Asset, mat pipeline.Materialization) bool {
	if strings.TrimSpace(mat.IncrementalKey) != "" {
		return true
	}
	key, _ := asset.Parameters.GetString("incremental_key")
	return strings.TrimSpace(key) != ""
}

func validateIngestrMaterializationConflict(asset *pipeline.Asset, key, value, source string) []*Issue {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	current, exists := asset.Parameters.GetString(key)
	if !exists || strings.TrimSpace(current) == "" {
		return nil
	}

	if normalizeIngestrMaterializationValue(key, current) == normalizeIngestrMaterializationValue(key, value) {
		return nil
	}

	return []*Issue{{
		Task:        asset,
		Description: fmt.Sprintf("Ingestr asset defines both parameters.%s=%q and %s=%q", key, current, source, value),
	}}
}

func normalizeIngestrMaterializationValue(key, value string) string {
	value = strings.TrimSpace(value)
	if key != "cluster_by" {
		return value
	}

	parts := strings.Split(value, ",")
	cleaned := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			cleaned = append(cleaned, part)
		}
	}
	return strings.Join(cleaned, ",")
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

func EnsureAssetStartDateIsValid(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.StartDate == "" {
		return issues, nil
	}

	_, err := time.Parse("2006-01-02", asset.StartDate)
	if err != nil {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("start_date must be in the format of YYYY-MM-DD in the asset definition, '%s' given", asset.StartDate),
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

	if asset.Materialization.Strategy == "" || asset.Materialization.Strategy == pipeline.MaterializationStrategyNone {
		return issues, nil
	}

	if !python.IsPythonMaterializationStrategySupported(asset.Materialization.Strategy) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Materialization strategy '%s' is not supported for Python assets. Supported strategies are: %s", asset.Materialization.Strategy, python.GetSupportedPythonStrategiesString()),
		})
	}

	return issues, nil
}

func ValidateScriptAssetHooksUnsupported(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0, 1)

	if asset.Type != pipeline.AssetTypePython && asset.Type != pipeline.AssetTypeR {
		return issues, nil
	}

	if asset.Hooks.IsZero() {
		return issues, nil
	}

	assetTypeLabel := "Python"
	if asset.Type == pipeline.AssetTypeR {
		assetTypeLabel = "R"
	}

	issues = append(issues, &Issue{
		Task:        asset,
		Description: fmt.Sprintf("Hooks are currently supported only for SQL assets. Hooks defined on %s assets are ignored during execution.", assetTypeLabel),
	})

	return issues, nil
}

func ValidateDefaultHookApplicableTypes(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if p.DefaultValues == nil {
		return issues, nil
	}

	for _, t := range p.DefaultValues.Hooks.ApplicableTypes {
		if !pipeline.IsSQLAssetType(pipeline.AssetType(t)) {
			issues = append(issues, &Issue{
				Description: fmt.Sprintf("Invalid applicable_type %q in default hooks: hooks are only supported for SQL asset types", t),
			})
		}
	}

	return issues, nil
}

func WarnAssetHookApplicableTypeIgnored(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0, 1)

	if len(asset.Hooks.ApplicableTypes) == 0 {
		return issues, nil
	}

	issues = append(issues, &Issue{
		Task:        asset,
		Description: "applicable_type has no effect on asset-level hooks; it only filters which asset types inherit pipeline default hooks.",
	})

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
		seedPath, _ := asset.Parameters.GetString("path")
		if seedPath == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Seed file path is required",
			})
			return issues, nil
		}

		lowerPath := strings.ToLower(seedPath)
		if strings.HasPrefix(lowerPath, "http://") || strings.HasPrefix(lowerPath, "https://") {
			// URL seeds are validated at runtime by ingestr, skip local file checks
			return issues, nil
		}

		seedFileTypeRaw, _ := asset.Parameters.GetString("file_type")
		fileType := strings.ToLower(strings.TrimSpace(seedFileTypeRaw))
		if fileType != "" && !supportedSeedFileTypes[fileType] {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Unsupported seed file_type %q (supported: csv, parquet, pq, json, jsonl, ndjson, avro)", seedFileTypeRaw),
			})
			return issues, nil
		}

		seedFilePath := filepath.Join(filepath.Dir(asset.DefinitionFile.Path), seedPath)
		_, err := os.Stat(seedFilePath)
		if os.IsNotExist(err) {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Seed file does not exist or cannot be found",
			})
			return issues, nil
		}

		if !isCSVSeed(seedPath, seedFileTypeRaw) {
			// For parquet/json/jsonl/ndjson/avro the schema is binary or
			// semi-structured; column-vs-header validation is left to ingestr
			// at runtime, just like the URL case above.
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

// supportedSeedFileTypes is the set of valid file_type values for seed assets.
// Must stay in sync with seedFileSchemes in pkg/ingestr/operator.go.
var supportedSeedFileTypes = map[string]bool{
	"csv":     true,
	"parquet": true,
	"pq":      true,
	"jsonl":   true,
	"ndjson":  true,
	"json":    true,
	"avro":    true,
}

// isCSVSeed reports whether the seed should be treated as a CSV file for the
// purpose of header/column validation. An explicit file_type parameter wins;
// otherwise the file extension decides.
func isCSVSeed(seedPath, fileType string) bool {
	if ft := strings.ToLower(strings.TrimSpace(fileType)); ft != "" {
		return ft == "csv"
	}
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(seedPath)), ".")
	switch ext {
	case "parquet", "pq", "jsonl", "ndjson", "json", "avro":
		return false
	}
	return true
}

var arnPattern = regexp.MustCompile(`^arn:[^:\n]*:[^:\n]*:[^:\n]*:[^:\n]*:(?:[^:\/\n]*[:\/])?.*$`)

// ingestrVersionPattern matches the bare family marker (vMAJOR) or a fully-qualified vMAJOR.MINOR.PATCH. MAJOR has no leading zero (other than the literal "0").
var ingestrVersionPattern = regexp.MustCompile(`^v(0|[1-9]\d*)(\.\d+\.\d+)?$`)

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
		rawVal, _ := asset.Parameters.GetString(key)
		value := strings.TrimSpace(rawVal)
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

	timeoutRaw, _ := asset.Parameters.GetString("timeout")
	timeoutSpec := strings.TrimSpace(timeoutRaw)
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

	executionRoleRaw, _ := asset.Parameters.GetString("execution_role")
	executionRole := strings.TrimSpace(executionRoleRaw)
	if executionRole != "" && !arnPattern.MatchString(executionRole) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "parameters.execution_role must be an Amazon Resource Name (ARN)",
		})
	}

	logLocationRaw, _ := asset.Parameters.GetString("logs")
	logLocation := strings.TrimSpace(logLocationRaw)
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

// ValidateColumnMetadata checks that optional column metadata fields, when set,
// are well-formed: foreign keys must reference an existing asset (and, when that
// asset declares its columns, an existing column on it), and numeric type-detail
// (precision/scale/length) must hold sane values.
func ValidateColumnMetadata(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0, len(asset.Columns))

	for _, column := range asset.Columns {
		issues = append(issues, validateColumnForeignKey(p, asset, &column)...)
		issues = append(issues, validateColumnTypeDetail(asset, &column)...)
	}

	return issues, nil
}

func validateColumnForeignKey(p *pipeline.Pipeline, asset *pipeline.Asset, column *pipeline.Column) []*Issue {
	fk := column.ForeignKey
	if fk == nil {
		return nil
	}

	issues := make([]*Issue, 0)

	if fk.Table == "" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Column '%s' has a foreign key without a referenced table", column.Name),
		})
	}
	if fk.Column == "" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Column '%s' has a foreign key without a referenced column", column.Name),
		})
	}
	if fk.Table == "" || fk.Column == "" {
		return issues
	}

	referenced := p.GetAssetByName(fk.Table)
	if referenced == nil {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Column '%s' has a foreign key referencing asset '%s', which does not exist in the pipeline", column.Name, fk.Table),
		})
		return issues
	}

	// Columns are optional in Bruin, so we can only verify the referenced column
	// when the target asset actually declares its columns; otherwise we have no
	// schema to check against and skip rather than emit a false positive.
	if len(referenced.Columns) > 0 && referenced.GetColumnWithName(fk.Column) == nil {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Column '%s' has a foreign key referencing column '%s.%s', which does not exist", column.Name, fk.Table, fk.Column),
		})
	}

	return issues
}

func validateColumnTypeDetail(asset *pipeline.Asset, column *pipeline.Column) []*Issue {
	issues := make([]*Issue, 0)

	if column.Precision != nil && *column.Precision <= 0 {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Column '%s' has an invalid precision '%d'; it must be a positive integer", column.Name, *column.Precision),
		})
	}
	if column.Length != nil && *column.Length <= 0 {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Column '%s' has an invalid length '%d'; it must be a positive integer", column.Name, *column.Length),
		})
	}
	if column.Scale != nil && *column.Scale < 0 {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Column '%s' has an invalid scale '%d'; it must not be negative", column.Name, *column.Scale),
		})
	}
	// Only compare scale against precision when precision is itself valid;
	// otherwise the user already gets the more actionable "invalid precision" error.
	if column.Precision != nil && *column.Precision > 0 && column.Scale != nil && *column.Scale > *column.Precision {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Column '%s' has a scale '%d' greater than its precision '%d'", column.Name, *column.Scale, *column.Precision),
		})
	}

	return issues
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

// validateNotifications checks a single Notifications value for empty/duplicate targets.
// issueContext is an optional label (e.g. "custom_check:my_check") included in issues for traceability.
func validateNotifications(n *pipeline.Notifications, issueContext []string) []*Issue {
	if n == nil {
		return nil
	}

	var issues []*Issue

	slackChannels := make([]string, 0, len(n.Slack))
	for _, s := range n.Slack {
		ch := strings.TrimPrefix(s.Channel, "#")
		if ch == "" {
			issues = append(issues, &Issue{Description: slackChannelEmpty, Context: issueContext})
			continue
		}
		if slices.Contains(slackChannels, ch) {
			issues = append(issues, &Issue{Description: slackChannelNotUnique, Context: issueContext})
		}
		slackChannels = append(slackChannels, ch)
	}

	issues = append(issues, validateConnectionKeys(
		mapSlice(n.MSTeams, func(t pipeline.MSTeamsNotification) string { return t.Connection }),
		msTeamsConnectionEmpty, msTeamsConnectionNotUnique, issueContext,
	)...)
	issues = append(issues, validateConnectionKeys(
		mapSlice(n.Discord, func(d pipeline.DiscordNotification) string { return d.Connection }),
		discordConnectionEmpty, discordConnectionNotUnique, issueContext,
	)...)

	emailRecipientGroups := make([]string, 0, len(n.Email))
	for _, email := range n.Email {
		if len(email.Recipients) == 0 {
			issues = append(issues, &Issue{Description: emailRecipientsEmpty, Context: issueContext})
			continue
		}

		recipients := make([]string, 0, len(email.Recipients))
		for _, recipient := range email.Recipients {
			recipient = strings.TrimSpace(recipient)
			if recipient == "" {
				issues = append(issues, &Issue{Description: emailRecipientEmpty, Context: issueContext})
				continue
			}
			recipients = append(recipients, recipient)
		}

		key := strings.Join(recipients, "\x00")
		if slices.Contains(emailRecipientGroups, key) {
			issues = append(issues, &Issue{Description: emailRecipientsNotUnique, Context: issueContext})
		}
		emailRecipientGroups = append(emailRecipientGroups, key)
	}

	return issues
}

func mapSlice[T any](items []T, key func(T) string) []string {
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = key(item)
	}
	return out
}

// validateConnectionKeys checks a list of connection keys for empty/duplicate values.
func validateConnectionKeys(keys []string, emptyMsg, dupeMsg string, issueContext []string) []*Issue {
	var issues []*Issue
	var seen []string
	for _, k := range keys {
		if k == "" {
			issues = append(issues, &Issue{Description: emptyMsg, Context: issueContext})
			continue
		}
		if slices.Contains(seen, k) {
			issues = append(issues, &Issue{Description: dupeMsg, Context: issueContext})
			continue
		}
		seen = append(seen, k)
	}
	return issues
}

// EnsurePipelineNotificationsAreValid validates all notification targets on the pipeline.
func EnsurePipelineNotificationsAreValid(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := validateNotifications(&p.Notifications, nil)
	if issues == nil {
		return []*Issue{}, nil
	}
	return issues, nil
}

// EnsureAssetNotificationsAreValid validates notification targets on the asset,
// its custom checks, and its column checks.
func EnsureAssetNotificationsAreValid(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	var issues []*Issue

	issues = append(issues, validateNotifications(asset.Notifications, nil)...)

	for _, check := range asset.CustomChecks {
		issues = append(issues, validateNotifications(check.Notifications, []string{"custom_check:" + check.Name})...)
	}

	for _, col := range asset.Columns {
		for _, check := range col.Checks {
			issues = append(issues, validateNotifications(check.Notifications, []string{col.Name + "." + check.Name})...)
		}
	}

	if len(issues) == 0 {
		return []*Issue{}, nil
	}
	return issues, nil
}

func EnsureMaterializationValuesAreValidForSingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type == pipeline.AssetTypePython || asset.Type == pipeline.AssetTypeIngestr {
		return issues, nil
	}

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
			asset.Materialization.Strategy != pipeline.MaterializationStrategyDeleteInsert && asset.Materialization.Strategy != pipeline.MaterializationStrategyTimeInterval && asset.Materialization.Strategy != pipeline.MaterializationStrategySCD2ByTime && asset.Materialization.Strategy != pipeline.MaterializationStrategySCD2ByColumn {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: "Incremental key is only supported with 'delete+insert', 'time_interval', 'scd2_by_time' and 'scd2_by_column' strategies.",
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

		case pipeline.MaterializationStrategyDataVaultHub:
			issues = append(issues, ensureDataVaultHubColumnsAreValid(asset)...)
		case pipeline.MaterializationStrategyDataVaultLink:
			issues = append(issues, ensureDataVaultLinkColumnsAreValid(asset)...)
		case pipeline.MaterializationStrategyDataVaultSatellite:
			issues = append(issues, ensureDataVaultSatelliteColumnsAreValid(asset)...)
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

func ensureDataVaultHubColumnsAreValid(asset *pipeline.Asset) []*Issue {
	issues := ensureDataVaultColumnsHaveNamesAndTypes(asset, "datavault_hub")
	if len(asset.Columns) == 0 {
		return issues
	}

	if !hasDataVaultColumn(asset, []string{"hash_key", "hub_hash_key"}, func(col pipeline.Column) bool {
		return col.PrimaryKey || strings.HasSuffix(strings.ToLower(col.Name), "_hk")
	}) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Materialization strategy 'datavault_hub' requires a hash key column",
		})
	}
	if !hasDataVaultColumn(asset, []string{"business_key"}, func(col pipeline.Column) bool {
		return strings.HasSuffix(strings.ToLower(col.Name), "_bk")
	}) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Materialization strategy 'datavault_hub' requires at least one business key column",
		})
	}
	appendCommonDataVaultColumnIssues(asset, "datavault_hub", &issues)
	return issues
}

func ensureDataVaultLinkColumnsAreValid(asset *pipeline.Asset) []*Issue {
	issues := ensureDataVaultColumnsHaveNamesAndTypes(asset, "datavault_link")
	if len(asset.Columns) == 0 {
		return issues
	}

	linkHashKeyName := ""
	for _, col := range asset.Columns {
		if dataVaultColumnMatches(col, []string{"link_hash_key", "hash_key"}) || col.PrimaryKey || strings.HasSuffix(strings.ToLower(col.Name), "_hk") {
			linkHashKeyName = col.Name
			break
		}
	}
	if linkHashKeyName == "" {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Materialization strategy 'datavault_link' requires a link hash key column",
		})
	}

	hasRelatedHashKey := false
	for _, col := range asset.Columns {
		if strings.EqualFold(col.Name, linkHashKeyName) {
			continue
		}
		if dataVaultColumnMatches(col, []string{"hub_hash_key", "parent_hash_key", "foreign_hash_key"}) || strings.HasSuffix(strings.ToLower(col.Name), "_hk") {
			hasRelatedHashKey = true
			break
		}
	}
	if !hasRelatedHashKey {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Materialization strategy 'datavault_link' requires at least one related hash key column",
		})
	}
	appendCommonDataVaultColumnIssues(asset, "datavault_link", &issues)
	return issues
}

func ensureDataVaultSatelliteColumnsAreValid(asset *pipeline.Asset) []*Issue {
	issues := ensureDataVaultColumnsHaveNamesAndTypes(asset, "datavault_satellite")
	if len(asset.Columns) == 0 {
		return issues
	}

	if !hasDataVaultColumn(asset, []string{"parent_hash_key", "hub_hash_key", "hash_key"}, func(col pipeline.Column) bool {
		return col.PrimaryKey || strings.HasSuffix(strings.ToLower(col.Name), "_hk")
	}) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Materialization strategy 'datavault_satellite' requires a parent hash key column",
		})
	}
	if !hasDataVaultColumn(asset, []string{"hashdiff", "hash_diff"}, func(col pipeline.Column) bool {
		name := strings.ToLower(col.Name)
		return name == "hashdiff" || name == "hash_diff"
	}) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Materialization strategy 'datavault_satellite' requires a hashdiff column",
		})
	}
	appendCommonDataVaultColumnIssues(asset, "datavault_satellite", &issues)
	return issues
}

func ensureDataVaultColumnsHaveNamesAndTypes(asset *pipeline.Asset, strategy string) []*Issue {
	issues := make([]*Issue, 0)
	if len(asset.Columns) == 0 {
		return append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Materialization strategy '%s' requires the 'columns' field to be set with actual columns", strategy),
		})
	}

	for _, col := range asset.Columns {
		if strings.TrimSpace(col.Name) == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Materialization strategy '%s' requires every column to have a name", strategy),
			})
		}
		if strings.TrimSpace(col.Type) == "" {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Materialization strategy '%s' requires column '%s' to have a type", strategy, col.Name),
			})
		}
	}

	return issues
}

func appendCommonDataVaultColumnIssues(asset *pipeline.Asset, strategy string, issues *[]*Issue) {
	if !hasDataVaultColumn(asset, []string{"load_datetime", "load_dts"}, func(col pipeline.Column) bool {
		switch strings.ToLower(col.Name) {
		case "load_dts", "load_datetime", "loaded_at":
			return true
		default:
			return false
		}
	}) {
		*issues = append(*issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Materialization strategy '%s' requires a load datetime column", strategy),
		})
	}
	if !hasDataVaultColumn(asset, []string{"record_source"}, func(col pipeline.Column) bool {
		return strings.EqualFold(col.Name, "record_source")
	}) {
		*issues = append(*issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Materialization strategy '%s' requires a record source column", strategy),
		})
	}
}

func hasDataVaultColumn(asset *pipeline.Asset, roles []string, fallback func(pipeline.Column) bool) bool {
	for _, col := range asset.Columns {
		if dataVaultColumnMatches(col, roles) {
			return true
		}
	}
	if fallback == nil {
		return false
	}
	for _, col := range asset.Columns {
		if fallback(col) {
			return true
		}
	}
	return false
}

func dataVaultColumnMatches(col pipeline.Column, roles []string) bool {
	if len(col.Meta) == 0 {
		return false
	}
	role := strings.ToLower(strings.TrimSpace(col.Meta["datavault_role"]))
	for _, candidate := range roles {
		if role == strings.ToLower(candidate) {
			return true
		}
	}
	return false
}

func EnsureSnowflakeSensorHasQueryParameterForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type != pipeline.AssetTypeSnowflakeQuerySensor {
		return issues, nil
	}

	query, ok := asset.Parameters.GetString("query")
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
	pipeline.AssetTypeMySQLTableSensor:      true,
	pipeline.AssetTypeDorisTableSensor:      true,
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
	pipeline.AssetTypeMySQLTableSensor:      "MySQL",
	pipeline.AssetTypeDorisTableSensor:      "Doris",
}

func ValidateTableSensorTableParameter(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if !TableSensorAllowedAssetTypes[asset.Type] {
		return issues, nil
	}

	table, ok := asset.Parameters.GetString("table")
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

// validateTableNameFormat validates a table sensor's `table` parameter against
// the platform's table-name capability (the single source of truth shared with
// asset-name validation, see EnsureAssetNameComponentCountIsValid). It returns an
// empty string when the name is valid.
func validateTableNameFormat(assetType pipeline.AssetType, tableName string) string {
	platformName := platformNames[assetType]

	platformKey, ok := pipeline.AssetTypeConnectionMapping[assetType]
	if !ok {
		return fmt.Sprintf("Table sensor is not supported for this asset type %s", assetType)
	}
	capability, ok := tablename.For(platformKey)
	if !ok {
		return fmt.Sprintf("Table sensor is not supported for this asset type %s", assetType)
	}

	for _, component := range strings.Split(tableName, ".") {
		if component == "" {
			return fmt.Sprintf("%s table sensor `table` parameter contains empty components, '%s' given", platformName, tableName)
		}
	}

	if err := capability.CheckName(tableName); err != nil {
		return fmt.Sprintf("%s table sensor `table` parameter must be in format %s, '%s' given", platformName, capability.FormatDesc, tableName)
	}

	return ""
}

// EnsureAssetNameComponentCountIsValid validates that an asset's own name has a
// component count supported by its target platform (e.g. rejecting
// `catalog.schema.table` on Postgres, which has no third level). It no-ops for
// asset types with no table-name capability (Python, ingestr, EMR/Spark, etc.),
// keeping non-database assets out of scope.
func EnsureAssetNameComponentCountIsValid(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	if asset.Name == "" {
		// Emptiness is reported by the task-name-valid rule.
		return issues, nil
	}

	platformKey, ok := pipeline.AssetTypeConnectionMapping[asset.Type]
	if !ok {
		return issues, nil
	}
	capability, ok := tablename.For(platformKey)
	if !ok {
		return issues, nil
	}

	if err := capability.CheckName(asset.Name); err != nil {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Asset name '%s' is not valid for %s: it must be in format %s", asset.Name, platformKey, capability.FormatDesc),
		})
	}

	return issues, nil
}

// ValidateSensorTimeout validates the optional `timeout` parameter on sensor
// assets. It uses the same single-unit duration syntax as pipeline
// interval_modifiers (s, m, h, d, ms, ns); combinators like "1h30m" and
// month suffix "M" are not supported.
func ValidateSensorTimeout(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if !strings.Contains(string(asset.Type), ".sensor.") {
		return issues, nil
	}

	raw, ok := asset.Parameters.GetString("timeout")
	if !ok || strings.TrimSpace(raw) == "" {
		return issues, nil
	}

	if _, err := helpers.ParseSensorDuration(raw); err != nil {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "parameters.timeout is invalid: " + err.Error(),
		})
	}

	return issues, nil
}

func EnsureBigQueryQuerySensorHasQueryParameterForASingleAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type != pipeline.AssetTypeBigqueryQuerySensor {
		return issues, nil
	}

	query, ok := asset.Parameters.GetString("query")
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
			valid, err := validatorInstance.IsValid(query.WithQueryType(ctx, query.QueryTypeDryRun), q)
			if err != nil {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: fmt.Sprintf("Failed to validate custom check query '%s': %s", check.Name, err),
				})
			} else if !valid {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: "Custom check query is invalid:" + renderedQuery,
				})
			}
		}
		return issues, nil
	}
}

// ValidateHookQueryDryRun validates SQL asset hooks using a dry-run against the target DB.
func ValidateHookQueryDryRun(connections connectionManager) AssetValidator {
	return func(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		var issues []*Issue

		if !asset.IsSQLAsset() || asset.Hooks.IsZero() {
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

		validateHook := func(position string, index int, hookQuery string) {
			normalized := strings.TrimSpace(hookQuery)
			if normalized == "" {
				return
			}

			q := &query.Query{Query: normalized}
			valid, err := validatorInstance.IsValid(query.WithQueryType(ctx, query.QueryTypeDryRun), q)
			if err != nil {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: fmt.Sprintf("Failed to validate %s hook query #%d: %s", position, index, err),
				})
				return
			}

			if !valid {
				issues = append(issues, &Issue{
					Task:        asset,
					Description: fmt.Sprintf("%s hook query #%d is invalid: %s", position, index, normalized),
				})
			}
		}

		for i, hook := range asset.Hooks.Pre {
			validateHook("pre", i+1, hook.Query)
		}

		for i, hook := range asset.Hooks.Post {
			validateHook("post", i+1, hook.Query)
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

type UsedTableValidatorRule struct {
	renderer jinja.RendererInterface
	parser   sqlparser.Parser
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

func EnsurePipelineMaxActiveStepsIsValid(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if p.MaxActiveSteps != nil && *p.MaxActiveSteps <= 0 {
		issues = append(issues, &Issue{
			Description: pipelineMaxActiveStepsMustBePositive,
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

var intervalRenderer = jinja.NewRendererWithYesterday("some-pipeline", "some-run-id")

func EnsureTimeIntervalIsValidForAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	validationCtx := context.WithValue(ctx, pipeline.RunConfigApplyIntervalModifiers, true)

	r, err := intervalRenderer.CloneForAsset(validationCtx, p, asset)
	if err != nil {
		return nil, err
	}

	renderedStartDate, err := r.Render("{{ start_datetime }}")
	if err != nil {
		return nil, err
	}

	renderedEndDate, err := r.Render("{{ end_datetime }}")
	if err != nil {
		return nil, err
	}

	parsedStartDate, err := time.Parse("2006-01-02T15:04:05", renderedStartDate)
	if err != nil {
		return nil, err
	}

	parsedEndDate, err := time.Parse("2006-01-02T15:04:05", renderedEndDate)
	if err != nil {
		return nil, err
	}

	if parsedStartDate.After(parsedEndDate) {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("start date %v is after end date %v for asset %v", parsedStartDate, parsedEndDate, asset.Name),
		})
	}

	return issues, nil
}

type validateUnknownYAMLFields struct {
	fs afero.Fs
}

// unknownFieldIssues filters a strict-decode error to only the lines about unknown fields,
// ignoring type mismatches and other YAML errors that are handled elsewhere.
func unknownFieldIssues(err error, task *pipeline.Asset) []*Issue {
	if err == nil {
		return nil
	}

	var issues []*Issue
	for _, line := range strings.Split(err.Error(), "\n") {
		line = strings.TrimSpace(line)
		if strings.Contains(line, "not found in type") {
			issues = append(issues, &Issue{
				Task:        task,
				Description: line,
			})
		}
	}

	return issues
}

func (v *validateUnknownYAMLFields) ValidatePipeline(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	if p.DefinitionFile.Path == "" {
		return nil, nil
	}

	data, err := afero.ReadFile(v.fs, p.DefinitionFile.Path)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read pipeline file at %s", p.DefinitionFile.Path)
	}

	var target pipeline.Pipeline
	strictErr := path.ConvertYamlToObjectStrict(data, &target)

	return unknownFieldIssues(strictErr, nil), nil
}

func (v *validateUnknownYAMLFields) ValidateAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	if asset.DefinitionFile.Path == "" {
		return nil, nil
	}

	strictErr := pipeline.ValidateAssetYAML(v.fs, asset.DefinitionFile.Path, asset.DefinitionFile.Type)

	return unknownFieldIssues(strictErr, asset), nil
}
