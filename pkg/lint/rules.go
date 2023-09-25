package lint

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

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

	pipelineNameCannotBeEmpty        = "The pipeline name cannot be empty, it must be a valid name made of alphanumeric characters, dashes, dots and underscores"
	pipelineNameMustBeAlphanumeric   = "The pipeline name must be made of alphanumeric characters, dashes, dots and underscores"
	pipelineStartDateCannotBeEmpty   = "The start_date in the pipeline.yml file cannot be empty, it must be a valid datetime in 'YYYY-MM-DD' format, e.g. 2021-01-01"
	pipelineStartDateMustBeValidDate = "The start_date in the pipeline.yml file must be a valid datetime in 'YYYY-MM-DD' format, e.g. 2021-01-01"

	pipelineContainsCycle = "The pipeline has a cycle with dependencies, make sure there are no cyclic dependencies"

	pipelineSlackFieldEmptyName           = "Slack notifications must have a `name` attribute"
	pipelineSlackFieldEmptyConnection     = "Slack notifications must have a `connection` attribute"
	pipelineSlackNameFieldNotUnique       = "The `name` attribute under the Slack notifications must be unique"
	pipelineSlackConnectionFieldNotUnique = "The `connection` attribute under the Slack notifications must be unique"

	athenaSQLEmptyDatabaseField       = "The `database` parameter cannot be empty"
	athenaSQLInvalidS3FilePath        = "The `s3_file_path` parameter must start with `s3://`"
	athenaSQLMissingDatabaseParameter = "The `database` parameter is required for Athena SQL tasks"
	athenaSQLEmptyS3FilePath          = "The `s3_file_path` parameter cannot be empty"
)

var validIDRegexCompiled = regexp.MustCompile(validIDRegex)

func EnsureTaskNameIsValid(pipeline *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	for _, task := range pipeline.Assets {
		if task.Name == "" {
			issues = append(issues, &Issue{
				Task:        task,
				Description: taskNameMustExist,
			})

			continue
		}

		if match := validIDRegexCompiled.MatchString(task.Name); !match {
			issues = append(issues, &Issue{
				Task:        task,
				Description: taskNameMustBeAlphanumeric,
			})
		}
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

func EnsureExecutableFileIsValid(fs afero.Fs) PipelineValidator {
	return func(p *pipeline.Pipeline) ([]*Issue, error) {
		issues := make([]*Issue, 0)
		for _, task := range p.Assets {
			if task.DefinitionFile.Type == pipeline.CommentTask {
				continue
			}

			if task.ExecutableFile.Path == "" {
				if task.Type == pipeline.AssetTypePython {
					issues = append(issues, &Issue{
						Task:        task,
						Description: executableFileCannotBeEmpty,
					})
				}
				continue
			}

			fileInfo, err := fs.Stat(task.ExecutableFile.Path)
			if errors.Is(err, os.ErrNotExist) {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileDoesNotExist,
				})
				continue
			}

			if fileInfo.IsDir() {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileIsADirectory,
				})
				continue
			}

			if fileInfo.Size() == 0 {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileIsEmpty,
				})
			}

			if isFileExecutable(fileInfo.Mode()) {
				issues = append(issues, &Issue{
					Task:        task,
					Description: executableFileIsNotExecutable,
				})
			}
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

func EnsureStartDateIsValid(pipeline *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if pipeline.StartDate == "" {
		issues = append(issues, &Issue{
			Description: pipelineStartDateCannotBeEmpty,
		})

		return issues, nil
	}

	_, err := time.Parse("2006-01-02", pipeline.StartDate)
	if err != nil {
		issues = append(issues, &Issue{
			Description: pipelineStartDateMustBeValidDate,
		})
	}

	return issues, nil
}

func isFileExecutable(mode os.FileMode) bool {
	return mode&0o111 != 0
}

func EnsureDependencyExists(p *pipeline.Pipeline) ([]*Issue, error) {
	taskMap := map[string]bool{}
	for _, task := range p.Assets {
		if task.Name == "" {
			continue
		}

		taskMap[task.Name] = true
	}

	issues := make([]*Issue, 0)
	for _, task := range p.Assets {
		for _, dep := range task.DependsOn {
			if _, ok := taskMap[dep]; !ok {
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Dependency '%s' does not exist", dep),
				})
			}
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

func EnsureOnlyAcceptedTaskTypesAreThere(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	for _, task := range p.Assets {
		if task.Type == "" {
			issues = append(issues, &Issue{
				Task:        task,
				Description: taskTypeMustExist,
			})
			continue
		}

		if _, ok := executor.DefaultExecutorsV2[task.Type]; !ok {
			issues = append(issues, &Issue{
				Task:        task,
				Description: fmt.Sprintf("Invalid task type '%s'", task.Type),
			})
		}
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

func EnsureAthenaSQLTypeTasksHasDatabaseAndS3FilePath(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	for _, task := range p.Assets {
		if task.Type == "athena.sql" {
			databaseVar, ok := task.Parameters["database"]

			if !ok {
				issues = append(issues, &Issue{
					Task:        task,
					Description: athenaSQLMissingDatabaseParameter,
				})
			}

			if ok && databaseVar == "" {
				issues = append(issues, &Issue{
					Task:        task,
					Description: athenaSQLEmptyDatabaseField,
				})
			}

			s3FilePathVar, ok := task.Parameters["s3_file_path"]

			if !ok {
				issues = append(issues, &Issue{
					Task:        task,
					Description: athenaSQLEmptyS3FilePath,
				})
			}

			if ok && !strings.HasPrefix(s3FilePathVar, "s3://") {
				issues = append(issues, &Issue{
					Task:        task,
					Description: athenaSQLInvalidS3FilePath,
					Context:     []string{fmt.Sprintf("Given `s3_file_path` is: %s", s3FilePathVar)},
				})
			}
		}
	}

	return issues, nil
}

func EnsureSlackFieldInPipelineIsValid(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	slackNames := make([]string, 0, len(p.Notifications.Slack))
	slackConnections := make([]string, 0)
	for _, slack := range p.Notifications.Slack {
		if slack.Name == "" {
			issues = append(issues, &Issue{
				Description: pipelineSlackFieldEmptyName,
			})
		}

		if slack.Connection == "" {
			issues = append(issues, &Issue{
				Description: pipelineSlackFieldEmptyConnection,
			})
		}

		if isStringInArray(slackNames, slack.Name) {
			issues = append(issues, &Issue{
				Description: pipelineSlackNameFieldNotUnique,
			})
		}
		if slack.Name != "" {
			slackNames = append(slackNames, slack.Name)
		}

		if isStringInArray(slackConnections, slack.Connection) {
			issues = append(issues, &Issue{
				Description: pipelineSlackConnectionFieldNotUnique,
			})
		}
		if slack.Connection != "" {
			slackConnections = append(slackConnections, slack.Connection)
		}
	}

	return issues, nil
}
