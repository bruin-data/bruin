package lint

import (
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

func GetRules(logger *zap.SugaredLogger, fs afero.Fs) ([]Rule, error) {
	rules := []Rule{
		&SimpleRule{
			Identifier: "task-name-valid",
			Validator:  EnsureTaskNameIsValid,
		},
		&SimpleRule{
			Identifier: "task-name-unique",
			Validator:  EnsureTaskNameIsUnique,
		},
		&SimpleRule{
			Identifier: "dependency-exists",
			Validator:  EnsureDependencyExists,
		},
		&SimpleRule{
			Identifier: "valid-executable-file",
			Validator:  EnsureExecutableFileIsValid(fs),
		},
		&SimpleRule{
			Identifier: "valid-pipeline-schedule",
			Validator:  EnsurePipelineScheduleIsValidCron,
		},
		&SimpleRule{
			Identifier: "valid-pipeline-name",
			Validator:  EnsurePipelineNameIsValid,
		},
		&SimpleRule{
			Identifier: "valid-task-type",
			Validator:  EnsureOnlyAcceptedTaskTypesAreThere,
		},
		&SimpleRule{
			Identifier: "acyclic-pipeline",
			Validator:  EnsurePipelineHasNoCycles,
		},
		&SimpleRule{
			Identifier: "valid-task-schedule",
			Validator:  EnsureTaskScheduleIsValid,
		},
		&SimpleRule{
			Identifier: "valid-athena-sql-task",
			Validator:  EnsureAthenaSQLTypeTasksHasDatabaseAndS3FilePath,
		},
		&SimpleRule{
			Identifier: "valid-slack-notification",
			Validator:  EnsureSlackFieldInPipelineIsValid,
		},
		&SimpleRule{
			Identifier: "valid-start-date",
			Validator:  EnsureStartDateIsValid,
		},
	}

	logger.Debugf("successfully loaded %d rules", len(rules))

	return rules, nil
}
