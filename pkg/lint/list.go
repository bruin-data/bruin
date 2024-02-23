package lint

import (
	"slices"

	"github.com/spf13/afero"
)

func GetRules(fs afero.Fs) ([]Rule, error) {
	rules := []Rule{
		&SimpleRule{
			Identifier:       "task-name-valid",
			Validator:        EnsureTaskNameIsValid,
			AssetValidator:   EnsureTaskNameIsValidForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "task-name-unique",
			Validator:        EnsureTaskNameIsUnique,
			AssetValidator:   EnsureTaskNameIsUniqueForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "dependency-exists",
			Validator:        EnsureDependencyExists,
			AssetValidator:   EnsureDependencyExistsForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-executable-file",
			Validator:        EnsureExecutableFileIsValid(fs),
			AssetValidator:   EnsureExecutableFileIsValidForASingleAsset(fs),
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-pipeline-schedule",
			Validator:        EnsurePipelineScheduleIsValidCron,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-pipeline-name",
			Validator:        EnsurePipelineNameIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-task-type",
			Validator:        EnsureOnlyAcceptedTaskTypesAreThere,
			AssetValidator:   EnsureTypeIsCorrectForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "acyclic-pipeline",
			Validator:        EnsurePipelineHasNoCycles,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-slack-notification",
			Validator:        EnsureSlackFieldInPipelineIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "materialization-config",
			Validator:        EnsureMaterializationValuesAreValid,
			AssetValidator:   EnsureMaterializationValuesAreValidForSingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-snowflake-query-sensor",
			Validator:        EnsureSnowflakeSensorHasQueryParameter,
			AssetValidator:   EnsureSnowflakeSensorHasQueryParameterForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
	}

	return rules, nil
}

func FilterRulesByLevel(rules []Rule, level Level) []Rule {
	filtered := make([]Rule, 0, len(rules))
	for _, rule := range rules {
		if slices.Contains(rule.GetApplicableLevels(), level) {
			filtered = append(filtered, rule)
		}
	}

	return filtered
}
