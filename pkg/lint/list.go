package lint

import (
	"slices"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/spf13/afero"
)

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

func GetRules(fs afero.Fs, finder repoFinder) ([]Rule, error) {
	gr := GlossaryChecker{
		gr: &glossary.GlossaryReader{
			RepoFinder: finder,
			FileNames:  []string{"glossary.yml", "glossary.yaml"},
		},
	}

	rules := []Rule{
		&SimpleRule{
			Identifier:       "task-name-valid",
			Validator:        CallFuncForEveryAsset(EnsureTaskNameIsValidForASingleAsset),
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
			Validator:        CallFuncForEveryAsset(EnsureDependencyExistsForASingleAsset),
			AssetValidator:   EnsureDependencyExistsForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-executable-file",
			Validator:        CallFuncForEveryAsset(EnsureExecutableFileIsValidForASingleAsset(fs)),
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
			Validator:        CallFuncForEveryAsset(EnsureTypeIsCorrectForASingleAsset),
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
			Validator:        CallFuncForEveryAsset(EnsureMaterializationValuesAreValidForSingleAsset),
			AssetValidator:   EnsureMaterializationValuesAreValidForSingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-snowflake-query-sensor",
			Validator:        CallFuncForEveryAsset(EnsureSnowflakeSensorHasQueryParameterForASingleAsset),
			AssetValidator:   EnsureSnowflakeSensorHasQueryParameterForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-bigquery-table-sensor",
			Validator:        CallFuncForEveryAsset(EnsureBigQueryTableSensorHasTableParameterForASingleAsset),
			AssetValidator:   EnsureBigQueryTableSensorHasTableParameterForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-ingestr",
			Validator:        CallFuncForEveryAsset(EnsureIngestrAssetIsValidForASingleAsset),
			AssetValidator:   EnsureIngestrAssetIsValidForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-pipeline-start-date",
			Validator:        EnsurePipelineStartDateIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-entity-references",
			Validator:        CallFuncForEveryAsset(gr.EnsureAssetEntitiesExistInGlossary),
			AssetValidator:   gr.EnsureAssetEntitiesExistInGlossary,
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
