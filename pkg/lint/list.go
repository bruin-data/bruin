package lint

import (
	"slices"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/spf13/afero"
)

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

func GetRules(fs afero.Fs, finder repoFinder, excludeWarnings bool) ([]Rule, error) {
	gr := GlossaryChecker{
		gr: &glossary.GlossaryReader{
			RepoFinder: finder,
			FileNames:  []string{"glossary.yml", "glossary.yaml"},
		},
	}

	parser, err := sqlparser.NewSQLParser()
	if err != nil {
		return make([]Rule, 0), errors.Wrap(err, "failed to instantiate sql parser")
	}

	rules := []Rule{
		&SimpleRule{
			Identifier:       "task-name-valid",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureTaskNameIsValidForASingleAsset),
			AssetValidator:   EnsureTaskNameIsValidForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "task-name-unique",
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsureTaskNameIsUnique,
			AssetValidator:   EnsureTaskNameIsUniqueForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "dependency-exists",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureDependencyExistsForASingleAsset),
			AssetValidator:   EnsureDependencyExistsForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-executable-file",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureExecutableFileIsValidForASingleAsset(fs)),
			AssetValidator:   EnsureExecutableFileIsValidForASingleAsset(fs),
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-pipeline-schedule",
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsurePipelineScheduleIsValidCron,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-pipeline-name",
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsurePipelineNameIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-task-type",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureTypeIsCorrectForASingleAsset),
			AssetValidator:   EnsureTypeIsCorrectForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "acyclic-pipeline",
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsurePipelineHasNoCycles,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-slack-notification",
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsureSlackFieldInPipelineIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-ms-teams-notification",
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsureMSTeamsFieldInPipelineIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "materialization-config",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureMaterializationValuesAreValidForSingleAsset),
			AssetValidator:   EnsureMaterializationValuesAreValidForSingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-snowflake-query-sensor",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureSnowflakeSensorHasQueryParameterForASingleAsset),
			AssetValidator:   EnsureSnowflakeSensorHasQueryParameterForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-bigquery-table-sensor",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureBigQueryTableSensorHasTableParameterForASingleAsset),
			AssetValidator:   EnsureBigQueryTableSensorHasTableParameterForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-ingestr",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureIngestrAssetIsValidForASingleAsset),
			AssetValidator:   EnsureIngestrAssetIsValidForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-pipeline-start-date",
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsurePipelineStartDateIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-entity-references",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(gr.EnsureAssetEntitiesExistInGlossary),
			AssetValidator:   gr.EnsureAssetEntitiesExistInGlossary,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "duplicate-column-names",
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(ValidateDuplicateColumnNames),
			AssetValidator:   ValidateDuplicateColumnNames,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		UsedTableValidatorRule{
			renderer: jinja.NewRendererWithYesterday("your-pipeline", "some-run-id"),
			parser:   parser,
		},
	}

	if excludeWarnings {
		return lo.Filter(rules, func(rule Rule, index int) bool {
			return rule.GetSeverity() != ValidatorSeverityWarning
		}), nil
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
