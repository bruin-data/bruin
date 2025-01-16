package lint

import (
	"slices"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/samber/lo"
	"github.com/spf13/afero"
)

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

func GetRules(fs afero.Fs, finder repoFinder, excludeWarnings bool, parser *sqlparser.SQLParser) ([]Rule, error) {
	gr := GlossaryChecker{
		gr: &glossary.GlossaryReader{
			RepoFinder: finder,
			FileNames:  []string{"glossary.yml", "glossary.yaml"},
		},
	}

	rules := []Rule{
		&SimpleRule{
			Identifier:       "task-name-valid",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureTaskNameIsValidForASingleAsset),
			AssetValidator:   EnsureTaskNameIsValidForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "task-name-unique",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsureTaskNameIsUnique,
			AssetValidator:   EnsureTaskNameIsUniqueForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "dependency-exists",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureDependencyExistsForASingleAsset),
			AssetValidator:   EnsureDependencyExistsForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-executable-file",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureExecutableFileIsValidForASingleAsset(fs)),
			AssetValidator:   EnsureExecutableFileIsValidForASingleAsset(fs),
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-pipeline-schedule",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsurePipelineScheduleIsValidCron,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-pipeline-name",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsurePipelineNameIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-task-type",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureTypeIsCorrectForASingleAsset),
			AssetValidator:   EnsureTypeIsCorrectForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "acyclic-pipeline",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsurePipelineHasNoCycles,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-slack-notification",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsureSlackFieldInPipelineIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-ms-teams-notification",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsureMSTeamsFieldInPipelineIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "materialization-config",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureMaterializationValuesAreValidForSingleAsset),
			AssetValidator:   EnsureMaterializationValuesAreValidForSingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-snowflake-query-sensor",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureSnowflakeSensorHasQueryParameterForASingleAsset),
			AssetValidator:   EnsureSnowflakeSensorHasQueryParameterForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-bigquery-table-sensor",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureBigQueryTableSensorHasTableParameterForASingleAsset),
			AssetValidator:   EnsureBigQueryTableSensorHasTableParameterForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-ingestr",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(EnsureIngestrAssetIsValidForASingleAsset),
			AssetValidator:   EnsureIngestrAssetIsValidForASingleAsset,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-pipeline-start-date",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        EnsurePipelineStartDateIsValid,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-entity-references",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(gr.EnsureAssetEntitiesExistInGlossary),
			AssetValidator:   gr.EnsureAssetEntitiesExistInGlossary,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "duplicate-column-names",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(ValidateDuplicateColumnNames),
			AssetValidator:   ValidateDuplicateColumnNames,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "custom-check-query-exists",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(ValidateCustomCheckQueryExists),
			AssetValidator:   ValidateCustomCheckQueryExists,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
		&SimpleRule{
			Identifier:       "assets-directory-exist",
			Fast:             true,
			Severity:         ValidatorSeverityWarning,
			Validator:        ValidateAssetDirectoryExist,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "assets-seed-validation",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        CallFuncForEveryAsset(ValidateAssetSeedValidation),
			AssetValidator:   ValidateAssetSeedValidation,
			ApplicableLevels: []Level{LevelPipeline, LevelAsset},
		},
	}

	if parser != nil {
		rules = append(rules, UsedTableValidatorRule{
			renderer: jinja.NewRendererWithYesterday("your-pipeline", "some-run-id"),
			parser:   parser,
		})
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

func FilterRulesBySpeed(rules []Rule, fast bool) []Rule {
	filtered := make([]Rule, 0, len(rules))
	for _, rule := range rules {
		if rule.IsFast() == fast {
			filtered = append(filtered, rule)
		}
	}
	return filtered
}
