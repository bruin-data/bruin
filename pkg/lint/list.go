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

func GetRules(fs afero.Fs, finder repoFinder, excludeWarnings bool, parser *sqlparser.SQLParser, cacheFoundGlossary bool, connectionManager connectionManager) ([]Rule, error) {
	gr := GlossaryChecker{
		gr: &glossary.GlossaryReader{
			RepoFinder: finder,
			FileNames:  []string{"glossary.yml", "glossary.yaml"},
		},
		cacheFoundGlossary: cacheFoundGlossary,
	}

	yamlFileValidator := WarnRegularYamlFiles{fs: fs}

	rules := []Rule{
		&SimpleRule{
			Identifier:       "task-name-valid",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   EnsureTaskNameIsValidForASingleAsset,
			ApplicableLevels: []Level{LevelAsset},
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
			AssetValidator:   EnsureDependencyExistsForASingleAsset,
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-executable-file",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   EnsureExecutableFileIsValidForASingleAsset(fs),
			ApplicableLevels: []Level{LevelAsset},
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
			AssetValidator:   EnsureTypeIsCorrectForASingleAsset,
			ApplicableLevels: []Level{LevelAsset},
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
			AssetValidator:   EnsureMaterializationValuesAreValidForSingleAsset,
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-snowflake-query-sensor",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   EnsureSnowflakeSensorHasQueryParameterForASingleAsset,
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-bigquery-table-sensor",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   EnsureBigQueryTableSensorHasTableParameterForASingleAsset,
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "valid-ingestr",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   EnsureIngestrAssetIsValidForASingleAsset,
			ApplicableLevels: []Level{LevelAsset},
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
			AssetValidator:   gr.EnsureAssetEntitiesExistInGlossary,
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "duplicate-column-names",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   ValidateDuplicateColumnNames,
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "custom-check-query-exists",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   ValidateCustomCheckQueryExists,
			ApplicableLevels: []Level{LevelAsset},
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
			AssetValidator:   ValidateAssetSeedValidation,
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "assets-python-validation",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   ValidatePythonAssetMaterialization,
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "emr-serverless-spark-validation",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   ValidateEMRServerlessAsset,
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "plain-yaml-files",
			Fast:             false,
			Severity:         ValidatorSeverityWarning,
			Validator:        yamlFileValidator.WarnRegularYamlFilesInRepo,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "valid-variables",
			Fast:             true,
			Severity:         ValidatorSeverityCritical,
			Validator:        ValidateVariables,
			ApplicableLevels: []Level{LevelPipeline},
		},
		&SimpleRule{
			Identifier:       "custom-check-query-dry-run",
			Fast:             false,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   ValidateCustomCheckQueryDryRun(connectionManager),
			ApplicableLevels: []Level{LevelAsset},
		},
		&SimpleRule{
			Identifier:       "materialization-type-match",
			Fast:             false,
			Severity:         ValidatorSeverityCritical,
			AssetValidator:   ValidateMaterializationTypeMatches(connectionManager),
			ApplicableLevels: []Level{LevelAsset},
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
