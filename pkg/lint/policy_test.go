package lint_test

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockSQLParser struct {
	mock.Mock
}

func (m *mockSQLParser) Start() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockSQLParser) UsedTables(sql, dialect string) ([]string, error) {
	args := m.Called(sql, dialect)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockSQLParser) GetMissingDependenciesForAsset(asset *pipeline.Asset, pipeline *pipeline.Pipeline, renderer jinja.RendererInterface) ([]string, error) {
	args := m.Called(asset, pipeline, renderer)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockSQLParser) ColumnLineage(sql, dialect string, schema sqlparser.Schema) (*sqlparser.Lineage, error) {
	args := m.Called(sql, dialect, schema)
	return args.Get(0).(*sqlparser.Lineage), args.Error(1)
}

func (m *mockSQLParser) RenameTables(sql, dialect string, tableMapping map[string]string) (string, error) {
	args := m.Called(sql, dialect, tableMapping)
	return args.String(0), args.Error(1)
}

func (m *mockSQLParser) AddLimit(sql string, limit int, dialect string) (string, error) {
	args := m.Called(sql, limit, dialect)
	return args.String(0), args.Error(1)
}

func (m *mockSQLParser) IsSingleSelectQuery(sql string, dialect string) (bool, error) {
	args := m.Called(sql, dialect)
	return args.Bool(0), args.Error(1)
}

func (m *mockSQLParser) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestPolicyRuleDefinition(t *testing.T) {
	t.Parallel()
	t.Run("rule definition must have a name", func(t *testing.T) {
		t.Parallel()
		spec := &lint.PolicySpecification{
			Definitions: []*lint.RuleDefinition{
				{
					Description: "A rule without a name",
					Criteria:    "true == true",
				},
			},
		}

		mockParser := new(mockSQLParser)
		_, err := spec.Rules(mockParser)
		assert.Error(t, err)
	})
	t.Run("rule definition must have a description", func(t *testing.T) {
		t.Parallel()
		spec := &lint.PolicySpecification{
			Definitions: []*lint.RuleDefinition{
				{
					Name:     "test-rule",
					Criteria: "true == true",
				},
			},
		}
		mockParser := new(mockSQLParser)
		_, err := spec.Rules(mockParser)
		assert.Error(t, err)
	})

	t.Run("rule definition must have criteria", func(t *testing.T) {
		t.Parallel()
		spec := &lint.PolicySpecification{
			Definitions: []*lint.RuleDefinition{
				{
					Name:        "test-rule",
					Description: "A rule without criteria",
				},
			},
		}
		mockParser := new(mockSQLParser)
		_, err := spec.Rules(mockParser)
		assert.Error(t, err)
	})
	t.Run("every rule must have a unique name", func(t *testing.T) {
		t.Parallel()
		spec := &lint.PolicySpecification{
			Definitions: []*lint.RuleDefinition{
				{
					Name:        "unique-rule",
					Description: "I am unique",
					Criteria:    "true",
				},
				{
					Name:        "unique-rule",
					Description: "I am unique",
					Criteria:    "true",
				},
			},
		}
		mockParser := new(mockSQLParser)
		_, err := spec.Rules(mockParser)
		assert.Error(t, err)
	})

	t.Run("custom rules cannot shadow builtin rules", func(t *testing.T) {
		t.Parallel()
		spec := &lint.PolicySpecification{
			Definitions: []*lint.RuleDefinition{
				{
					Name:        "unique-rule",
					Description: "I am unique",
					Criteria:    "true",
				},
				{
					Name:        "asset-has-description",
					Description: "I am unique",
					Criteria:    "true",
				},
			},
		}
		mockParser := new(mockSQLParser)
		_, err := spec.Rules(mockParser)
		assert.Error(t, err)
	})
	t.Run("rule name must only be alpha-numeric and dash", func(t *testing.T) {
		t.Parallel()
		invalidNames := []string{
			"rule one",
			"rule.two",
			"rule?three",
			"rule/four",
		}
		for _, invalid := range invalidNames {
			spec := &lint.PolicySpecification{
				RuleSets: []lint.RuleSet{
					{
						Name:  "ruleset-" + invalid,
						Rules: []string{invalid},
					},
				},
				Definitions: []*lint.RuleDefinition{
					{
						Name:        invalid,
						Description: "unit test",
						Criteria:    "true",
					},
				},
			}
			mockParser := new(mockSQLParser)
			_, err := spec.Rules(mockParser)
			assert.Error(t, err)
		}
	})

	t.Run("ruleset name must only be alpha-numeric and dash", func(t *testing.T) {
		t.Parallel()
		invalidNames := []string{
			"rule set one",
			"rule.set.two",
			"rule?set?three",
			"rule/set/four",
		}
		for _, invalid := range invalidNames {
			spec := &lint.PolicySpecification{
				RuleSets: []lint.RuleSet{
					{
						Name:  "ruleset-" + invalid,
						Rules: []string{"placeholder"},
					},
				},
				Definitions: []*lint.RuleDefinition{
					{
						Name:        "placeholder",
						Description: "unit test",
						Criteria:    "true",
					},
				},
			}
			mockParser := new(mockSQLParser)
			_, err := spec.Rules(mockParser)
			assert.Error(t, err)
		}
	})
}

func TestPolicyRuleSet(t *testing.T) {
	t.Parallel()
	t.Run("ruleset must have a name", func(t *testing.T) {
		t.Parallel()
		spec := &lint.PolicySpecification{
			RuleSets: []lint.RuleSet{
				{},
			},
		}
		mockParser := new(mockSQLParser)
		_, err := spec.Rules(mockParser)
		assert.Error(t, err)
	})
	t.Run("ruleset must specify rules", func(t *testing.T) {
		t.Parallel()
		spec := &lint.PolicySpecification{
			RuleSets: []lint.RuleSet{
				{
					Name: "test-ruleset",
				},
			},
		}
		mockParser := new(mockSQLParser)
		_, err := spec.Rules(mockParser)
		assert.Error(t, err)
	})
}

func TestDescriptionMustNotBePlaceholderPolicy(t *testing.T) {
	t.Parallel()

	newRule := func(t *testing.T) lint.Rule {
		t.Helper()

		spec := &lint.PolicySpecification{
			RuleSets: []lint.RuleSet{
				{
					Name:  "placeholder-description",
					Rules: []string{"description-must-not-be-placeholder"},
				},
			},
		}

		rules, err := spec.Rules(nil)
		require.NoError(t, err)
		require.Len(t, rules, 1)
		return rules[0]
	}

	t.Run("does not match placeholders inside larger words", func(t *testing.T) {
		t.Parallel()

		asset := &pipeline.Asset{
			Description: "Tracks orders with wiped status records.",
			Columns: []pipeline.Column{
				{
					Name:        "attempt_count",
					Description: "Number of attempts to process the order.",
				},
			},
		}

		issues, err := newRule(t).ValidateAsset(t.Context(), &pipeline.Pipeline{}, asset)
		require.NoError(t, err)
		assert.Empty(t, issues)
	})

	t.Run("matches single-word placeholder tokens", func(t *testing.T) {
		t.Parallel()

		asset := &pipeline.Asset{
			Description: "TODO: add the real asset description.",
		}

		issues, err := newRule(t).ValidateAsset(t.Context(), &pipeline.Pipeline{}, asset)
		require.NoError(t, err)
		require.Len(t, issues, 1)
		assert.Contains(t, issues[0].Description, "'todo'")
	})

	t.Run("matches multi-word placeholder tokens", func(t *testing.T) {
		t.Parallel()

		asset := &pipeline.Asset{
			Description: "Order data synchronized from Shopify.",
			Columns: []pipeline.Column{
				{
					Name:        "status",
					Description: "Description goes here.",
				},
			},
		}

		issues, err := newRule(t).ValidateAsset(t.Context(), &pipeline.Pipeline{}, asset)
		require.NoError(t, err)
		require.Len(t, issues, 1)
		assert.Contains(t, issues[0].Description, "'description goes here'")
	})
}
