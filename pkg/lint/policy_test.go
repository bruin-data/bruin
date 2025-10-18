package lint_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
)

type mockSQLParser struct {
	mock.Mock
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
