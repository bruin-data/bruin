package lint_test

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

		_, err := spec.Rules()
		require.Error(t, err)
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
		_, err := spec.Rules()
		require.Error(t, err)
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
		_, err := spec.Rules()
		require.Error(t, err)
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
		_, err := spec.Rules()
		require.Error(t, err)
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
		_, err := spec.Rules()
		require.Error(t, err)
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
			_, err := spec.Rules()
			require.Error(t, err)
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
			_, err := spec.Rules()
			require.Error(t, err)
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
		_, err := spec.Rules()
		require.Error(t, err)
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
		_, err := spec.Rules()
		require.Error(t, err)
	})
}

func TestBuiltinColumnsMatchQueryPolicy(t *testing.T) {
	t.Parallel()

	t.Run("columns-match-query rule exists in builtin rules", func(t *testing.T) {
		t.Parallel()
		spec := &lint.PolicySpecification{
			RuleSets: []lint.RuleSet{
				{
					Name:  "test-ruleset",
					Rules: []string{"columns-match-query"},
				},
			},
		}
		rules, err := spec.Rules()
		require.NoError(t, err)
		assert.Len(t, rules, 1)
		assert.Equal(t, "policy:test-ruleset:columns-match-query", rules[0].Name())
	})

	t.Run("columns-match-query rule cannot be shadowed by custom rule", func(t *testing.T) {
		t.Parallel()
		spec := &lint.PolicySpecification{
			Definitions: []*lint.RuleDefinition{
				{
					Name:        "columns-match-query",
					Description: "Custom rule trying to shadow builtin",
					Criteria:    "true",
				},
			},
		}
		_, err := spec.Rules()
		require.Error(t, err)
		assert.Contains(t, err.Error(), "rule is builtin: columns-match-query")
	})

	t.Run("columns-match-query rule is available in builtin rules map", func(t *testing.T) {
		t.Parallel()
		// This test verifies that the rule exists in the builtinRules map
		// We can't directly access the map from tests, but we can verify it through the policy system
		spec := &lint.PolicySpecification{
			RuleSets: []lint.RuleSet{
				{
					Name:  "test-ruleset",
					Rules: []string{"columns-match-query"},
				},
			},
		}
		rules, err := spec.Rules()
		require.NoError(t, err)
		assert.Len(t, rules, 1)

		// Verify the rule has the correct applicable levels
		levels := rules[0].GetApplicableLevels()
		assert.Contains(t, levels, lint.LevelAsset)
		assert.NotContains(t, levels, lint.LevelPipeline)
	})
}
