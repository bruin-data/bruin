package lint_test

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/stretchr/testify/assert"
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
		_, err := spec.Rules()
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
		_, err := spec.Rules()
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
		_, err := spec.Rules()
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
		_, err := spec.Rules()
		assert.Error(t, err)
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
		_, err := spec.Rules()
		assert.Error(t, err)
	})
}
