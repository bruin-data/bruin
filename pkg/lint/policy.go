package lint

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bmatcuk/doublestar"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/pkg/errors"
)

var (
	errEmptyName     = errors.New("Name is empty")
	errEmptyDesc     = errors.New("Description is empty")
	errEmptyCriteria = errors.New("Criteria is empty")
	errNoRules       = errors.New("No rules specified")
)

type validatorEnv struct {
	Asset    *pipeline.Asset    `expr:"asset"`
	Pipeline *pipeline.Pipeline `expr:"pipeline"`
}

type RuleDefinition struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
	Criteria    string `yaml:"criteria"`

	evalutor *vm.Program
}

func (def *RuleDefinition) validate() error {

	if strings.TrimSpace(def.Name) == "" {
		return errEmptyName
	}
	if strings.TrimSpace(def.Description) == "" {
		return errEmptyDesc
	}
	if strings.TrimSpace(def.Criteria) == "" {
		return errEmptyCriteria
	}
	return nil
}

func (def *RuleDefinition) compile() error {
	program, err := expr.Compile(
		def.Criteria,
		expr.AsBool(),
		expr.Env(validatorEnv{}),
	)
	if err != nil {
		return fmt.Errorf("error compiling rule: %s: %w", def.Name, err)
	}
	def.evalutor = program
	return nil
}

type RuleSet struct {
	Name     string   `yaml:"name"`
	Selector string   `yaml:"selector"`
	Rules    []string `yaml:"rules"`
}

func (rs *RuleSet) validate() error {
	if strings.TrimSpace(rs.Name) == "" {
		return errEmptyName
	}
	if len(rs.Rules) == 0 {
		return errNoRules
	}

	return nil
}

type PolicySpecification struct {
	Definitions []*RuleDefinition `yaml:"define"`
	RuleSets    []RuleSet         `yaml:"rulesets"`

	compiledRules map[string]*RuleDefinition
}

func (spec *PolicySpecification) init() error {
	spec.compiledRules = make(map[string]*RuleDefinition)
	for idx, def := range spec.Definitions {
		err := def.validate()
		if err != nil {
			return fmt.Errorf("invalid rule definition at index %d: %w", idx, err)
		}
		if err := def.compile(); err != nil {
			return err
		}
		spec.compiledRules[def.Name] = def
	}
	return nil
}

func (spec *PolicySpecification) Rules() ([]Rule, error) {
	if err := spec.init(); err != nil {
		return nil, err
	}

	rules := []Rule{}
	for idx, ruleSet := range spec.RuleSets {

		err := ruleSet.validate()
		if err != nil {
			return nil, fmt.Errorf("invalid ruleset at index %d: %w", idx, err)
		}

		for _, ruleName := range ruleSet.Rules {
			def, ok := spec.compiledRules[ruleName]
			if !ok {
				// todo(turtledev): add context to this error
				return nil, fmt.Errorf("rule %s not found in definitions", ruleName)
			}
			validator := newPolicyValidator(ruleSet, def)
			rules = append(rules, &SimpleRule{
				Identifier:       fmt.Sprintf("policy:%s:%s", ruleSet.Name, def.Name),
				Fast:             true,
				Severity:         ValidatorSeverityCritical,
				Validator:        CallFuncForEveryAsset(validator),
				AssetValidator:   validator,
				ApplicableLevels: []Level{LevelPipeline, LevelAsset},
			})
		}
	}
	return rules, nil
}

func newPolicyValidator(rs RuleSet, def *RuleDefinition) AssetValidator {
	return func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		if rs.Selector != "" {
			// hack
			root := strings.Replace(
				asset.ExecutableFile.Path,
				filepath.Dir(filepath.Dir(pipeline.DefinitionFile.Path)),
				"",
				1,
			)
			root = strings.TrimPrefix(root, "/")
			matched, err := doublestar.Match(rs.Selector, root)
			if err != nil || !matched {
				return nil, nil
			}
		}

		env := validatorEnv{asset, pipeline}
		result, err := expr.Run(def.evalutor, env)
		if err != nil {
			return nil, fmt.Errorf("error evaluating rule %s: %w", def.Name, err)
		}

		if result.(bool) {
			return nil, nil
		}

		return []*Issue{
			{
				Task:        asset,
				Description: def.Description,
			},
		}, nil
	}
}
