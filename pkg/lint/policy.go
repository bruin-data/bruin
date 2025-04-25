package lint

import (
	"context"
	"fmt"
	"strings"

	"github.com/bmatcuk/doublestar"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

var (
	errEmptyName     = errors.New("Name is empty")
	errEmptyDesc     = errors.New("Description is empty")
	errEmptyCriteria = errors.New("Criteria is empty")
	errNoRules       = errors.New("No rules specified")
)

var builtinRules = map[string]AssetValidator{
	"asset_name_is_lowercase": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		if strings.ToLower(asset.Name) == asset.Name {
			return nil, nil
		}

		return []*Issue{
			{
				Task:        asset,
				Description: "Asset name must be lowercase",
			},
		}, nil
	},
	"asset_name_is_schema_dot_table": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		if strings.Count(asset.Name, ".") == 1 {
			return nil, nil
		}

		return []*Issue{
			{
				Task:        asset,
				Description: "Asset name must be of the form {schema}.{table}",
			},
		}, nil
	},
	"asset_has_description": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		if strings.TrimSpace(asset.Description) != "" {
			return nil, nil
		}
		return []*Issue{
			{
				Task:        asset,
				Description: "Asset must have a description",
			},
		}, nil
	},
	"asset_has_owner": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		if strings.TrimSpace(asset.Owner) != "" {
			return nil, nil
		}
		return []*Issue{
			{
				Task:        asset,
				Description: "Asset must have an owner",
			},
		}, nil
	},
	"asset_has_columns": func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		if len(asset.Columns) > 0 {
			return nil, nil
		}
		return []*Issue{
			{
				Task:        asset,
				Description: "Asset must have columns",
			},
		}, nil
	},
}

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
	Name     string           `yaml:"name"`
	Selector []map[string]any `yaml:"selector"`
	Rules    []string         `yaml:"rules"`
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
			validator, found := spec.getValidator(ruleName)
			if !found {
				return nil, fmt.Errorf("no such rule: %s", ruleName)
			}
			validator = withSelector(ruleSet.Selector, validator)
			rules = append(rules, &SimpleRule{
				Identifier:       fmt.Sprintf("policy:%s:%s", ruleSet.Name, ruleName),
				Fast:             true,
				Severity:         ValidatorSeverityCritical,
				Validator:        CallFuncForEveryAsset(validator),
				AssetValidator:   validator,
				ApplicableLevels: []Level{LevelAsset},
			})
		}
	}
	return rules, nil
}

func (spec *PolicySpecification) getValidator(name string) (AssetValidator, bool) {
	def, found := spec.compiledRules[name]
	if found {
		return newPolicyValidator(def), true
	}
	validator, found := builtinRules[name]
	return validator, found
}

func newPolicyValidator(def *RuleDefinition) AssetValidator {
	return func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
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

func withSelector(selector []map[string]any, validator AssetValidator) AssetValidator {
	return func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		match, err := doesSelectorMatch(selector, asset)
		if err != nil {
			return nil, fmt.Errorf("error matching selector: %w", err)
		}

		if !match {
			return nil, nil
		}

		return validator(ctx, pipeline, asset)
	}
}

func doesSelectorMatch(selectors []map[string]any, asset *pipeline.Asset) (bool, error) {
	for _, sel := range selectors {
		for key, val := range sel {
			switch key {
			case "path":
				pattern, ok := val.(string)
				if !ok {
					return false, fmt.Errorf("invalid selector value for key %s: %v", key, val)
				}
				match, err := doublestar.Match(pattern, asset.ExecutableFile.Path)
				if err != nil {
					return false, fmt.Errorf("error matching path pattern %s: %w", pattern, err)
				}
				if !match {
					return false, nil
				}
			}
		}
	}
	return true, nil
}

func loadPolicy(fs afero.Fs) (rules []Rule, err error) {
	policyFile := locatePolicy(fs)
	if policyFile == "" {
		return nil, nil
	}
	fd, err := fs.Open(policyFile)
	if err != nil {
		return nil, fmt.Errorf("error opening policy file: %w", err)
	}
	defer fd.Close()

	spec := PolicySpecification{}
	err = yaml.NewDecoder(fd).Decode(&spec)
	if err != nil {
		return nil, fmt.Errorf("error reading policy file: %w", err)
	}

	policyRules, err := spec.Rules()
	if err != nil {
		return nil, fmt.Errorf("error reading policy: %w", err)
	}

	rules = append(rules, policyRules...)
	return
}

func locatePolicy(fs afero.Fs) string {
	names := []string{
		"policies.yaml",
		"policies.yml",
	}
	for _, name := range names {
		fi, err := fs.Stat(name)
		if err != nil {
			continue
		}
		if fi.Mode().IsRegular() {
			return name
		}
	}
	return ""
}
