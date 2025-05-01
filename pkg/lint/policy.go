package lint

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
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
	errNoSuchTarget  = errors.New("No such target")
)

type assetValidatorEnv struct {
	Asset    *pipeline.Asset    `expr:"asset"`
	Pipeline *pipeline.Pipeline `expr:"pipeline"`
}

type pipelineValidatorEnv struct {
	Pipeline *pipeline.Pipeline `expr:"pipeline"`
}

type validators struct {
	Pipeline PipelineValidator
	Asset    AssetValidator
}

type RuleTarget string

var (
	RuleTargetAsset    = RuleTarget("asset")
	RuleTargetPipeline = RuleTarget("pipeline")
)

func (target RuleTarget) Valid() bool {
	return slices.Contains(
		[]RuleTarget{
			RuleTargetAsset,
			RuleTargetPipeline,
		},
		target,
	)
}

type RuleDefinition struct {
	Name        string     `yaml:"name"`
	Description string     `yaml:"description"`
	Criteria    string     `yaml:"criteria"`
	RuleTarget  RuleTarget `yaml:"target"`

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
	if def.RuleTarget == "" {
		def.RuleTarget = RuleTargetAsset
	}
	if !def.RuleTarget.Valid() {
		return errNoSuchTarget
	}

	return nil
}

func (def *RuleDefinition) compile() error {
	var env any = assetValidatorEnv{}
	if def.RuleTarget == RuleTargetPipeline {
		env = pipelineValidatorEnv{}
	}
	program, err := expr.Compile(
		def.Criteria,
		expr.AsBool(),
		expr.Env(env),
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
	Definitions []*RuleDefinition `yaml:"custom_rules"`
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
			validators, found := spec.getValidators(ruleName)
			if !found {
				return nil, fmt.Errorf("no such rule: %s", ruleName)
			}
			validators = withSelector(ruleSet.Selector, validators)
			rules = append(rules, &SimpleRule{
				Identifier:     fmt.Sprintf("policy:%s:%s", ruleSet.Name, ruleName),
				Fast:           true,
				Severity:       ValidatorSeverityCritical,
				Validator:      validators.Pipeline,
				AssetValidator: validators.Asset,
			})
		}
	}
	return rules, nil
}

func (spec *PolicySpecification) getValidators(name string) (validators, bool) {
	def, found := spec.compiledRules[name]
	if !found {
		return validators{}, false
	}
	v := validators{}

	switch def.RuleTarget {
	case RuleTargetAsset:
		assetValidator := assetValidatorFromRuleDef(def)
		v.Asset = assetValidator
		v.Pipeline = CallFuncForEveryAsset(assetValidator)
	case RuleTargetPipeline:
		v.Pipeline = pipelineValidatorFromRuleDef(def)
	}

	return v, true
}

func assetValidatorFromRuleDef(def *RuleDefinition) AssetValidator {
	return func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
		env := assetValidatorEnv{asset, pipeline}
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

func pipelineValidatorFromRuleDef(def *RuleDefinition) PipelineValidator {
	return func(pipe *pipeline.Pipeline) ([]*Issue, error) {
		env := pipelineValidatorEnv{pipe}
		result, err := expr.Run(def.evalutor, env)
		if err != nil {
			return nil, fmt.Errorf("error evaluating rule %s: %w", def.Name, err)
		}

		if result.(bool) {
			return nil, nil
		}

		return []*Issue{
			{
				Description: def.Description,
			},
		}, nil
	}
}

func withSelector(selector []map[string]any, downstream validators) validators {
	return validators{
		Pipeline: func(pipeline *pipeline.Pipeline) ([]*Issue, error) {
			match, err := doesSelectorMatch(selector, pipeline, nil)
			if err != nil {
				return nil, fmt.Errorf("error matching selector: %w", err)
			}

			if !match {
				return nil, nil
			}

			return downstream.Pipeline(pipeline)
		},
		Asset: func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
			if downstream.Asset == nil {
				return nil, nil
			}
			match, err := doesSelectorMatch(selector, pipeline, asset)
			if err != nil {
				return nil, fmt.Errorf("error matching selector: %w", err)
			}

			if !match {
				return nil, nil
			}
			return downstream.Asset(ctx, pipeline, asset)
		},
	}

}

func doesSelectorMatch(selectors []map[string]any, pipeline *pipeline.Pipeline, asset *pipeline.Asset) (bool, error) {
	for _, sel := range selectors {
		for key, val := range sel {
			if asset == nil && slices.Contains([]string{"asset", "tag"}, key) {
				continue
			}

			pattern, ok := val.(string)
			if !ok {
				return false, fmt.Errorf("invalid selector value for key %s: %v", key, val)
			}
			var subject string
			switch key {
			case "pipeline":
				subject = pipeline.Name
			case "asset":
				subject = asset.Name
			case "path":
				if asset == nil {
					subject = pipeline.DefinitionFile.Path
				} else {
					subject = asset.ExecutableFile.Path
				}
			case "tag":
				subject = strings.Join(asset.Tags, " ")
			default:
				return false, fmt.Errorf("unknown selector key: %s", key)
			}

			pattern = addBoundaryAnchors(pattern)
			match, err := regexp.MatchString(pattern, subject)
			if err != nil {
				return false, fmt.Errorf("error matching %s: %w", key, err)
			}
			if !match {
				return false, nil
			}
		}
	}
	return true, nil
}

func addBoundaryAnchors(pattern string) string {
	if !strings.HasPrefix(pattern, "^") {
		pattern = "^" + pattern
	}
	if !strings.HasSuffix(pattern, "$") {
		pattern += "$"
	}
	return pattern
}

func loadPolicy(fs afero.Fs) (rules []Rule, err error) {
	wd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("error obtaining working directory: %w", err)
	}

	repo, err := git.FindRepoInSubtree(wd)
	if errors.Is(err, git.ErrNoGitRepoFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("loadPolicy: %w", err)
	}

	policyFile := locatePolicy(fs, repo.Path)
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
	return rules, nil
}

func locatePolicy(fs afero.Fs, repo string) string {
	names := []string{
		"policy.yaml",
		"policy.yml",
	}
	for _, name := range names {
		fileName := filepath.Join(repo, name)
		fi, err := fs.Stat(fileName)
		if err != nil {
			continue
		}
		if fi.Mode().IsRegular() {
			return fileName
		}
	}
	return ""
}
