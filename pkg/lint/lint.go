package lint

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

type (
	pipelineFinder    func(root string, pipelineDefinitionFile []string) ([]string, error)
	PipelineValidator func(pipeline *pipeline.Pipeline) ([]*Issue, error)
	AssetValidator    func(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error)
)

type pipelineBuilder interface {
	CreatePipelineFromPath(ctx context.Context, pathToPipeline string, opts ...pipeline.CreatePipelineOption) (*pipeline.Pipeline, error)
}

type Issue struct {
	Task        *pipeline.Asset
	Description string
	Context     []string
}

type Rule interface {
	Name() string
	IsFast() bool
	Validate(pipeline *pipeline.Pipeline) ([]*Issue, error)
	ValidateAsset(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error)
	GetApplicableLevels() []Level
	GetSeverity() ValidatorSeverity
}

type SimpleRule struct {
	Identifier       string
	Fast             bool
	Validator        PipelineValidator
	AssetValidator   AssetValidator
	ApplicableLevels []Level
	Severity         ValidatorSeverity
}

func (g *SimpleRule) Validate(pipeline *pipeline.Pipeline) ([]*Issue, error) {
	return g.Validator(pipeline)
}

func (g *SimpleRule) IsFast() bool {
	return g.Fast
}

func (g *SimpleRule) ValidateAsset(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	if g.AssetValidator == nil {
		return []*Issue{}, errors.New(fmt.Sprintf("the rule '%s' cannot be used to validate assets, please open an issue", g.Identifier))
	}

	return g.AssetValidator(ctx, pipeline, asset)
}

func (g *SimpleRule) Name() string {
	return g.Identifier
}

func (g *SimpleRule) GetApplicableLevels() []Level {
	return g.ApplicableLevels
}

func (g *SimpleRule) GetSeverity() ValidatorSeverity {
	return g.Severity
}

type Linter struct {
	findPipelines pipelineFinder
	builder       pipelineBuilder
	rules         []Rule
	logger        logger.Logger
}

func NewLinter(findPipelines pipelineFinder, builder pipelineBuilder, rules []Rule, logger logger.Logger) *Linter {
	return &Linter{
		findPipelines: findPipelines,
		builder:       builder,
		rules:         rules,
		logger:        logger,
	}
}

func (l *Linter) Lint(rootPath string, pipelineDefinitionFileName []string, c *cli.Context) (*PipelineAnalysisResult, error) {
	pipelines, err := l.extractPipelinesFromPath(rootPath, pipelineDefinitionFileName)

	excludeTag := ""
	if c != nil {
		excludeTag = c.String("exclude-tag")
	}
	assetStats := make(map[string]int)

	for _, p := range pipelines {
		filtered := make([]*pipeline.Asset, 0, len(p.Assets))
		for _, a := range p.Assets {
			skip := false
			if excludeTag != "" {
				for _, tag := range a.Tags {
					if tag == excludeTag {
						skip = true
						break
					}
				}
			}
			if !skip {
				filtered = append(filtered, a)
				_, ok := assetStats[string(a.Type)]
				if !ok {
					assetStats[string(a.Type)] = 0
				}
				assetStats[string(a.Type)]++
			}
		}
		p.Assets = filtered
	}

	telemetry.SendEventWithAssetStats("validate_assets", assetStats, c)

	if err != nil {
		return nil, err
	}

	return l.LintPipelines(pipelines)
}

func (l *Linter) LintAsset(rootPath string, pipelineDefinitionFileName []string, assetNameOrPath string, c *cli.Context) (*PipelineAnalysisResult, error) {
	pipelines, err := l.extractPipelinesFromPath(rootPath, pipelineDefinitionFileName)
	if err != nil {
		return nil, err
	}

	var assetPipeline *pipeline.Pipeline
	var asset *pipeline.Asset
	for _, fp := range pipelines {
		asset = fp.GetAssetByPath(assetNameOrPath)
		if asset != nil {
			assetPipeline = fp
			l.logger.Debugf("found an asset with path under the pipeline '%s'", fp.DefinitionFile.Path)
			break
		}
	}

	if asset == nil {
		l.logger.Debugf("couldn't find an asset with the path '%s', trying it as a name instead", assetNameOrPath)

		matchedAssetCount := 0
		for _, fp := range pipelines {
			maybeAsset := fp.GetAssetByName(assetNameOrPath)
			if maybeAsset != nil {
				matchedAssetCount++
				asset = maybeAsset
				assetPipeline = fp
			}
		}

		if matchedAssetCount > 1 {
			return nil, errors.Errorf("there are %d assets with the name '%s', you'll have to use an asset path or go to the pipeline directory", matchedAssetCount, assetNameOrPath)
		}
	}

	if asset == nil {
		return nil, errors.Errorf("failed to find an asset with the path or name '%s' under the path '%s'", assetNameOrPath, rootPath)
	}

	telemetry.SendEventWithAssetStats("validate_assets", map[string]int{string(asset.Type): 1}, c)

	pipelineResult := &PipelineIssues{
		Pipeline: assetPipeline,
		Issues:   make(map[Rule][]*Issue),
	}

	rules := l.rules
	policyRules, err := loadPolicy(assetPipeline.DefinitionFile.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to load policy: %w", err)
	}
	if len(policyRules) > 0 {
		assetRules := []Rule{}

		// suboptimal, but works.
		for _, rule := range policyRules {
			if slices.Contains(rule.GetApplicableLevels(), LevelAsset) {
				assetRules = append(assetRules, rule)
			}
		}
		rules = slices.Concat([]Rule{}, rules, assetRules)
	}

	// now the actual validation starts
	for _, rule := range rules {
		issues, err := rule.ValidateAsset(context.TODO(), assetPipeline, asset)
		if err != nil {
			return nil, err
		}

		if len(issues) > 0 {
			pipelineResult.Issues[rule] = issues
		}
	}

	return &PipelineAnalysisResult{
		Pipelines: []*PipelineIssues{
			pipelineResult,
		},
	}, nil
}

func (l *Linter) extractPipelinesFromPath(rootPath string, pipelineDefinitionFileName []string) ([]*pipeline.Pipeline, error) {
	pipelinePaths, err := l.findPipelines(rootPath, pipelineDefinitionFileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("the given pipeline path does not exist, please make sure you gave the right path")
		}

		return nil, errors.Wrap(err, "error getting pipeline paths")
	}

	if len(pipelinePaths) == 0 {
		return nil, fmt.Errorf("no pipelines found in path '%s'", rootPath)
	}

	l.logger.Debugf("found %d pipelines", len(pipelinePaths))
	sort.Strings(pipelinePaths)

	err = EnsureNoNestedPipelines(pipelinePaths)
	if err != nil {
		return nil, err
	}

	l.logger.Debug("no nested pipelines found, moving forward")
	pipelines := make([]*pipeline.Pipeline, 0, len(pipelinePaths))
	for _, pipelinePath := range pipelinePaths {
		l.logger.Debugf("creating pipeline from path '%s'", pipelinePath)

		p, err := l.builder.CreatePipelineFromPath(context.Background(), pipelinePath, pipeline.WithMutate())
		if err != nil {
			return nil, errors.Wrapf(err, "error creating pipeline from path '%s'", pipelinePath)
		}
		pipelines = append(pipelines, p)
	}

	l.logger.Debugf("constructed %d pipelines", len(pipelines))
	return pipelines, nil
}

type PipelineAnalysisResult struct {
	Pipelines []*PipelineIssues `json:"pipelines"`
}

func (p *PipelineAnalysisResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Pipelines)
}

// ErrorCount returns the number of errors found in an analysis result.
func (p *PipelineAnalysisResult) ErrorCount() int {
	count := 0
	for _, pipelineIssues := range p.Pipelines {
		for rule, issues := range pipelineIssues.Issues {
			if rule.GetSeverity() == ValidatorSeverityCritical {
				count += len(issues)
			}
		}
	}

	return count
}

// WarningCount returns the number of warnings, a.k.a non-critical issues found in an analysis result.
func (p *PipelineAnalysisResult) WarningCount() int {
	count := 0
	for _, pipelineIssues := range p.Pipelines {
		for rule, issues := range pipelineIssues.Issues {
			if rule.GetSeverity() == ValidatorSeverityWarning {
				count += len(issues)
			}
		}
	}

	return count
}

type PipelineIssues struct {
	Pipeline *pipeline.Pipeline
	Issues   map[Rule][]*Issue
}

func (p *PipelineIssues) MarshalJSON() ([]byte, error) {
	type IssueSummary struct {
		Asset       string   `json:"asset"`
		Description string   `json:"description"`
		Context     []string `json:"context"`
		Severity    string   `json:"severity"`
	}

	severityNames := map[ValidatorSeverity]string{
		ValidatorSeverityCritical: "critical",
		ValidatorSeverityWarning:  "warning",
	}

	issuesByAsset := make(map[string][]*IssueSummary)

	for rule, issues := range p.Issues {
		for _, issue := range issues {
			if issue.Task == nil {
				continue
			}

			ctx := make([]string, 0, len(issue.Context))
			if issue.Context != nil {
				ctx = issue.Context
			}

			issuesByAsset[issue.Task.Name] = append(issuesByAsset[issue.Task.Name], &IssueSummary{
				Asset:       issue.Task.Name,
				Description: issue.Description,
				Context:     ctx,
				Severity:    severityNames[rule.GetSeverity()],
			})
		}
	}

	return json.Marshal(struct {
		Pipeline string                     `json:"pipeline"`
		Issues   map[string][]*IssueSummary `json:"issues"`
	}{
		Pipeline: p.Pipeline.Name,
		Issues:   issuesByAsset,
	})
}

func (l *Linter) LintPipelines(pipelines []*pipeline.Pipeline) (*PipelineAnalysisResult, error) {
	result := &PipelineAnalysisResult{}

	for _, p := range pipelines {
		pipelineResult, err := l.LintPipeline(p)
		if err != nil {
			return nil, err
		}
		result.Pipelines = append(result.Pipelines, pipelineResult)
	}

	return result, nil
}

func (l *Linter) LintPipeline(p *pipeline.Pipeline) (*PipelineIssues, error) {
	return RunLintRulesOnPipeline(p, l.rules)
}

func RunLintRulesOnPipeline(p *pipeline.Pipeline, rules []Rule) (*PipelineIssues, error) {
	pipelineResult := &PipelineIssues{
		Pipeline: p,
		Issues:   make(map[Rule][]*Issue),
	}
	ctx := context.Background()

	policyRules, err := loadPolicy(p.DefinitionFile.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to load policy: %w", err)
	}
	if len(policyRules) > 0 {
		rules = slices.Concat([]Rule{}, rules, policyRules)
	}

	for _, rule := range rules {
		levels := rule.GetApplicableLevels()
		if slices.Contains(levels, LevelPipeline) {
			issues, err := rule.Validate(p)
			if err != nil {
				return nil, err
			}
			if len(issues) > 0 {
				pipelineResult.Issues[rule] = append(pipelineResult.Issues[rule], issues...)
			}
		} else if slices.Contains(levels, LevelAsset) {
			for _, asset := range p.Assets {
				issues, err := rule.ValidateAsset(ctx, p, asset)
				if err != nil {
					return nil, err
				}
				if len(issues) > 0 {
					pipelineResult.Issues[rule] = append(pipelineResult.Issues[rule], issues...)
				}
			}
		}
	}

	return pipelineResult, nil
}

func EnsureNoNestedPipelines(pipelinePaths []string) error {
	var previousPath string
	for i, path := range pipelinePaths {
		if i != 0 && strings.HasPrefix(path, previousPath+"/") {
			return fmt.Errorf("nested pipelines are not allowed: seems like '%s' is already a parent pipeline for '%s'", previousPath, path)
		}

		previousPath = path
	}

	return nil
}
