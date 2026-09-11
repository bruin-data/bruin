package lint

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"

	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	semantic "github.com/bruin-data/bruin/semantic-engine"
	"github.com/spf13/afero"
)

// SemanticQueryDryRunRule validates the compiled queries of semantic models
// using the same dry-run capabilities as SQL asset validations.
//
// Semantic models live in the repository-level `semantic` directory and compile
// to SQL via the semantic engine. Structural problems (unparsable YAML, missing
// names, unresolvable refs) are reported without a database connection. When at
// least one dry-run-capable connection is available from the pipelines under
// validation, every generated query is dry-run via `IsValid` with
// `QueryTypeDryRun`, exactly like `QueryValidatorRule` does for SQL assets.
type SemanticQueryDryRunRule struct {
	Connections connectionManager
	Fs          afero.Fs
	SemanticDir string
	Logger      logger.Logger
}

func (r *SemanticQueryDryRunRule) Name() string {
	return "semantic-query-dry-run"
}

func (r *SemanticQueryDryRunRule) IsFast() bool {
	return false
}

func (r *SemanticQueryDryRunRule) GetApplicableLevels() []Level {
	return []Level{LevelCrossPipeline}
}

func (r *SemanticQueryDryRunRule) GetSeverity() ValidatorSeverity {
	return ValidatorSeverityCritical
}

func (r *SemanticQueryDryRunRule) Validate(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	return r.ValidateCrossPipeline(ctx, []*pipeline.Pipeline{p})
}

func (r *SemanticQueryDryRunRule) ValidateAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	return r.ValidateCrossPipeline(ctx, []*pipeline.Pipeline{p})
}

func (r *SemanticQueryDryRunRule) ValidateCrossPipeline(ctx context.Context, pipelines []*pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	if r.SemanticDir == "" {
		return issues, nil
	}

	exists, err := afero.DirExists(r.Fs, r.SemanticDir)
	if err != nil {
		return issues, err
	}
	if !exists {
		r.debugf("skipping semantic validation, directory '%s' not found", r.SemanticDir)
		return issues, nil
	}

	models, invalid, err := semantic.LoadDirPartialFS(r.Fs, r.SemanticDir)
	if err != nil {
		// Reading the directory itself failed; surface it as a single issue
		// rather than failing the whole validation run.
		issues = append(issues, &Issue{
			Description: fmt.Sprintf("Failed to load semantic models from '%s': %s", r.SemanticDir, err),
		})
		return issues, nil
	}

	for _, name := range sortedKeysFromMap(invalid) {
		issues = append(issues, &Issue{
			Description: fmt.Sprintf("Invalid semantic model '%s': %s", name, invalid[name]),
		})
	}

	if len(models) == 0 {
		return issues, nil
	}

	if r.Connections == nil {
		r.debugf("skipping semantic query dry-run, no connection manager")
		return issues, nil
	}

	validators := r.collectValidators(pipelines)
	if len(validators) == 0 {
		r.debugf("skipping semantic query dry-run, no dry-run-capable connections found")
		return issues, nil
	}

	dryRunCtx := query.WithQueryType(ctx, query.QueryTypeDryRun)

	names := semantic.Names(models)
	for _, name := range names {
		model := models[name]
		queries, compileIssues := buildSemanticValidationQueries(model, models)
		issues = append(issues, compileIssues...)

		for _, vq := range queries {
			if err := r.dryRunQuery(dryRunCtx, validators, vq.sql); err != nil {
				issues = append(issues, &Issue{
					Description: fmt.Sprintf("Semantic model '%s' %s is invalid: %s", name, vq.label, err),
					Context:     []string{vq.sql},
				})
			}
		}
	}

	return issues, nil
}

func (r *SemanticQueryDryRunRule) debugf(template string, args ...interface{}) {
	if r.Logger != nil {
		r.Logger.Debugf(template, args...)
	}
}

// collectValidators returns the distinct dry-run-capable connections referenced
// by SQL assets across all pipelines, mirroring how QueryValidatorRule resolves
// `p.GetConnectionNameForAsset` per asset.
func (r *SemanticQueryDryRunRule) collectValidators(pipelines []*pipeline.Pipeline) []queryValidator {
	seen := make(map[string]bool)
	validators := make([]queryValidator, 0)

	for _, p := range pipelines {
		if p == nil {
			continue
		}
		for _, asset := range p.Assets {
			if asset == nil || !asset.IsSQLAsset() {
				continue
			}
			connName, err := p.GetConnectionNameForAsset(asset)
			if err != nil || connName == "" || seen[connName] {
				continue
			}
			seen[connName] = true

			raw := r.Connections.GetConnection(connName)
			if raw == nil {
				r.debugf("skipping semantic dry-run connection '%s', connection not found", connName)
				continue
			}
			validator, ok := raw.(queryValidator)
			if !ok {
				r.debugf("skipping semantic dry-run connection '%s', connection does not support dry-run", connName)
				continue
			}
			validators = append(validators, validator)
		}
	}

	return validators
}

// dryRunQuery succeeds when at least one validator accepts the query.
// Semantic models are warehouse-agnostic, so a model backed by BigQuery tables
// must not fail validation just because the repo also configures Snowflake.
func (r *SemanticQueryDryRunRule) dryRunQuery(ctx context.Context, validators []queryValidator, sql string) error {
	var lastErr error
	for _, validator := range validators {
		valid, err := validator.IsValid(ctx, &query.Query{Query: sql})
		if err != nil {
			lastErr = err
			continue
		}
		if !valid {
			lastErr = errors.New("query is invalid")
			continue
		}
		return nil
	}

	if lastErr == nil {
		lastErr = errors.New("query is invalid")
	}
	return lastErr
}

type semanticValidationQuery struct {
	label string
	sql   string
}

// buildSemanticValidationQueries compiles a set of queries that together
// exercise every query-like fragment of a semantic model: source table,
// dimension expressions (including time granularities), metric expressions,
// segment filters, and join conditions. Compile errors are returned as issues
// so callers can report them without a database connection.
func buildSemanticValidationQueries(model *semantic.Model, models map[string]*semantic.Model) ([]semanticValidationQuery, []*Issue) {
	issues := make([]*Issue, 0)
	queries := make([]semanticValidationQuery, 0)

	engine, err := semantic.NewEngineWithModels(model, models)
	if err != nil {
		issues = append(issues, &Issue{
			Description: fmt.Sprintf("Invalid semantic model '%s': %s", model.Name, err),
		})
		return queries, issues
	}

	addQuery := func(label string, q *semantic.Query) {
		sql, err := engine.GenerateSQL(q)
		if err != nil {
			issues = append(issues, &Issue{
				Description: fmt.Sprintf("Semantic model '%s' %s failed to compile: %s", model.Name, label, err),
			})
			return
		}
		queries = append(queries, semanticValidationQuery{label: label, sql: sql})
	}

	if len(model.Dimensions) == 0 && len(model.Metrics) == 0 {
		queries = append(queries, semanticValidationQuery{
			label: "source query",
			sql:   fmt.Sprintf("SELECT 1 FROM %s LIMIT 0", model.Source.Table),
		})
		return queries, issues
	}

	// Comprehensive query covering all dimensions and metrics at once.
	allDims := make([]semantic.DimensionRef, 0, len(model.Dimensions))
	for _, d := range model.Dimensions {
		allDims = append(allDims, semantic.DimensionRef{Name: d.Name})
	}
	allMetrics := make([]string, 0, len(model.Metrics))
	for _, m := range model.Metrics {
		allMetrics = append(allMetrics, m.Name)
	}
	addQuery("query", &semantic.Query{Dimensions: allDims, Metrics: allMetrics})

	// One query per metric isolates metric expression and filter errors.
	for _, m := range model.Metrics {
		addQuery(fmt.Sprintf("metric '%s' query", m.Name), &semantic.Query{Metrics: []string{m.Name}})
	}

	// One query per dimension (and per granularity) isolates dimension errors.
	for _, d := range model.Dimensions {
		addQuery(fmt.Sprintf("dimension '%s' query", d.Name), &semantic.Query{
			Dimensions: []semantic.DimensionRef{{Name: d.Name}},
		})
		granularities := sortedKeys(d.Granularities)
		for _, g := range granularities {
			addQuery(fmt.Sprintf("dimension '%s' granularity '%s' query", d.Name, g), &semantic.Query{
				Dimensions: []semantic.DimensionRef{{Name: d.Name, Granularity: g}},
			})
		}
	}

	// One query per segment validates segment filters.
	firstSelector := firstMetricOrDimension(model)
	for _, s := range model.Segments {
		q := &semantic.Query{Segments: []string{s.Name}}
		if metric, ok := firstSelector.(string); ok {
			q.Metrics = []string{metric}
		} else if dim, ok := firstSelector.(semantic.DimensionRef); ok {
			q.Dimensions = []semantic.DimensionRef{dim}
		}
		addQuery(fmt.Sprintf("segment '%s' query", s.Name), q)
	}

	// One query per join validates join conditions via a qualified dimension
	// from the join target.
	for _, join := range model.Joins {
		targetDim := firstJoinTargetDimension(join, models)
		if targetDim == "" {
			continue
		}
		q := &semantic.Query{
			Dimensions: []semantic.DimensionRef{{Name: targetDim}},
		}
		if metric, ok := firstSelector.(string); ok {
			q.Metrics = []string{metric}
		}
		addQuery(fmt.Sprintf("join '%s' query", join.Name), q)
	}

	return queries, issues
}

func firstMetricOrDimension(model *semantic.Model) any {
	if len(model.Metrics) > 0 {
		return model.Metrics[0].Name
	}
	if len(model.Dimensions) > 0 {
		return semantic.DimensionRef{Name: model.Dimensions[0].Name}
	}
	return nil
}

func firstJoinTargetDimension(join semantic.Join, models map[string]*semantic.Model) string {
	targetName := join.Model
	if targetName == "" {
		targetName = join.Name
	}
	target, ok := models[targetName]
	if !ok || len(target.Dimensions) == 0 {
		return ""
	}
	relation := join.Name
	if relation == "" {
		relation = targetName
	}
	return relation + "." + target.Dimensions[0].Name
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedKeysFromMap[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// SemanticDirFromConfigPath resolves the repository-level semantic directory
// from the .bruin.yml config path, mirroring loadRepoSemanticModels in cmd.
func SemanticDirFromConfigPath(configPath string) string {
	if configPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(configPath), "semantic")
}

// GetSemanticQueryDryRunRule builds the cross-pipeline rule that dry-runs
// compiled semantic queries, mirroring GetCustomCheckQueryDryRunRule and
// GetHookQueryDryRunRule.
func GetSemanticQueryDryRunRule(connections connectionManager, fs afero.Fs, semanticDir string, log logger.Logger) *SimpleRule {
	rule := &SemanticQueryDryRunRule{
		Connections: connections,
		Fs:          fs,
		SemanticDir: semanticDir,
		Logger:      log,
	}
	return &SimpleRule{
		Identifier:             "semantic-query-dry-run",
		Fast:                   false,
		Severity:               ValidatorSeverityCritical,
		CrossPipelineValidator: rule.ValidateCrossPipeline,
		ApplicableLevels:       []Level{LevelCrossPipeline},
	}
}
