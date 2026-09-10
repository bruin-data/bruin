package lint

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"sync"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	semantic "github.com/bruin-data/bruin/semantic-engine"
	"github.com/spf13/afero"
)

type semanticLayerChecker struct {
	fs     afero.Fs
	finder repoFinder
	seen   sync.Map
}

func (c *semanticLayerChecker) Validate(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	dir := semanticDirForPipeline(ctx, c.fs, p, c.finder)
	if dir == "" || !semanticDirExists(c.fs, dir) {
		return nil, nil
	}
	if _, loaded := c.seen.LoadOrStore(dir, true); loaded {
		return nil, nil
	}

	issues, models, err := loadSemanticCatalogIssues(c.fs, dir)
	if err != nil {
		return issues, nil
	}

	if names := semantic.Names(models); len(names) > 0 {
		if _, err := semantic.NewEngineWithModels(models[names[0]], models); err != nil {
			issues = append(issues, &Issue{
				Description: fmt.Sprintf("Semantic catalog is invalid: %s", err),
			})
		}
	}

	return issues, nil
}

type semanticQueryDryRunner struct {
	fs          afero.Fs
	finder      repoFinder
	connections connectionManager
	seen        sync.Map
}

func GetSemanticQueryDryRunRule(fs afero.Fs, finder repoFinder, connections connectionManager) *SimpleRule {
	runner := &semanticQueryDryRunner{
		fs:          fs,
		finder:      finder,
		connections: connections,
	}
	return &SimpleRule{
		Identifier:       "semantic-query-dry-run",
		Fast:             false,
		Severity:         ValidatorSeverityCritical,
		Validator:        runner.Validate,
		ApplicableLevels: []Level{LevelPipeline},
	}
}

func (r *semanticQueryDryRunner) Validate(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	dir := semanticDirForPipeline(ctx, r.fs, p, r.finder)
	if dir == "" || !semanticDirExists(r.fs, dir) {
		return nil, nil
	}
	if _, loaded := r.seen.Load(dir); loaded {
		return nil, nil
	}

	_, models, err := loadSemanticCatalogIssues(r.fs, dir)
	if err != nil {
		return nil, nil
	}

	queryable := queryableSemanticModels(models)
	if len(queryable) == 0 {
		r.seen.Store(dir, true)
		return nil, nil
	}

	validator := queryValidatorForPipeline(p, r.connections)
	if validator == nil {
		return nil, nil
	}
	r.seen.Store(dir, true)

	var issues []*Issue
	for _, model := range queryable {
		q := &query.Query{Query: model.Source.DryRunSQL(model.Name)}
		valid, err := validator.IsValid(query.WithQueryType(ctx, query.QueryTypeDryRun), q)
		switch {
		case err != nil:
			issues = append(issues, &Issue{
				Description: fmt.Sprintf("Failed to validate semantic model %q query: %s", model.Name, err),
				Context:     []string{q.Query},
			})
		case !valid:
			issues = append(issues, &Issue{
				Description: fmt.Sprintf("Semantic model %q query is invalid: %s", model.Name, q.Query),
				Context:     []string{q.Query},
			})
		}
	}

	return issues, nil
}

func loadSemanticCatalogIssues(fs afero.Fs, dir string) ([]*Issue, map[string]*semantic.Model, error) {
	models, invalid, err := semantic.LoadDirPartialFS(fs, dir)
	if err != nil {
		return []*Issue{{
			Description: fmt.Sprintf("Failed to load semantic models from '%s': %s", dir, err),
		}}, nil, err
	}

	names := make([]string, 0, len(invalid))
	for name := range invalid {
		names = append(names, name)
	}
	sort.Strings(names)

	issues := make([]*Issue, 0, len(names))
	for _, name := range names {
		issues = append(issues, &Issue{
			Description: fmt.Sprintf("Semantic model %q is invalid: %s", name, invalid[name]),
		})
	}
	return issues, models, nil
}

func queryableSemanticModels(models map[string]*semantic.Model) []*semantic.Model {
	names := semantic.Names(models)
	out := make([]*semantic.Model, 0)
	for _, name := range names {
		model := models[name]
		if model != nil && model.Source.IsQueryable() {
			out = append(out, model)
		}
	}
	return out
}

func queryValidatorForPipeline(p *pipeline.Pipeline, connections connectionManager) queryValidator {
	if p == nil || connections == nil {
		return nil
	}
	for _, asset := range p.Assets {
		if asset == nil || !asset.IsSQLAsset() {
			continue
		}
		name, err := p.GetConnectionNameForAsset(asset)
		if err != nil || name == "" {
			continue
		}
		conn := connections.GetConnection(name)
		validator, ok := conn.(queryValidator)
		if ok {
			return validator
		}
	}
	return nil
}

func semanticDirForPipeline(ctx context.Context, fs afero.Fs, p *pipeline.Pipeline, finder repoFinder) string {
	if configPath, ok := ctx.Value(config.ConfigFilePathContextKey).(string); ok && configPath != "" {
		return filepath.Join(filepath.Dir(configPath), "semantic")
	}
	if p == nil || p.DefinitionFile.Path == "" {
		return ""
	}

	dir := filepath.Dir(p.DefinitionFile.Path)
	for {
		if hasBruinConfig(fs, dir) {
			return filepath.Join(dir, "semantic")
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	if finder != nil {
		repo, err := finder.Repo(p.DefinitionFile.Path)
		if err == nil && repo != nil && repo.Path != "" {
			return filepath.Join(repo.Path, "semantic")
		}
	}
	return ""
}

func hasBruinConfig(fs afero.Fs, dir string) bool {
	info, err := fs.Stat(filepath.Join(dir, ".bruin.yml"))
	return err == nil && !info.IsDir()
}

func semanticDirExists(fs afero.Fs, dir string) bool {
	info, err := fs.Stat(dir)
	if err != nil {
		return false
	}
	return info.IsDir()
}
