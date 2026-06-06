package docsgen

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/git"
	bruinpath "github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

var defaultPipelineDefinitionFiles = []string{"pipeline.yml", "pipeline.yaml"}

type PipelineBuilder interface {
	CreatePipelinesFromPath(ctx context.Context, path string, opts ...pipeline.CreatePipelineOption) ([]*pipeline.Pipeline, error)
}

type Options struct {
	InputPath               string
	OutputPath              string
	Title                   string
	Variant                 string
	PipelineDefinitionFiles []string
	GeneratedAt             time.Time
	// ExcludeCode strips asset source content from the generated docs. By
	// default the full asset source is embedded so it can be viewed inline.
	ExcludeCode bool
}

type Result struct {
	OutputPath    string
	SearchRoot    string
	PipelineCount int
	AssetCount    int
}

type siteData struct {
	Version     int                  `json:"version"`
	Title       string               `json:"title"`
	GeneratedAt string               `json:"generated_at"`
	SearchRoot  string               `json:"search_root"`
	Repository  *repositoryInfo      `json:"repository,omitempty"`
	Stats       stats                `json:"stats"`
	Pipelines   []*pipeline.Pipeline `json:"pipelines"`
}

type repositoryInfo struct {
	Name string `json:"name"`
}

type stats struct {
	Pipelines int `json:"pipelines"`
	Assets    int `json:"assets"`
}

func Generate(ctx context.Context, builder PipelineBuilder, opts Options) (*Result, error) {
	if builder == nil {
		return nil, errors.New("pipeline builder is required")
	}

	pipelineDefinitionFiles := opts.PipelineDefinitionFiles
	if len(pipelineDefinitionFiles) == 0 {
		pipelineDefinitionFiles = defaultPipelineDefinitionFiles
	}

	inputPathWasDefaulted := opts.InputPath == ""
	inputPath := opts.InputPath
	if inputPath == "" {
		inputPath = "."
	}

	outputPath := opts.OutputPath
	if outputPath == "" {
		outputPath = "bruin-docs.html"
	}

	title := opts.Title
	if title == "" {
		title = "Bruin Docs"
	}

	generatedAt := opts.GeneratedAt
	if generatedAt.IsZero() {
		generatedAt = time.Now().UTC()
	}

	searchRoot, repo, err := resolveSearchRoot(inputPath, inputPathWasDefaulted, pipelineDefinitionFiles)
	if err != nil {
		return nil, err
	}

	pipelines, err := collectPipelines(ctx, builder, searchRoot, opts.Variant, pipelineDefinitionFiles)
	if err != nil {
		return nil, err
	}
	if len(pipelines) == 0 {
		return nil, fmt.Errorf("no Bruin pipelines found under %s", searchRoot)
	}

	assetCount := 0
	for _, pl := range pipelines {
		if opts.ExcludeCode {
			pl.WipeContentOfAssets()
		}
		relativizePipelinePaths(pl, searchRoot)
		sort.SliceStable(pl.Assets, func(i, j int) bool {
			return strings.ToLower(pl.Assets[i].Name) < strings.ToLower(pl.Assets[j].Name)
		})
		assetCount += len(pl.Assets)
	}
	sort.SliceStable(pipelines, func(i, j int) bool {
		return pipelineSortKey(pipelines[i]) < pipelineSortKey(pipelines[j])
	})

	relSearchRoot := "."
	repoInfo := (*repositoryInfo)(nil)
	if repo != nil {
		repoInfo = &repositoryInfo{
			Name: filepath.Base(repo.Path),
		}
	}

	data := siteData{
		Version:     1,
		Title:       title,
		GeneratedAt: generatedAt.Format(time.RFC3339),
		SearchRoot:  relSearchRoot,
		Repository:  repoInfo,
		Stats: stats{
			Pipelines: len(pipelines),
			Assets:    assetCount,
		},
		Pipelines: pipelines,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal docs data: %w", err)
	}

	htmlBytes, err := renderHTML(title, string(jsonData))
	if err != nil {
		return nil, err
	}

	absoluteOutputPath, err := filepath.Abs(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve output path %q: %w", outputPath, err)
	}
	if err := os.MkdirAll(filepath.Dir(absoluteOutputPath), 0o755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	if err := os.WriteFile(absoluteOutputPath, htmlBytes, 0o600); err != nil {
		return nil, fmt.Errorf("failed to write docs HTML: %w", err)
	}
	if err := os.Chmod(absoluteOutputPath, 0o600); err != nil {
		return nil, fmt.Errorf("failed to set docs HTML permissions: %w", err)
	}

	return &Result{
		OutputPath:    absoluteOutputPath,
		SearchRoot:    searchRoot,
		PipelineCount: len(pipelines),
		AssetCount:    assetCount,
	}, nil
}

func collectPipelines(ctx context.Context, builder PipelineBuilder, searchRoot, variant string, pipelineDefinitionFiles []string) ([]*pipeline.Pipeline, error) {
	pipelinePaths, err := bruinpath.GetPipelinePaths(searchRoot, pipelineDefinitionFiles)
	if err != nil {
		return nil, fmt.Errorf("failed to discover pipelines: %w", err)
	}
	sort.Strings(pipelinePaths)

	createOpts := []pipeline.CreatePipelineOption{pipeline.WithMutate()}
	if variant != "" {
		createOpts = append(createOpts, pipeline.WithVariant(variant))
	}

	pipelines := make([]*pipeline.Pipeline, 0, len(pipelinePaths))
	for _, pipelinePath := range pipelinePaths {
		created, err := builder.CreatePipelinesFromPath(ctx, pipelinePath, createOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to parse pipeline at %s: %w", pipelinePath, err)
		}
		pipelines = append(pipelines, created...)
	}

	return pipelines, nil
}

func resolveSearchRoot(inputPath string, preferRepoRoot bool, pipelineDefinitionFiles []string) (string, *git.Repo, error) {
	absoluteInputPath, err := filepath.Abs(inputPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to resolve input path %q: %w", inputPath, err)
	}

	stat, err := os.Stat(absoluteInputPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to inspect input path %q: %w", inputPath, err)
	}

	repo, repoErr := git.FindRepoFromPath(absoluteInputPath)
	if repoErr != nil && !errors.Is(repoErr, git.ErrNoGitRepoFound) {
		return "", nil, fmt.Errorf("failed to find git repository for %q: %w", inputPath, repoErr)
	}

	if preferRepoRoot && repo != nil {
		return repo.Path, repo, nil
	}

	if stat.IsDir() {
		return absoluteInputPath, repo, nil
	}

	pipelineRoot, err := bruinpath.GetPipelineRootFromTask(absoluteInputPath, pipelineDefinitionFiles)
	if err != nil {
		return "", repo, fmt.Errorf("failed to find pipeline for %q: %w", inputPath, err)
	}
	return pipelineRoot, repo, nil
}

func relativizePipelinePaths(pl *pipeline.Pipeline, root string) {
	pl.DefinitionFile.Path = relPath(root, pl.DefinitionFile.Path)
	for _, asset := range pl.Assets {
		asset.ExecutableFile.Path = relPath(root, asset.ExecutableFile.Path)
		asset.DefinitionFile.Path = relPath(root, asset.DefinitionFile.Path)
	}
}

func relPath(root, value string) string {
	if value == "" {
		return value
	}
	if !filepath.IsAbs(value) {
		return filepath.ToSlash(value)
	}
	rel, err := filepath.Rel(root, value)
	if err != nil || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
		return filepath.ToSlash(filepath.Base(value))
	}
	return filepath.ToSlash(rel)
}

func pipelineSortKey(pl *pipeline.Pipeline) string {
	return strings.ToLower(pl.Name) + "\x00" + strings.ToLower(pl.SelectedVariant) + "\x00" + pl.DefinitionFile.Path
}

func renderHTML(title, jsonData string) ([]byte, error) {
	appJS, err := staticFiles.ReadFile("static/app.min.js")
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded docs app: %w", err)
	}
	appCSS, err := staticFiles.ReadFile("static/style.min.css")
	if err != nil {
		return nil, fmt.Errorf("failed to read embedded docs styles: %w", err)
	}

	var out strings.Builder
	out.Grow(len(appCSS) + len(appJS) + len(jsonData) + len(title) + 260)
	out.WriteString(`<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="color-scheme" content="light"><title>`)
	out.WriteString(html.EscapeString(title))
	out.WriteString(`</title><style>`)
	out.Write(appCSS)
	out.WriteString(`</style></head><body><div id="app"></div><script id="bruin-docs-data" type="application/json">`)
	out.WriteString(jsonData)
	out.WriteString(`</script><script>`)
	out.Write(appJS)
	out.WriteString(`</script></body></html>`)

	return []byte(out.String()), nil
}
