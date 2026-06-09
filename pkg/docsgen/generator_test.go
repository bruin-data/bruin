package docsgen

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

type fakePipelineBuilder struct {
	pipelines map[string][]*pipeline.Pipeline
}

func (f fakePipelineBuilder) CreatePipelinesFromPath(_ context.Context, path string, _ ...pipeline.CreatePipelineOption) ([]*pipeline.Pipeline, error) {
	return f.pipelines[path], nil
}

func TestGenerateWritesSelfContainedHTMLWithEmbeddedPipelineJSON(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, ".git"), 0o755))
	pipelineDir := filepath.Join(root, "warehouse")
	assetsDir := filepath.Join(pipelineDir, "assets")
	require.NoError(t, os.MkdirAll(assetsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "pipeline.yml"), []byte("name: warehouse\n"), 0o644))

	pipelinePath := filepath.Join(pipelineDir, "pipeline.yml")
	assetPath := filepath.Join(assetsDir, "orders.sql")
	builder := fakePipelineBuilder{
		pipelines: map[string][]*pipeline.Pipeline{
			pipelineDir: {
				{
					Name:           "warehouse",
					DefinitionFile: pipeline.DefinitionFile{Name: "pipeline.yml", Path: pipelinePath},
					Assets: []*pipeline.Asset{
						{
							Name:           "orders",
							Type:           pipeline.AssetTypeBigqueryQuery,
							Description:    "Modeled order facts",
							DefinitionFile: pipeline.TaskDefinitionFile{Name: "orders.sql", Path: assetPath, Type: "asset"},
							ExecutableFile: pipeline.ExecutableFile{Name: "orders.sql", Path: assetPath, Content: "select * from raw.orders"},
						},
					},
				},
			},
		},
	}

	outputPath := filepath.Join(root, "site", "docs.html")
	result, err := Generate(t.Context(), builder, Options{
		InputPath:   root,
		OutputPath:  outputPath,
		Title:       "Warehouse Docs",
		GeneratedAt: time.Date(2026, 6, 3, 12, 0, 0, 0, time.UTC),
	})
	require.NoError(t, err)
	require.Equal(t, 1, result.PipelineCount)
	require.Equal(t, 1, result.AssetCount)
	require.Equal(t, outputPath, result.OutputPath)

	html, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.Contains(t, string(html), "bruin-docs-data")
	require.NotContains(t, string(html), root)
	info, err := os.Stat(outputPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	data := extractDocsData(t, string(html))
	require.Equal(t, "Warehouse Docs", data["title"])
	require.Equal(t, "2026-06-03T12:00:00Z", data["generated_at"])

	pipelines, ok := data["pipelines"].([]any)
	require.True(t, ok)
	require.Len(t, pipelines, 1)

	firstPipeline := pipelines[0].(map[string]any)
	require.Equal(t, "warehouse", firstPipeline["name"])
	require.Equal(t, "warehouse/pipeline.yml", firstPipeline["definition_file"].(map[string]any)["path"])

	assets := firstPipeline["assets"].([]any)
	require.Len(t, assets, 1)
	firstAsset := assets[0].(map[string]any)
	require.Equal(t, "orders", firstAsset["name"])
	require.Equal(t, "select * from raw.orders", firstAsset["executable_file"].(map[string]any)["content"])
	require.Equal(t, "warehouse/assets/orders.sql", firstAsset["executable_file"].(map[string]any)["path"])
}

func TestGenerateDoesNotEmbedAbsolutePathsForFilesOutsideSearchRoot(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outsideRoot := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, ".git"), 0o755))
	pipelineDir := filepath.Join(root, "warehouse")
	require.NoError(t, os.MkdirAll(pipelineDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "pipeline.yml"), []byte("name: warehouse\n"), 0o644))

	pipelinePath := filepath.Join(pipelineDir, "pipeline.yml")
	outsideAssetPath := filepath.Join(outsideRoot, "shared", "orders.sql")
	builder := fakePipelineBuilder{
		pipelines: map[string][]*pipeline.Pipeline{
			pipelineDir: {
				{
					Name:           "warehouse",
					DefinitionFile: pipeline.DefinitionFile{Name: "pipeline.yml", Path: pipelinePath},
					Assets: []*pipeline.Asset{
						{
							Name:           "orders",
							Type:           pipeline.AssetTypeBigqueryQuery,
							DefinitionFile: pipeline.TaskDefinitionFile{Name: "orders.sql", Path: outsideAssetPath, Type: "asset"},
							ExecutableFile: pipeline.ExecutableFile{Name: "orders.sql", Path: outsideAssetPath, Content: "select * from raw.orders"},
						},
					},
				},
			},
		},
	}

	outputPath := filepath.Join(root, "site", "docs.html")
	_, err := Generate(t.Context(), builder, Options{
		InputPath:  root,
		OutputPath: outputPath,
	})
	require.NoError(t, err)

	html, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	require.NotContains(t, string(html), outsideRoot)
	require.NotContains(t, string(html), outsideAssetPath)

	data := extractDocsData(t, string(html))
	asset := data["pipelines"].([]any)[0].(map[string]any)["assets"].([]any)[0].(map[string]any)
	require.Equal(t, "orders.sql", asset["definition_file"].(map[string]any)["path"])
	require.Equal(t, "orders.sql", asset["executable_file"].(map[string]any)["path"])
}

func TestGenerateExcludeCodeStripsAssetContent(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, ".git"), 0o755))
	pipelineDir := filepath.Join(root, "warehouse")
	assetsDir := filepath.Join(pipelineDir, "assets")
	require.NoError(t, os.MkdirAll(assetsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(pipelineDir, "pipeline.yml"), []byte("name: warehouse\n"), 0o644))

	pipelinePath := filepath.Join(pipelineDir, "pipeline.yml")
	assetPath := filepath.Join(assetsDir, "orders.sql")
	builder := fakePipelineBuilder{
		pipelines: map[string][]*pipeline.Pipeline{
			pipelineDir: {
				{
					Name:           "warehouse",
					DefinitionFile: pipeline.DefinitionFile{Name: "pipeline.yml", Path: pipelinePath},
					Assets: []*pipeline.Asset{
						{
							Name:           "orders",
							Type:           pipeline.AssetTypeBigqueryQuery,
							DefinitionFile: pipeline.TaskDefinitionFile{Name: "orders.sql", Path: assetPath, Type: "asset"},
							ExecutableFile: pipeline.ExecutableFile{Name: "orders.sql", Path: assetPath, Content: "select * from raw.orders"},
						},
					},
				},
			},
		},
	}

	outputPath := filepath.Join(root, "site", "docs.html")
	_, err := Generate(t.Context(), builder, Options{
		InputPath:   root,
		OutputPath:  outputPath,
		ExcludeCode: true,
	})
	require.NoError(t, err)

	html, err := os.ReadFile(outputPath)
	require.NoError(t, err)

	data := extractDocsData(t, string(html))
	asset := data["pipelines"].([]any)[0].(map[string]any)["assets"].([]any)[0].(map[string]any)
	require.Empty(t, asset["executable_file"].(map[string]any)["content"])
}

func TestGenerateReturnsHelpfulErrorWhenNoPipelinesAreFound(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	outputPath := filepath.Join(root, "docs.html")

	_, err := Generate(t.Context(), fakePipelineBuilder{}, Options{
		InputPath:  root,
		OutputPath: outputPath,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no Bruin pipelines found")
}

func extractDocsData(t *testing.T, html string) map[string]any {
	t.Helper()

	startMarker := `<script id="bruin-docs-data" type="application/json">`
	start := strings.Index(html, startMarker)
	require.NotEqual(t, -1, start)
	start += len(startMarker)
	end := strings.Index(html[start:], "</script>")
	require.NotEqual(t, -1, end)

	var data map[string]any
	require.NoError(t, json.Unmarshal([]byte(html[start:start+end]), &data))
	return data
}
