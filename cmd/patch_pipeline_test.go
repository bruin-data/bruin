package cmd

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	errors2 "github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func createPatchPipelineCommand() *cli.Command {
	return &cli.Command{
		Name:      "patch-pipeline",
		Usage:     "patch a single Bruin pipeline with the given fields",
		ArgsUsage: "[path to the pipeline definition]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "body",
				Usage:    "the JSON object containing the patch body",
				Required: false,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			pipelinePath := c.Args().Get(0)
			if pipelinePath == "" {
				printErrorJSON(errors2.New("empty pipeline path given, you must provide an existing pipeline path"))
				return cli.Exit("", 1)
			}

			p, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate(), pipeline.WithOnlyPipeline())
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to load pipeline"))
				return cli.Exit("", 1)
			}

			if err := json.Unmarshal([]byte(c.String("body")), &p); err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to apply patch to pipeline"))
				return cli.Exit("", 1)
			}

			if err := p.Persist(afero.NewOsFs()); err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to persist pipeline"))
				return cli.Exit("", 1)
			}

			return nil
		},
	}
}

//nolint:paralleltest // CLI tests cannot run in parallel due to race conditions in cli library
func TestPatchPipeline_BasicFunctionality(t *testing.T) {
	tests := []struct {
		name           string
		pipelinePath   string
		patchBody      map[string]interface{}
		expectedFields map[string]interface{}
	}{
		{
			name:         "patch simple pipeline name and retries",
			pipelinePath: path.AbsPathForTests(t, "../pkg/pipeline/testdata/persist/simple-pipeline.yml"),
			patchBody: map[string]interface{}{
				"name":    "patched-simple-pipeline",
				"retries": 5,
			},
			expectedFields: map[string]interface{}{
				"name":    "patched-simple-pipeline",
				"retries": 5,
			},
		},
		{
			name:         "patch complex pipeline concurrency and schedule",
			pipelinePath: path.AbsPathForTests(t, "../pkg/pipeline/testdata/persist/complex-pipeline.yml"),
			patchBody: map[string]interface{}{
				"concurrency": 10,
				"schedule":    "daily",
			},
			expectedFields: map[string]interface{}{
				"concurrency": 10,
				"schedule":    "daily",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tempDir := t.TempDir()
			tempPipelinePath := filepath.Join(tempDir, "pipeline.yml")

			originalContent, err := os.ReadFile(tt.pipelinePath)
			require.NoError(t, err)
			err = os.WriteFile(tempPipelinePath, originalContent, 0o644)
			require.NoError(t, err)

			app := &cli.Command{
				Name: "bruin",
				Commands: []*cli.Command{
					{
						Name:     "internal",
						Commands: []*cli.Command{createPatchPipelineCommand()},
					},
				},
			}

			patchJSON, err := json.Marshal(tt.patchBody)
			require.NoError(t, err)

			err = app.Run(context.Background(), []string{"bruin", "internal", "patch-pipeline", "--body", string(patchJSON), tempPipelinePath})
			require.NoError(t, err)

			directPipeline, err := pipeline.PipelineFromPath(tempPipelinePath, afero.NewOsFs())
			require.NoError(t, err)
			for field, expectedValue := range tt.expectedFields {
				switch field {
				case "name":
					assert.Equal(t, expectedValue, directPipeline.Name)
				case "retries":
					assert.Equal(t, expectedValue, directPipeline.Retries)
				case "concurrency":
					assert.Equal(t, expectedValue, directPipeline.Concurrency)
				case "schedule":
					assert.Equal(t, expectedValue, string(directPipeline.Schedule))
				}
			}
		})
	}
}

//nolint:paralleltest // CLI tests cannot run in parallel due to race conditions in cli library
func TestPatchPipeline_WithAssets(t *testing.T) {
	tempDir := t.TempDir()
	tempPipelinePath := filepath.Join(tempDir, "pipeline.yml")

	originalContent := `name: test-pipeline
schedule: daily`
	err := os.WriteFile(tempPipelinePath, []byte(originalContent), 0o644)
	require.NoError(t, err)

	patchBody := map[string]interface{}{
		"name": "pipeline-with-assets",
		"assets": []map[string]interface{}{
			{
				"name": "test-asset",
				"type": "python",
			},
		},
	}

	app := &cli.Command{
		Name: "bruin",
		Commands: []*cli.Command{
			{
				Name:     "internal",
				Commands: []*cli.Command{PatchPipeline()},
			},
		},
	}

	patchJSON, err := json.Marshal(patchBody)
	require.NoError(t, err)

	err = app.Run(context.Background(), []string{"bruin", "internal", "patch-pipeline", "--body", string(patchJSON), tempPipelinePath})
	require.NoError(t, err)

	p, err := pipeline.PipelineFromPath(tempPipelinePath, afero.NewOsFs())
	require.NoError(t, err)

	assert.Equal(t, "pipeline-with-assets", p.Name)

	fileContent, err := os.ReadFile(tempPipelinePath)
	require.NoError(t, err)
	assert.Contains(t, string(fileContent), "assets:")
	assert.Contains(t, string(fileContent), "test-asset")
	assert.Contains(t, string(fileContent), "python")
}

//nolint:paralleltest // CLI tests cannot run in parallel due to race conditions in cli library
func TestPatchPipeline_PreservesExistingFields(t *testing.T) {
	tempDir := t.TempDir()
	tempPipelinePath := filepath.Join(tempDir, "pipeline.yml")

	originalContent, err := os.ReadFile(path.AbsPathForTests(t, "../pkg/pipeline/testdata/persist/complex-pipeline.yml"))
	require.NoError(t, err)
	err = os.WriteFile(tempPipelinePath, originalContent, 0o644)
	require.NoError(t, err)

	patchBody := map[string]interface{}{
		"name": "updated-complex-pipeline",
	}

	app := &cli.Command{
		Name: "bruin",
		Commands: []*cli.Command{
			{
				Name:     "internal",
				Commands: []*cli.Command{PatchPipeline()},
			},
		},
	}

	patchJSON, err := json.Marshal(patchBody)
	require.NoError(t, err)

	err = app.Run(context.Background(), []string{"bruin", "internal", "patch-pipeline", "--body", string(patchJSON), tempPipelinePath})
	require.NoError(t, err)

	p, err := pipeline.PipelineFromPath(tempPipelinePath, afero.NewOsFs())
	require.NoError(t, err)

	assert.Equal(t, "updated-complex-pipeline", p.Name)

	assert.Equal(t, "hourly", string(p.Schedule))
	assert.Equal(t, "2024-01-01", p.StartDate)
	assert.Equal(t, 3, p.Retries)
	assert.Equal(t, 5, p.Concurrency)
	assert.True(t, p.Catchup)
	assert.True(t, p.Agent)
	assert.NotEmpty(t, p.DefaultConnections)
	assert.NotEmpty(t, p.Variables)
	assert.NotEmpty(t, p.Tags)
	assert.NotEmpty(t, p.Domains)
	assert.NotEmpty(t, p.Meta)
}

//nolint:paralleltest // CLI tests cannot run in parallel due to race conditions in cli library
func TestPatchPipeline_ErrorHandling(t *testing.T) {
	t.Skip("Error handling tests are disabled due to CLI v3 behavior with cli.Exit")
}

//nolint:paralleltest // CLI tests cannot run in parallel due to race conditions in cli library
func TestPatchPipeline_OnlyPipelineOption(t *testing.T) {
	tempDir := t.TempDir()
	tempPipelinePath := filepath.Join(tempDir, "pipeline.yml")

	originalContent := `name: test-pipeline
schedule: daily`
	err := os.WriteFile(tempPipelinePath, []byte(originalContent), 0o644)
	require.NoError(t, err)

	assetsDir := filepath.Join(tempDir, "assets")
	err = os.MkdirAll(assetsDir, 0o755)
	require.NoError(t, err)

	assetFile := filepath.Join(assetsDir, "test-asset.yml")
	assetContent := `name: filesystem-asset
type: python`
	err = os.WriteFile(assetFile, []byte(assetContent), 0o644)
	require.NoError(t, err)

	patchBody := map[string]interface{}{
		"name": "pipeline-with-patch-assets",
		"assets": []map[string]interface{}{
			{
				"name": "patch-asset",
				"type": "bq.sql",
			},
		},
	}

	app := &cli.Command{
		Name: "bruin",
		Commands: []*cli.Command{
			{
				Name:     "internal",
				Commands: []*cli.Command{PatchPipeline()},
			},
		},
	}

	patchJSON, err := json.Marshal(patchBody)
	require.NoError(t, err)

	err = app.Run(context.Background(), []string{"bruin", "internal", "patch-pipeline", "--body", string(patchJSON), tempPipelinePath})
	require.NoError(t, err)

	p, err := pipeline.PipelineFromPath(tempPipelinePath, afero.NewOsFs())
	require.NoError(t, err)

	assert.Equal(t, "pipeline-with-patch-assets", p.Name)

	fileContent, err := os.ReadFile(tempPipelinePath)
	require.NoError(t, err)
	assert.Contains(t, string(fileContent), "assets:")
	assert.Contains(t, string(fileContent), "patch-asset")
	assert.Contains(t, string(fileContent), "bq.sql")

	assert.NotContains(t, string(fileContent), "filesystem-asset")
}
