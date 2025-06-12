package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/spf13/afero"
)

var PipelineDefinitionFiles = []string{"pipeline.yml", "pipeline.yaml"}

var (
	fs = afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 0)

	faint          = color.New(color.Faint).SprintFunc()
	infoPrinter    = color.New(color.Bold)
	errorPrinter   = color.New(color.FgRed, color.Bold)
	warningPrinter = color.New(color.FgYellow, color.Bold)
	successPrinter = color.New(color.FgGreen, color.Bold)

	assetsDirectoryNames = []string{"tasks", "assets"}

	builderConfig = pipeline.BuilderConfig{
		PipelineFileName:    PipelineDefinitionFiles,
		TasksDirectoryNames: assetsDirectoryNames,
		TasksFileSuffixes:   []string{"task.yml", "task.yaml", "asset.yml", "asset.yaml"},
	}

	DefaultGlossaryReader = &glossary.GlossaryReader{
		RepoFinder: &git.RepoFinder{},
		FileNames:  []string{"glossary.yml", "glossary.yaml"},
	}

	DefaultPipelineBuilder = pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs, DefaultGlossaryReader)
)

func renderAssetParamsMutator(renderer jinja.RendererInterface) pipeline.AssetMutator {
	return func(ctx context.Context, a *pipeline.Asset, p *pipeline.Pipeline) (*pipeline.Asset, error) {
		renderer = renderer.CloneForAsset(ctx, p, a)
		for key, value := range a.Parameters {
			renderedValue, err := renderer.Render(value)
			if err != nil {
				return nil, fmt.Errorf("error rendering parameter %q: %w", key, err)
			}
			a.Parameters[key] = renderedValue
		}

		return a, nil
	}
}

func variableOverridesMutator(variables []string) pipeline.PipelineMutator {
	return func(ctx context.Context, p *pipeline.Pipeline) (*pipeline.Pipeline, error) {
		overrides := map[string]any{}
		for _, variable := range variables {
			parsed, err := parseVariable(variable)
			if err != nil {
				return nil, fmt.Errorf("invalid variable override %q: %w", variable, err)
			}
			for key, value := range parsed {
				overrides[key] = value
			}
		}
		err := p.Variables.Merge(overrides)
		if err != nil {
			return nil, fmt.Errorf("invalid variable overrides: %w", err)
		}
		return p, nil
	}
}

func parseVariable(variable string) (map[string]any, error) {
	var composite map[string]any
	variable = strings.TrimSpace(variable)

	err := json.Unmarshal([]byte(variable), &composite)
	if err == nil {
		return composite, nil
	}

	// this is a heuristic to detect if the variable is a JSON object
	if strings.HasPrefix(variable, "{") {
		return nil, err
	}

	segments := strings.SplitN(variable, "=", 2)
	if len(segments) != 2 {
		return nil, errors.New("variable must of form key=value")
	}

	key := strings.TrimSpace(segments[0])
	var value any
	if err := json.Unmarshal([]byte(segments[1]), &value); err != nil {
		return nil, err
	}
	return map[string]any{key: value}, nil
}
