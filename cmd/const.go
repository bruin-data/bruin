package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/spf13/afero"
)

var pipelineDefinitionFiles = []string{"pipeline.yml", "pipeline.yaml"}

var (
	fs = afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 0)

	faint          = color.New(color.Faint).SprintFunc()
	infoPrinter    = color.New(color.Bold)
	errorPrinter   = color.New(color.FgRed, color.Bold)
	warningPrinter = color.New(color.FgYellow, color.Bold)
	successPrinter = color.New(color.FgGreen, color.Bold)

	assetsDirectoryNames = []string{"tasks", "assets"}

	builderConfig = pipeline.BuilderConfig{
		PipelineFileName:    pipelineDefinitionFiles,
		TasksDirectoryNames: assetsDirectoryNames,
		TasksFileSuffixes:   []string{"task.yml", "task.yaml", "asset.yml", "asset.yaml"},
	}

	DefaultGlossaryReader = &glossary.GlossaryReader{
		RepoFinder: &git.RepoFinder{},
		FileNames:  []string{"glossary.yml", "glossary.yaml"},
	}

	DefaultPipelineBuilder = pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs, DefaultGlossaryReader)
)

func variableOverridesMutator(variables []string) pipeline.PipelineMutator {
	return func(ctx context.Context, p *pipeline.Pipeline) (*pipeline.Pipeline, error) {
		var overrides = map[string]any{}
		for _, variable := range variables {
			var composite map[string]any
			variable = strings.TrimSpace(variable)

			err := json.Unmarshal([]byte(variable), &composite)
			if err == nil {
				for k, v := range composite {
					overrides[k] = v
				}
				continue
			}

			// this is a heuristic to detect if the variable is a JSON object
			if strings.HasPrefix(variable, "{") {
				return nil, fmt.Errorf("invalid variable override %q: %w", variable, err)
			}

			segments := strings.SplitN(variable, "=", 2)
			if len(segments) != 2 {
				return nil, fmt.Errorf("invalid variable override key %q: variable must of form key=value", variable)
			}

			key := strings.TrimSpace(segments[0])
			var value any
			if err := json.Unmarshal([]byte(segments[1]), &value); err != nil {
				return nil, fmt.Errorf("invalid variable override value %q: %w", variable, err)
			}
			overrides[key] = value
		}
		err := p.Variables.Merge(overrides)
		if err != nil {
			return nil, fmt.Errorf("invalid variable overrides: %w", err)
		}
		return p, nil
	}
}
