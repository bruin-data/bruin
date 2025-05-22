package cmd

import (
	"context"
	"encoding/json"
	"fmt"

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
			var v map[string]any
			err := json.Unmarshal([]byte(variable), &v)
			if err != nil {
				return nil, fmt.Errorf("invalid variable %q: %w", variable, err)
			}
			for k, v := range v {
				overrides[k] = v
			}
		}
		err := p.Variables.Merge(overrides)
		if err != nil {
			return nil, fmt.Errorf("invalid variable overrides: %w", err)
		}
		return p, nil
	}
}
