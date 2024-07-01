package cmd

import (
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/spf13/afero"
)

const (
	pipelineDefinitionFile = "pipeline.yml"
)

var (
	fs = afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 0)

	faint          = color.New(color.Faint).SprintFunc()
	infoPrinter    = color.New(color.Bold)
	errorPrinter   = color.New(color.FgRed, color.Bold)
	successPrinter = color.New(color.FgGreen, color.Bold)

	builderConfig = pipeline.BuilderConfig{
		PipelineFileName:    pipelineDefinitionFile,
		TasksDirectoryNames: []string{"tasks", "assets"},
		TasksFileSuffixes:   []string{"task.yml", "task.yaml", "asset.yml", "asset.yaml"},
	}

	glossaryReader = &glossary.GlossaryReader{
		RepoFinder: &git.RepoFinder{},
		FileNames:  []string{"glossary.yml", "glossary.yaml"},
	}

	DefaultPipelineBuilder = pipeline.NewBuilder(builderConfig, pipeline.CreateTaskFromYamlDefinition(fs), pipeline.CreateTaskFromFileComments(fs), fs, glossaryReader)
)
