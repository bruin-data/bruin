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
	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/fatih/color"
	"github.com/spf13/afero"
)

var PipelineDefinitionFiles = []string{"pipeline.yml", "pipeline.yaml"}

var (
	fs = afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 0)

	faint          = color.New(color.Faint).SprintFunc()
	infoPrinter    = color.New(color.Bold)
	summaryPrinter = color.New()
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
	SeedAssetsValidator    = &lint.SimpleRule{
		Identifier:       "assets-seed-validation",
		Fast:             true,
		Severity:         lint.ValidatorSeverityCritical,
		AssetValidator:   lint.ValidateAssetSeedValidation,
		ApplicableLevels: []lint.Level{lint.LevelAsset},
	}
)

func renderAssetParamsMutator(renderer jinja.RendererInterface) pipeline.AssetMutator {
	return func(ctx context.Context, a *pipeline.Asset, p *pipeline.Pipeline) (*pipeline.Asset, error) {
		var err error
		renderer, err = renderer.CloneForAsset(ctx, p, a)
		if err != nil {
			return nil, fmt.Errorf("error creating renderer for asset %s: %w", a.Name, err)
		}
		for key, value := range a.Parameters {
			renderedValue, err := renderer.Render(value)
			if err != nil {
				return nil, fmt.Errorf("error rendering parameter %q: %w", key, err)
			}
			a.Parameters[key] = strings.TrimSpace(renderedValue)
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

	err := unmarshalWithIntegerPreservation([]byte(variable), &composite)
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
	valueStr := segments[1]

	var value any
	if err := unmarshalWithIntegerPreservation([]byte(valueStr), &value); err != nil {
		return nil, err
	}
	return map[string]any{key: value}, nil
}

// unmarshalWithIntegerPreservation unmarshals JSON while preserving integer types
// instead of converting all numbers to float64
func unmarshalWithIntegerPreservation(data []byte, v any) error {
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.UseNumber()

	err := decoder.Decode(v)
	if err != nil {
		return err
	}

	// Convert json.Number values to appropriate int or float types
	convertNumbers(v)
	return nil
}

// convertNumbers recursively converts json.Number values to int64 or float64
func convertNumbers(v any) {
	switch val := v.(type) {
	case *map[string]any:
		convertNumbersInMap(*val)
	case map[string]any:
		convertNumbersInMap(val)
	case *[]any:
		convertNumbersInSlice(*val)
	case []any:
		convertNumbersInSlice(val)
	case *any:
		convertNumbers(*val)
	}
}

func convertNumbersInMap(m map[string]any) {
	for key, value := range m {
		if num, ok := value.(json.Number); ok {
			if intVal, err := num.Int64(); err == nil {
				m[key] = intVal
			} else if floatVal, err := num.Float64(); err == nil {
				m[key] = floatVal
			}
		} else {
			convertNumbers(value)
			m[key] = value
		}
	}
}

func convertNumbersInSlice(s []any) {
	for i, value := range s {
		if num, ok := value.(json.Number); ok {
			if intVal, err := num.Int64(); err == nil {
				s[i] = intVal
			} else if floatVal, err := num.Float64(); err == nil {
				s[i] = floatVal
			}
		} else {
			convertNumbers(value)
		}
	}
}
