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
			a.Parameters[key] = renderedValue
		}

		return a, nil
	}
}

func variableOverridesMutator(variables []string) pipeline.PipelineMutator {
	return func(ctx context.Context, p *pipeline.Pipeline) (*pipeline.Pipeline, error) {
		overrides := map[string]any{}
		
		// Try to reconstruct JSON that may have been split by comma separation
		reconstructedVars := reconstructSplitJSON(variables)
		
		for _, variable := range reconstructedVars {
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

func reconstructSplitJSON(variables []string) []string {
	if len(variables) <= 1 {
		return variables
	}
	
	var result []string
	var currentJSON strings.Builder
	var braceCount int
	var bracketCount int
	var inJSON bool
	var inKeyValueJSON bool
	var isFirstKeyValuePart bool
	
	for _, variable := range variables {
		variable = strings.TrimSpace(variable)
		
		// Check if this looks like the start of a JSON object
		if strings.HasPrefix(variable, "{") && !inJSON && !inKeyValueJSON {
			inJSON = true
			braceCount = 0
			currentJSON.Reset()
		}
		
		// Check if this looks like key=JSON pattern (e.g., "users=[\"mark\"")
		if strings.Contains(variable, "=") && (strings.Contains(variable, "[") || strings.Contains(variable, "{")) && !inJSON && !inKeyValueJSON {
			parts := strings.SplitN(variable, "=", 2)
			if len(parts) == 2 {
				valueStart := parts[1]
				if strings.HasPrefix(valueStart, "[") || strings.HasPrefix(valueStart, "{") {
					inKeyValueJSON = true
					braceCount = 0
					bracketCount = 0
					currentJSON.Reset()
					currentJSON.WriteString(variable)
					
					// Count initial brackets/braces
					for _, char := range valueStart {
						switch char {
						case '{':
							braceCount++
						case '}':
							braceCount--
						case '[':
							bracketCount++
						case ']':
							bracketCount--
						}
					}
					
					// Check if already complete
					if braceCount == 0 && bracketCount == 0 {
						result = append(result, currentJSON.String())
						inKeyValueJSON = false
						currentJSON.Reset()
					}
					// Mark that we've processed the first part
					isFirstKeyValuePart = false
					continue
				}
			}
		}
		
		if inJSON {
			if currentJSON.Len() > 0 {
				currentJSON.WriteString(",")
			}
			currentJSON.WriteString(variable)
			
			// Count braces to determine when JSON is complete
			for _, char := range variable {
				switch char {
				case '{':
					braceCount++
				case '}':
					braceCount--
				}
			}
			
			// If braces are balanced, JSON is complete
			if braceCount == 0 {
				result = append(result, currentJSON.String())
				inJSON = false
				currentJSON.Reset()
			}
		} else if inKeyValueJSON {
			if !isFirstKeyValuePart {
				// Add comma and space to properly separate JSON array elements
				currentJSON.WriteString(", ")
			}
			isFirstKeyValuePart = false
			currentJSON.WriteString(variable)
			
			// Count both braces and brackets to determine when JSON is complete
			for _, char := range variable {
				switch char {
				case '{':
					braceCount++
				case '}':
					braceCount--
				case '[':
					bracketCount++
				case ']':
					bracketCount--
				}
			}
			
			// If brackets and braces are balanced, JSON is complete
			if braceCount == 0 && bracketCount == 0 {
				result = append(result, currentJSON.String())
				inKeyValueJSON = false
				currentJSON.Reset()
			}
		} else {
			// Not JSON, add as-is
			result = append(result, variable)
		}
	}
	
	// If we're still in JSON mode, something went wrong
	if inJSON || inKeyValueJSON {
		// Return original variables as fallback
		return variables
	}
	
	return result
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
	valueStr := segments[1]
	
	var value any
	if err := json.Unmarshal([]byte(valueStr), &value); err != nil {
		return nil, err
	}
	return map[string]any{key: value}, nil
}
