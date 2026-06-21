package cmd

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/odi"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func ImportODI() *cli.Command {
	return &cli.Command{
		Name:      "odi",
		Usage:     "Import Oracle Data Integrator XML exports as Bruin assets",
		ArgsUsage: "[ODI XML file or directory] [pipeline path]",
		Description: `Import Oracle Data Integrator (ODI/Sunopsis) XML exports into a Bruin pipeline.

The importer reads scenario XML files, logical schema XML files, and generated ODI SQL text.
It creates Oracle SQL assets for executable ODI scenario steps, Oracle source assets for
referenced upstream tables, empty control assets for resolved scenario calls, ODI variable macros,
and a pipeline.yml file when the target pipeline does not exist.`,
		Before: telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "connection",
				Aliases: []string{"c"},
				Usage:   "Oracle connection name to set on imported assets",
			},
			&cli.BoolFlag{
				Name:  "overwrite",
				Usage: "overwrite existing generated asset files",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			ctx = query.WithQueryType(ctx, query.QueryTypeImport)
			sourcePath := c.Args().Get(0)
			if sourcePath == "" {
				return cli.Exit("ODI XML file or directory is required", 1)
			}

			pipelinePath := c.Args().Get(1)
			if pipelinePath == "" {
				return cli.Exit("pipeline path is required", 1)
			}

			result, err := odi.Import(ctx, afero.NewOsFs(), odi.ImportOptions{
				SourcePath:   sourcePath,
				PipelinePath: pipelinePath,
				Connection:   c.String("connection"),
				Overwrite:    c.Bool("overwrite"),
			})
			if err != nil {
				errorPrinter.Printf("Failed to import ODI files: %s\n", err)
				return cli.Exit("", 1)
			}

			printODIImportResult(pipelinePath, result)
			return nil
		},
	}
}

func printODIImportResult(pipelinePath string, result *odi.ImportResult) {
	fmt.Printf("Imported ODI project into pipeline '%s'\n", pipelinePath)
	fmt.Printf("  XML files scanned: %d\n", result.XMLFiles)
	fmt.Printf("  Scenarios imported: %d\n", result.Scenarios)
	fmt.Printf("  SQL assets written: %d\n", result.SQLAssets)
	fmt.Printf("  Source assets written: %d\n", result.SourceAssets)
	if result.ControlAssets > 0 {
		fmt.Printf("  Control assets written: %d\n", result.ControlAssets)
	}
	if result.ScenarioCallsResolved > 0 {
		fmt.Printf("  Scenario calls resolved: %d\n", result.ScenarioCallsResolved)
	}
	if result.VariableMacros > 0 {
		fmt.Printf("  ODI variable macros: %d\n", result.VariableMacros)
	}
	if result.SkippedAssets > 0 {
		fmt.Printf("  Existing assets skipped: %d\n", result.SkippedAssets)
	}
	if result.PipelineCreated {
		fmt.Println("  Created pipeline.yml")
	}
	if result.VariableMacrosWritten {
		fmt.Printf("  Created variable macros: %s\n", result.VariableMacrosPath)
	} else if result.VariableMacrosSkipped {
		fmt.Printf("  Existing variable macros skipped: %s\n", result.VariableMacrosPath)
	}
	if len(result.ControlFlowWarnings) > 0 {
		fmt.Printf("  Control-flow notes: %d\n", len(result.ControlFlowWarnings))
		if result.ControlFlowReportWritten {
			fmt.Printf("  Created control-flow report: %s\n", result.ControlFlowReportPath)
		} else if result.ControlFlowReportSkipped {
			fmt.Printf("  Existing control-flow report skipped: %s\n", result.ControlFlowReportPath)
		}
		for _, warning := range controlFlowWarningsPreview(result.ControlFlowWarnings) {
			fmt.Printf("    - %s step %d (%s): %s\n", warning.Scenario, warning.StepNumber, warning.Kind, warning.Message)
		}
		if len(result.ControlFlowWarnings) > 5 {
			fmt.Printf("    ... %d more warnings in report\n", len(result.ControlFlowWarnings)-5)
		}
	}

	if len(result.LogicalSchemaMapping) == 0 {
		return
	}

	keys := make([]string, 0, len(result.LogicalSchemaMapping))
	for key := range result.LogicalSchemaMapping {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var pairs []string
	for _, key := range keys {
		pairs = append(pairs, fmt.Sprintf("%s=%s", key, result.LogicalSchemaMapping[key]))
	}
	fmt.Printf("  Logical schemas: %s\n", strings.Join(pairs, ", "))
}

func controlFlowWarningsPreview(warnings []odi.ControlFlowWarning) []odi.ControlFlowWarning {
	if len(warnings) <= 5 {
		return warnings
	}
	return warnings[:5]
}
