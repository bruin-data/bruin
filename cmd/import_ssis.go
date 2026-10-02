package cmd

import (
	"context"
	"fmt"

	"github.com/bruin-data/bruin/pkg/ssis"
	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func ImportSSIS() *cli.Command {
	return &cli.Command{
		Name: "ssis", Usage: "Import SSIS packages or SQL Server Agent exports as SQL and Python assets",
		ArgsUsage: "[.dtsx file, Agent .json export, or directory] [pipeline path]",
		Description: "Import direct-input Execute SQL tasks and standalone Python process tasks. " +
			"Success dependencies are preserved. Unsupported tasks and control flow fail before writing. " +
			"SQL containing sp_execute_external_script remains SQL; its Python still runs in SQL Server. " +
			"Configure connections and Python requirements and review generated assets before running.",
		Before: telemetry.BeforeCommand,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "connection", Aliases: []string{"c"}, Usage: "Bruin MSSQL connection for imported SQL assets", Required: true},
			&cli.BoolFlag{Name: "overwrite", Usage: "overwrite generated asset files (never pipeline.yml)"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if c.Args().Len() != 2 {
				return cli.Exit("source and pipeline paths are required; see bruin import ssis --help", 1)
			}
			result, err := ssis.Import(ctx, afero.NewOsFs(), ssis.ImportOptions{
				SourcePath: c.Args().Get(0), PipelinePath: c.Args().Get(1),
				Connection: c.String("connection"), Overwrite: c.Bool("overwrite"),
			})
			if err != nil {
				return fmt.Errorf("failed to import SSIS: %w", err)
			}
			fmt.Printf("Imported %d SQL and %d Python assets into %s; skipped %d existing assets.\n",
				result.SQLAssets, result.PythonAssets, c.Args().Get(1), result.SkippedAssets)
			fmt.Println("Review database contexts, credentials in source code, and Python dependencies before running. Schedules and execution identities are not migrated.")
			return nil
		},
	}
}
