package cmd

import (
	"testing"

	"github.com/urfave/cli/v2"
)

func BenchmarkInternalParsePipeline(b *testing.B) {
	app := cli.NewApp()
	parser := ParsePipeline()
	parser.Flags[0] = &cli.BoolFlag{
		Name:        "column-lineage",
		Aliases:     []string{"c"},
		Usage:       "return the column lineage for the given asset",
		Required:    true,
		DefaultText: "false",
	}

	app.Commands = []*cli.Command{
		parser,
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < b.N; i++ {
			if err := app.Run([]string{"cmd.test", "parse-pipeline", "./testdata/lineage"}); err != nil {
				b.Fatalf("Failed to run Lint command: %v", err)
			}
		}
	}
}

func BenchmarkInternalParseAsset(b *testing.B) {
	app := cli.NewApp()
	parser := ParseAsset()
	parser.Flags[0] = &cli.BoolFlag{
		Name:        "column-lineage",
		Aliases:     []string{"c"},
		Usage:       "return the column lineage for the given asset",
		Required:    true,
		DefaultText: "false",
	}
	app.Commands = []*cli.Command{
		parser,
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < b.N; i++ {
			if err := app.Run([]string{"cmd.test", "parse-pipeline", "./testdata/lineage"}); err != nil {
				b.Fatalf("Failed to run Lint command: %v", err)
			}
		}
	}
}

func BenchmarkInternalParsePipelineWithoutColumnLineage(b *testing.B) {
	app := cli.NewApp()
	parser := ParsePipeline()
	parser.Flags[0] = &cli.BoolFlag{
		Name:        "column-lineage",
		Aliases:     []string{"c"},
		Usage:       "return the column lineage for the given asset",
		Required:    false,
		DefaultText: "false",
	}
	app.Commands = []*cli.Command{
		parser,
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < b.N; i++ {
			if err := app.Run([]string{"cmd.test", "parse-pipeline", "./testdata/lineage"}); err != nil {
				b.Fatalf("Failed to run Lint command: %v", err)
			}
		}
	}
}

func BenchmarkInternalParseAssetWithoutColumnLineage(b *testing.B) {
	app := cli.NewApp()
	parser := ParseAsset()
	parser.Flags[0] = &cli.BoolFlag{
		Name:        "column-lineage",
		Aliases:     []string{"c"},
		Usage:       "return the column lineage for the given asset",
		Required:    false,
		DefaultText: "false",
	}
	app.Commands = []*cli.Command{
		parser,
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < b.N; i++ {
			if err := app.Run([]string{"cmd.test", "parse-pipeline", "./testdata/lineage"}); err != nil {
				b.Fatalf("Failed to run Lint command: %v", err)
			}
		}
	}
}
