package cmd

import (
	"context"
	"testing"

	"github.com/urfave/cli/v2"
)

func BenchmarkInternalParsePipeline(b *testing.B) {
	app := cli.NewApp()
	parsePipeline := ParsePipeline()
	parsePipeline.Flags[0] = &cli.BoolFlag{
		Name:        "column-lineage",
		Aliases:     []string{"c"},
		Usage:       "return the column lineage for the given asset",
		Required:    true,
		DefaultText: "false",
	}
	app.Commands = []*cli.Command{
		parsePipeline,
	}

	app.Run([]string{"parse-pipeline"})
	// Run the benchmark
	for i := 0; i < b.N; i++ {
		err := app.RunContext(context.Background(), []string{
			"/Users/yuvraj/Workspace/bruin/example/simple-pipeline",
		})
		if err != nil {
			b.Fatalf("Failed to run ParsePipeline command: %v", err)
		}
	}
}
