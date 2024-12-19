package cmd

import (
	"context"
	"testing"

	"github.com/urfave/cli/v2"
)

func BenchmarkInternalParsePipeline(b *testing.B) {
	app := cli.NewApp()
	app.Commands = []*cli.Command{
		ParsePipeline(),
	}

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		err := app.RunContext(context.Background(), []string{"internal", "parse-pipeline", "-c", "./testdata/lineage"})
		if err != nil {
			b.Fatalf("Failed to run Lint command: %v", err)
		}
	}
}
