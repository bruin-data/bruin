package cmd

import (
	"testing"
)

func BenchmarkInternalParsePipeline(b *testing.B) {
	r := ParseCommand{
		builder:      DefaultPipelineBuilder,
		errorPrinter: errorPrinter,
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < b.N; i++ {
			if err := r.ParsePipeline("./testdata/lineage", true); err != nil {
				b.Fatalf("Failed to run Internal Parse Pipeline command: %v", err)
			}
		}
	}
}

func BenchmarkInternalParseAsset(b *testing.B) {
	r := ParseCommand{
		builder:      DefaultPipelineBuilder,
		errorPrinter: errorPrinter,
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < b.N; i++ {
			if err := r.Run("./testdata/lineage/assets/hello_bq.sql", true); err != nil {
				b.Fatalf("Failed to run Internal Parse Pipeline command: %v", err)
			}
		}
	}
}

func BenchmarkInternalParsePipelineWithoutColumnLineage(b *testing.B) {
	r := ParseCommand{
		builder:      DefaultPipelineBuilder,
		errorPrinter: errorPrinter,
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < b.N; i++ {
			if err := r.ParsePipeline("./testdata/lineage", false); err != nil {
				b.Fatalf("Failed to run Internal Parse Pipeline command: %v", err)
			}
		}
	}
}

func BenchmarkInternalParseAssetWithoutColumnLineage(b *testing.B) {
	r := ParseCommand{
		builder:      DefaultPipelineBuilder,
		errorPrinter: errorPrinter,
	}

	for i := 0; i < b.N; i++ {
		for i := 0; i < b.N; i++ {
			if err := r.Run("./testdata/lineage/assets/hello_bq.sql", false); err != nil {
				b.Fatalf("Failed to run Internal Parse Pipeline command: %v", err)
			}
		}
	}
}
