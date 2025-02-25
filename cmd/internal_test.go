package cmd

import (
	"testing"
	"time"
)

func BenchmarkInternalParsePipeline(b *testing.B) {
	r := ParseCommand{
		builder:      DefaultPipelineBuilder,
		errorPrinter: errorPrinter,
	}

	for range [10]int{} {
		b.ResetTimer()
		start := time.Now()
		if err := r.ParsePipeline("./testdata/lineage", true, false); err != nil {
			b.Fatalf("Failed to run Internal Parse Pipeline command: %v", err)
		}
		b.StopTimer()
		if time.Since(start) > 100*time.Millisecond {
			b.Fatalf("Benchmark took longer than 100ms")
		}
	}
}

func BenchmarkInternalParseAsset(b *testing.B) {
	r := ParseCommand{
		builder:      DefaultPipelineBuilder,
		errorPrinter: errorPrinter,
	}

	for range [10]int{} {
		b.ResetTimer()
		start := time.Now()
		if err := r.Run("./testdata/lineage/assets/hello_bq.sql", true); err != nil {
			b.Fatalf("Failed to run Internal Parse Pipeline command: %v", err)
		}
		b.StopTimer()
		if time.Since(start) > 100*time.Millisecond {
			b.Fatalf("Benchmark took longer than 100ms")
		}
	}
}

func BenchmarkInternalParsePipelineWithoutColumnLineage(b *testing.B) {
	r := ParseCommand{
		builder:      DefaultPipelineBuilder,
		errorPrinter: errorPrinter,
	}

	for range [10]int{} {
		b.ResetTimer()
		start := time.Now()
		if err := r.ParsePipeline("./testdata/lineage", false, false); err != nil {
			b.Fatalf("Failed to run Internal Parse Pipeline command: %v", err)
		}
		b.StopTimer()
		if time.Since(start) > 100*time.Millisecond {
			b.Fatalf("Benchmark took longer than 100ms")
		}
	}
}

func BenchmarkInternalParseAssetWithoutColumnLineage(b *testing.B) {
	r := ParseCommand{
		builder:      DefaultPipelineBuilder,
		errorPrinter: errorPrinter,
	}

	for range [10]int{} {
		b.ResetTimer()
		start := time.Now()
		if err := r.Run("./testdata/lineage/assets/hello_bq.sql", false); err != nil {
			b.Fatalf("Failed to run Internal Parse Pipeline command: %v", err)
		}
		b.StopTimer()
		if time.Since(start) > 100*time.Millisecond {
			b.Fatalf("Benchmark took longer than 100ms")
		}
	}
}
