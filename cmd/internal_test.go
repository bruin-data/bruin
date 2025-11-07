package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
)

func BenchmarkInternalParsePipeline(b *testing.B) {
	r := ParseCommand{
		builder:      DefaultPipelineBuilder,
		errorPrinter: errorPrinter,
	}

	for range [10]int{} {
		b.ResetTimer()
		start := time.Now()
		if err := r.ParsePipeline(context.Background(), "./testdata/lineage", true, false); err != nil {
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
		if err := r.Run(context.Background(), "./testdata/lineage/assets/hello_bq.sql", true); err != nil {
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
		if err := r.ParsePipeline(context.Background(), "./testdata/lineage", false, false); err != nil {
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
		if err := r.Run(context.Background(), "./testdata/lineage/assets/hello_bq.sql", false); err != nil {
			b.Fatalf("Failed to run Internal Parse Pipeline command: %v", err)
		}
		b.StopTimer()
		if time.Since(start) > 100*time.Millisecond {
			b.Fatalf("Benchmark took longer than 100ms")
		}
	}
}

func TestConvertToBruinAsset(t *testing.T) {
	t.Parallel()
	builder := DefaultPipelineBuilder

	tests := []struct {
		name        string
		fileContent string
		filePath    string
		wantErr     bool
		wantContent string
		wantAsset   bool
	}{
		{
			name:        "convert SQL file",
			fileContent: "SELECT * FROM table;",
			filePath:    "test.sql",
			wantErr:     false,
			wantContent: "/* @bruin\n\ntype: bq.sql\nmaterialization:\n  type: table\n\n@bruin */\n\nSELECT * FROM table;",
			wantAsset:   true,
		},
		{
			name:        "convert Python file",
			fileContent: "print('hello')",
			filePath:    "test.py",
			wantErr:     false,
			wantContent: "\"\"\"@bruin\ndescription: this is a python asset\n@bruin\"\"\"\n\nprint('hello')",
			wantAsset:   true,
		},
		{
			name:        "unsupported file type",
			fileContent: "some content",
			filePath:    "test.txt",
			wantErr:     false, // function returns nil for unsupported types
			wantContent: "some content",
			wantAsset:   false,
		},
		{
			name:        "file with spaces in name",
			fileContent: "SELECT * FROM table;",
			filePath:    "my test.sql",
			wantErr:     false,
			wantContent: "/* @bruin\n\ntype: bq.sql\nmaterialization:\n  type: table\n\n@bruin */\n\nSELECT * FROM table;",
			wantAsset:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()

			fullPath := filepath.Join(tmpDir, tt.filePath)
			err := os.WriteFile(fullPath, []byte(tt.fileContent), 0o644)
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}
			fs := afero.NewOsFs()
			err = convertToBruinAsset(fs, fullPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("convertToBruinAsset() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr { //nolint
				content, err := os.ReadFile(fullPath)
				if err != nil {
					t.Fatalf("Failed to read result file: %v", err)
				}
				if got := string(content); got != tt.wantContent {
					// Loose check
					if normalize(got) != normalize(tt.wantContent) {
						t.Errorf("check failed: got = %q, want = %q", normalize(got), normalize(tt.wantContent))
					}
				}

				// Verify the file can be parsed as an asset
				asset, err := builder.CreateAssetFromFile(fullPath, nil)
				if tt.wantAsset {
					if err != nil {
						t.Errorf("Failed to create asset from converted file: %v", err)
					}
					if asset == nil {
						t.Error("Expected asset to be created, but got nil")
					}

					// Check materialization for SQL files
					if strings.HasSuffix(tt.filePath, ".sql") {
						if asset.Materialization.Type != "table" {
							t.Errorf("Expected materialization type 'table', got '%s'", asset.Materialization.Type)
						}
					}

					// Check no materialization for Python files
					if strings.HasSuffix(tt.filePath, ".py") {
						if asset.Materialization.Type != "" {
							t.Errorf("Expected no materialization for Python file, got '%s'", asset.Materialization.Type)
						}
					}
				} else { //nolint
					if asset != nil {
						t.Error("Expected no asset to be created, but got one")
					}
				}
			}
		})
	}
}

func normalize(s string) string {
	lines := strings.Split(s, "\n")
	var out []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return strings.Join(out, "\n")
}
