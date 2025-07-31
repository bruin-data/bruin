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
			wantContent: "/* @bruin\n\ntype: bq.sql\n\n@bruin */\n\nSELECT * FROM table;",
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
			wantContent: "/* @bruin\n\ntype: bq.sql\n\n@bruin */\n\nSELECT * FROM table;",
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

// func TestDryRunCommandRun(t *testing.T) {
// 	t.Parallel()

// 	tests := []struct {
// 		name           string
// 		assetPath      string
// 		assetContent   string
// 		connectionName string
// 		environment    string
// 		configFile     string
// 		wantErr        bool
// 		errorContains  string
// 	}{
// 		{
// 			name:           "empty asset path",
// 			assetPath:      "",
// 			connectionName: "gcp-default",
// 			wantErr:        true,
// 			errorContains:  "Please give an asset path to parse: bruin internal dry-run-asset <path to the asset file>)\n",
// 		},
// 		{
// 			name:           "non-existent file",
// 			assetPath:      "/non/existent/file.sql",
// 			connectionName: "gcp-default",
// 			wantErr:        true,
// 		},
// 		{
// 			name:           "non-SQL asset",
// 			assetPath:      "test.py",
// 			assetContent:   "print('hello world')",
// 			connectionName: "gcp-default",
// 			wantErr:        true,
// 			errorContains:  "dry run is only supported for SQL assets",
// 		},
// 		{
// 			name:           "SQL asset with no query content",
// 			assetPath:      "empty.sql",
// 			assetContent:   "/* @bruin\ntype: bq.sql\n@bruin */\n\n",
// 			connectionName: "gcp-default",
// 			wantErr:        true,
// 			errorContains:  "no SQL query found in asset",
// 		},
// 		{
// 			name:           "SQL asset with only comments",
// 			assetPath:      "comments.sql",
// 			assetContent:   "/* @bruin\ntype: bq.sql\n@bruin */\n\n-- Just comments\n/* No actual query */",
// 			connectionName: "gcp-default",
// 			wantErr:        true,
// 			errorContains:  "no SQL query found in asset",
// 		},
// 		{
// 			name:           "valid SQL asset with default connection",
// 			assetPath:      "test.sql",
// 			assetContent:   "/* @bruin\ntype: bq.sql\n@bruin */\n\nSELECT * FROM users;",
// 			connectionName: "gcp-default",
// 			wantErr:        false,
// 		},
// 		{
// 			name:           "valid SQL asset with custom connection",
// 			assetPath:      "analytics.sql",
// 			assetContent:   "/* @bruin\ntype: bq.sql\n@bruin */\n\nSELECT COUNT(*) FROM events;",
// 			connectionName: "my-bigquery-conn",
// 			environment:    "production",
// 			configFile:     "/path/to/config.yml",
// 			wantErr:        false,
// 		},
// 		{
// 			name:           "complex SQL query",
// 			assetPath:      "complex.sql",
// 			assetContent:   "/* @bruin\ntype: bq.sql\n@bruin */\n\nWITH active_users AS (\n  SELECT id, name FROM users WHERE active = true\n)\nSELECT u.id, u.name, p.status\nFROM active_users u\nLEFT JOIN payments p ON u.id = p.user_id\nWHERE p.amount > 100;",
// 			connectionName: "gcp-default",
// 			wantErr:        false,
// 		},
// 		{
// 			name:           "SQL with multiple statements",
// 			assetPath:      "multi.sql",
// 			assetContent:   "/* @bruin\ntype: bq.sql\n@bruin */\n\nSELECT * FROM users; SELECT * FROM payments;",
// 			connectionName: "gcp-default",
// 			wantErr:        false,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			t.Parallel()

// 			// Simple file creation if content provided
// 			var fullPath string
// 			if tt.assetContent != "" {
// 				tmpDir := t.TempDir()
// 				fullPath = filepath.Join(tmpDir, tt.assetPath)
// 				err := os.WriteFile(fullPath, []byte(tt.assetContent), 0o644)
// 				require.NoError(t, err)
// 			} else {
// 				fullPath = tt.assetPath
// 			}

// 			// Create command instance
// 			cmd := DryRunCommand{
// 				builder:      DefaultPipelineBuilder,
// 				errorPrinter: errorPrinter,
// 			}

// 			// Test the function
// 			err := cmd.Run(context.Background(), fullPath, tt.connectionName, tt.environment, tt.configFile)

// 			// Simple assertions
// 			if tt.wantErr {
// 				require.Error(t, err)
// 				if tt.errorContains != "" {
// 					assert.Contains(t, err.Error(), tt.errorContains)
// 				}
// 			} else {
// 				require.NoError(t, err)
// 			}
// 		})
// 	}
// }




