package cmd

import (
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
		if err := r.ParsePipeline(b.Context(), "./testdata/lineage", true, false); err != nil {
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
		if err := r.Run(b.Context(), "./testdata/lineage/assets/hello_bq.sql", true); err != nil {
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
		if err := r.ParsePipeline(b.Context(), "./testdata/lineage", false, false); err != nil {
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
		if err := r.Run(b.Context(), "./testdata/lineage/assets/hello_bq.sql", false); err != nil {
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

func TestLockDependenciesCommand_FindRequirementsPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		inputPath      string
		wantErr        bool
		wantErrContain string
		wantReqSuffix  string
	}{
		{
			name:          "direct requirements.txt input",
			inputPath:     "./testdata/lineage/assets/nested/requirements.txt",
			wantErr:       false,
			wantReqSuffix: "requirements.txt",
		},
		{
			name:          "python asset input finds requirements.txt",
			inputPath:     "./testdata/lineage/assets/nested/hello_python.py",
			wantErr:       false,
			wantReqSuffix: "requirements.txt",
		},
		{
			name:           "sql asset returns error - not a python asset",
			inputPath:      "./testdata/lineage/assets/hello_bq.sql",
			wantErr:        true,
			wantErrContain: "asset is not a Python asset",
		},
		{
			name:           "non-existent file returns error",
			inputPath:      "./testdata/lineage/assets/nonexistent.py",
			wantErr:        true,
			wantErrContain: "failed to parse asset",
		},
		{
			name:           "random txt file returns error - not a valid asset",
			inputPath:      "./testdata/lineage/pipeline.yml",
			wantErr:        true,
			wantErrContain: "file is not a valid asset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cmd := &LockDependenciesCommand{
				builder: DefaultPipelineBuilder,
			}

			result, err := cmd.FindRequirementsPath(tt.inputPath)

			if tt.wantErr {
				if err == nil {
					t.Errorf("FindRequirementsPath() expected error, got nil")
					return
				}
				if tt.wantErrContain != "" && !strings.Contains(err.Error(), tt.wantErrContain) {
					t.Errorf("FindRequirementsPath() error = %v, want error containing %q", err, tt.wantErrContain)
				}
				return
			}

			if err != nil {
				t.Errorf("FindRequirementsPath() unexpected error: %v", err)
				return
			}

			if result == nil {
				t.Error("FindRequirementsPath() returned nil result")
				return
			}

			if tt.wantReqSuffix != "" && !strings.HasSuffix(result.RequirementsPath, tt.wantReqSuffix) {
				t.Errorf("FindRequirementsPath() RequirementsPath = %v, want suffix %v", result.RequirementsPath, tt.wantReqSuffix)
			}

			if result.InputPath == "" {
				t.Error("FindRequirementsPath() InputPath should not be empty")
			}
		})
	}
}

func TestLockDependenciesCommand_FindRequirementsPath_PythonAssetWithoutRequirements(t *testing.T) {
	t.Parallel()

	// Create a temp directory with a Python asset but no requirements.txt
	tmpDir := t.TempDir()

	// Initialize a git repo in the temp directory
	gitDir := filepath.Join(tmpDir, ".git")
	if err := os.Mkdir(gitDir, 0o755); err != nil {
		t.Fatalf("Failed to create .git directory: %v", err)
	}

	// Create a Python asset file
	pyContent := `"""@bruin
name: test_asset
@bruin"""

print("hello")
`
	pyPath := filepath.Join(tmpDir, "test_asset.py")
	if err := os.WriteFile(pyPath, []byte(pyContent), 0o644); err != nil {
		t.Fatalf("Failed to create Python file: %v", err)
	}

	cmd := &LockDependenciesCommand{
		builder: DefaultPipelineBuilder,
	}

	_, err := cmd.FindRequirementsPath(pyPath)
	if err == nil {
		t.Error("FindRequirementsPath() expected error for Python asset without requirements.txt, got nil")
		return
	}

	if !strings.Contains(err.Error(), "no requirements.txt found") {
		t.Errorf("FindRequirementsPath() error = %v, want error containing 'no requirements.txt found'", err)
	}
}
