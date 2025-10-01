package cmd

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
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

func TestAssetMetadataCommand(t *testing.T) {
	t.Parallel()
	cmd := AssetMetadata()

	assert.Equal(t, "asset-metadata", cmd.Name)
	assert.NotNil(t, cmd.Action)

	flagNames := make(map[string]bool)
	for _, flag := range cmd.Flags {
		if f, ok := flag.(*cli.StringFlag); ok {
			flagNames[f.Name] = true
		}
	}

	expectedFlags := []string{"environment", "config-file", "start-date", "end-date"}
	for _, expectedFlag := range expectedFlags {
		assert.True(t, flagNames[expectedFlag], "Expected flag %s to be present", expectedFlag)
	}

	startDateFlag := findStringFlag(cmd.Flags, "start-date")
	require.NotNil(t, startDateFlag, "start-date flag should exist")
	assert.Contains(t, startDateFlag.Usage, "start date of the range")
	assert.Contains(t, startDateFlag.Usage, "YYYY-MM-DD")
	assert.NotNil(t, startDateFlag.Sources, "start-date flag should have sources")

	endDateFlag := findStringFlag(cmd.Flags, "end-date")
	require.NotNil(t, endDateFlag, "end-date flag should exist")
	assert.Contains(t, endDateFlag.Usage, "end date of the range")
	assert.Contains(t, endDateFlag.Usage, "YYYY-MM-DD")
	assert.NotNil(t, endDateFlag.Sources, "end-date flag should have sources")
}

func TestAssetMetadataDefaultDates(t *testing.T) {
	t.Parallel()

	yesterday := time.Now().AddDate(0, 0, -1)
	expectedStartDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	expectedEndDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 0, time.UTC)

	assert.Equal(t, expectedStartDate, defaultStartDate, "Default start date should be beginning of yesterday")
	assert.Equal(t, expectedEndDate, defaultEndDate, "Default end date should be end of yesterday")

	cmd := AssetMetadata()
	startDateFlag := findStringFlag(cmd.Flags, "start-date")
	require.NotNil(t, startDateFlag)
	assert.Equal(t, expectedStartDate.Format("2006-01-02 15:04:05.000000"), startDateFlag.Value)

	endDateFlag := findStringFlag(cmd.Flags, "end-date")
	require.NotNil(t, endDateFlag)
	assert.Equal(t, expectedEndDate.Format("2006-01-02 15:04:05")+".999999", endDateFlag.Value)
}

func findStringFlag(flags []cli.Flag, name string) *cli.StringFlag {
	for _, flag := range flags {
		if stringFlag, ok := flag.(*cli.StringFlag); ok && stringFlag.Name == name {
			return stringFlag
		}
	}
	return nil
}
