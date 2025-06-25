package python

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsePyprojectToml(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tomlContent string
		expected    map[string]any
		wantErr     bool
	}{
		{
			name: "valid_pyproject_toml_with_sqlfluff_config",
			tomlContent: `[tool.sqlfluff.core]
dialect = "duckdb"
max_line_length = 120
exclude_rules = ["LT05", "ST06"]

[tool.sqlfluff.indentation]
indented_joins = false
indented_using_on = true`,
			expected: map[string]any{
				"core": map[string]any{
					"dialect":         "duckdb",
					"max_line_length": int64(120),
					"exclude_rules":   []any{"LT05", "ST06"},
				},
				"indentation": map[string]any{
					"indented_joins":    false,
					"indented_using_on": true,
				},
			},
			wantErr: false,
		},
		{
			name:        "empty_pyproject_toml",
			tomlContent: "",
			expected:    nil,
			wantErr:     false,
		},
		{
			name: "pyproject_toml_without_sqlfluff_section",
			tomlContent: `[tool.other]
value = "test"`,
			expected: nil,
			wantErr:  false,
		},
		{
			name:        "non_existent_file",
			tomlContent: "",
			expected:    nil,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			testTempDir := t.TempDir()

			var filePath string
			if tt.name == "non_existent_file" {
				filePath = filepath.Join(testTempDir, "nonexistent.toml")
				// Don't create the file for this test case
			} else {
				filePath = filepath.Join(testTempDir, "test.toml")
				err := os.WriteFile(filePath, []byte(tt.tomlContent), 0o644)
				require.NoError(t, err)
			}

			result, err := parsePyprojectToml(filePath)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestConvertTomlConfigToIni(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		config   map[string]any
		prefix   string
		expected string
	}{
		{
			name: "simple_config_values",
			config: map[string]any{
				"core": map[string]any{
					"dialect":         "duckdb",
					"max_line_length": int64(120),
					"exclude_rules":   []any{"LT05", "ST06"},
				},
			},
			prefix:   "",
			expected: "[sqlfluff:core]\ndialect = duckdb\nexclude_rules = LT05,ST06\nmax_line_length = 120\n\n",
		},
		{
			name: "nested_config_with_boolean",
			config: map[string]any{
				"indentation": map[string]any{
					"indented_joins":    false,
					"indented_using_on": true,
				},
			},
			prefix:   "",
			expected: "[sqlfluff:indentation]\nindented_joins = false\nindented_using_on = true\n\n",
		},
		{
			name:     "empty_config",
			config:   map[string]any{},
			prefix:   "",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := convertTomlConfigToIni(tt.config, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}
