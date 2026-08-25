package jinja

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestLoadMacros(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupFs       func(afero.Fs)
		macrosPath    string
		wantContains  []string
		wantErr       bool
		wantErrString string
	}{
		{
			name: "loads single macro file",
			setupFs: func(fs afero.Fs) {
				_ = fs.MkdirAll("/pipeline/macros", 0o755)
				_ = afero.WriteFile(fs, "/pipeline/macros/test.sql", []byte(`{% macro test() %}SELECT 1{% endmacro %}`), 0o644)
			},
			macrosPath:   "/pipeline/macros",
			wantContains: []string{"{% macro test() %}SELECT 1{% endmacro %}"},
			wantErr:      false,
		},
		{
			name: "loads multiple macro files",
			setupFs: func(fs afero.Fs) {
				_ = fs.MkdirAll("/pipeline/macros", 0o755)
				_ = afero.WriteFile(fs, "/pipeline/macros/macro1.sql", []byte(`{% macro m1() %}SELECT 1{% endmacro %}`), 0o644)
				_ = afero.WriteFile(fs, "/pipeline/macros/macro2.sql", []byte(`{% macro m2() %}SELECT 2{% endmacro %}`), 0o644)
			},
			macrosPath: "/pipeline/macros",
			wantContains: []string{
				"{% macro m1() %}SELECT 1{% endmacro %}",
				"{% macro m2() %}SELECT 2{% endmacro %}",
			},
			wantErr: false,
		},
		{
			name: "ignores non-sql files",
			setupFs: func(fs afero.Fs) {
				_ = fs.MkdirAll("/pipeline/macros", 0o755)
				_ = afero.WriteFile(fs, "/pipeline/macros/macro.sql", []byte(`{% macro test() %}SELECT 1{% endmacro %}`), 0o644)
				_ = afero.WriteFile(fs, "/pipeline/macros/readme.md", []byte("# Macros"), 0o644)
				_ = afero.WriteFile(fs, "/pipeline/macros/notes.txt", []byte("Some notes"), 0o644)
			},
			macrosPath: "/pipeline/macros",
			wantContains: []string{
				"{% macro test() %}SELECT 1{% endmacro %}",
			},
			wantErr: false,
		},
		{
			name: "returns empty string when directory doesn't exist",
			setupFs: func(fs afero.Fs) {
				// Don't create the directory
			},
			macrosPath:   "/pipeline/macros",
			wantContains: []string{},
			wantErr:      false,
		},
		{
			name: "handles empty macros directory",
			setupFs: func(fs afero.Fs) {
				_ = fs.MkdirAll("/pipeline/macros", 0o755)
			},
			macrosPath:   "/pipeline/macros",
			wantContains: []string{},
			wantErr:      false,
		},
		{
			name: "ignores subdirectories",
			setupFs: func(fs afero.Fs) {
				_ = fs.MkdirAll("/pipeline/macros/subdir", 0o755)
				_ = afero.WriteFile(fs, "/pipeline/macros/macro.sql", []byte(`{% macro test() %}SELECT 1{% endmacro %}`), 0o644)
				_ = afero.WriteFile(fs, "/pipeline/macros/subdir/ignored.sql", []byte(`{% macro ignored() %}SELECT 2{% endmacro %}`), 0o644)
			},
			macrosPath: "/pipeline/macros",
			wantContains: []string{
				"{% macro test() %}SELECT 1{% endmacro %}",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewMemMapFs()
			tt.setupFs(fs)

			result, err := LoadMacros(fs, tt.macrosPath)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrString != "" {
					require.Contains(t, err.Error(), tt.wantErrString)
				}
				return
			}

			require.NoError(t, err)
			for _, want := range tt.wantContains {
				require.Contains(t, result, want)
			}
		})
	}
}

func TestRendererWithMacros(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		macroContent string
		query        string
		context      Context
		want         string
		wantErr      bool
	}{
		{
			name: "renders query with simple macro",
			macroContent: `{% macro simple_select() %}
SELECT 1 as id, 'test' as name
{% endmacro %}`,
			query:   "{{ simple_select() }}",
			context: Context{},
			want:    "\n\nSELECT 1 as id, 'test' as name\n",
			wantErr: false,
		},
		{
			name: "renders query with parameterized macro",
			macroContent: `{% macro filter_by_id(id) %}
SELECT * FROM users WHERE id = {{ id }}
{% endmacro %}`,
			query:   "{{ filter_by_id(123) }}",
			context: Context{},
			want:    "\n\nSELECT * FROM users WHERE id = 123\n",
			wantErr: false,
		},
		{
			name: "renders query with multiple macros",
			macroContent: `{% macro m1() %}SELECT 1{% endmacro %}
{% macro m2() %}SELECT 2{% endmacro %}`,
			query:   "{{ m1() }} UNION ALL {{ m2() }}",
			context: Context{},
			want:    "\n\nSELECT 1 UNION ALL SELECT 2",
			wantErr: false,
		},
		{
			name: "combines macro with jinja variables",
			macroContent: `{% macro select_table(table_name) %}
SELECT * FROM {{ table_name }}
{% endmacro %}`,
			query: "{{ select_table(my_table) }}",
			context: Context{
				"my_table": "users",
			},
			want:    "\n\nSELECT * FROM users\n",
			wantErr: false,
		},
		{
			name:         "works without macros",
			macroContent: "",
			query:        "SELECT {{ column }} FROM table",
			context: Context{
				"column": "id",
			},
			want:    "SELECT id FROM table",
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			renderer := NewRendererWithMacros(tt.context, tt.macroContent)
			result, err := renderer.Render(tt.query)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, result)
		})
	}
}

func TestRendererCloneLoadsPipelineMacros(t *testing.T) {
	t.Parallel()

	startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	executionDate := endDate
	ctx := context.WithValue(t.Context(), pipeline.RunConfigStartDate, startDate)
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, endDate)
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, executionDate)
	ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run-id")

	pipe := &pipeline.Pipeline{
		Name: "test-pipeline",
		Macros: []pipeline.Macro{
			`{% macro test_macro(value) %}SELECT {{ value }}{% endmacro %}`,
		},
	}
	asset := &pipeline.Asset{Name: "test.asset"}
	renderer := NewRendererWithYesterday("test-pipeline", "test-run-id")

	clonedRenderer, err := renderer.CloneForAsset(ctx, pipe, asset)
	require.NoError(t, err)

	result, err := clonedRenderer.Render("{{ test_macro(1) }}")
	require.NoError(t, err)
	require.Equal(t, "SELECT 1", strings.TrimSpace(result))
}

func TestRendererClonePreservesExplicitMacros(t *testing.T) {
	t.Parallel()

	startDate := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	executionDate := endDate
	ctx := context.WithValue(t.Context(), pipeline.RunConfigStartDate, startDate)
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, endDate)
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, executionDate)
	ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test-run-id")

	macroContent := `{% macro test_macro() %}SELECT 1{% endmacro %}`
	renderer := NewRendererWithMacros(Context{}, macroContent)
	pipe := &pipeline.Pipeline{
		Name:   "test-pipeline",
		Macros: []pipeline.Macro{`{% macro test_macro() %}SELECT 2{% endmacro %}`},
	}
	asset := &pipeline.Asset{Name: "test.asset"}

	clonedRenderer, err := renderer.CloneForAsset(ctx, pipe, asset)
	require.NoError(t, err)

	result, err := clonedRenderer.Render("{{ test_macro() }}")
	require.NoError(t, err)
	require.Equal(t, "\nSELECT 1", result)
}
