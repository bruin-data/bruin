package jinja

import (
	"context"
	"fmt"
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
			want:    "\nSELECT 1 as id, 'test' as name\n",
			wantErr: false,
		},
		{
			name: "renders query with parameterized macro",
			macroContent: `{% macro filter_by_id(id) %}
SELECT * FROM users WHERE id = {{ id }}
{% endmacro %}`,
			query:   "{{ filter_by_id(123) }}",
			context: Context{},
			want:    "\nSELECT * FROM users WHERE id = 123\n",
			wantErr: false,
		},
		{
			name: "renders query with multiple macros",
			macroContent: `{% macro m1() %}SELECT 1{% endmacro %}
{% macro m2() %}SELECT 2{% endmacro %}`,
			query:   "{{ m1() }} UNION ALL {{ m2() }}",
			context: Context{},
			want:    "SELECT 1 UNION ALL SELECT 2",
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
			want:    "\nSELECT * FROM users\n",
			wantErr: false,
		},
		{
			name:         "macro preamble does not affect scalar output",
			macroContent: `{% macro unused() %}SELECT 1{% endmacro %}`,
			query:        "{{ value }}",
			context: Context{
				"value": "data.csv",
			},
			want:    "data.csv",
			wantErr: false,
		},
		{
			name:         "preserves intentional blank lines in query output",
			macroContent: `{% macro unused() %}SELECT 1{% endmacro %}`,
			query:        "SELECT 1\n\n\n\nSELECT 2",
			context:      Context{},
			want:         "SELECT 1\n\n\n\nSELECT 2",
			wantErr:      false,
		},
		{
			name:         "renders an empty query",
			macroContent: `{% macro unused() %}SELECT 1{% endmacro %}`,
			query:        "",
			context:      Context{},
			want:         "",
			wantErr:      false,
		},
		{
			name:         "avoids output marker collisions in the macro output",
			macroContent: "{% macro unused() %}SELECT 1{% endmacro %}\n" + macroOutputBoundary + "\n",
			query:        "SELECT 1",
			context:      Context{},
			want:         "SELECT 1",
			wantErr:      false,
		},
		{
			name:         "avoids output marker collisions straddling the macro content",
			macroContent: "{% macro unused() %}SELECT 1{% endmacro %}\n" + strings.TrimSuffix(macroOutputBoundary, "__"),
			query:        "SELECT 1",
			context:      Context{},
			want:         "SELECT 1",
			wantErr:      false,
		},
		{
			name:         "avoids output marker collisions in the query",
			macroContent: `{% macro unused() %}SELECT 1{% endmacro %}`,
			query:        macroOutputBoundary,
			context:      Context{},
			want:         macroOutputBoundary,
			wantErr:      false,
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
	require.Equal(t, "SELECT 1", result)
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
	require.Equal(t, "SELECT 1", result)
}

func TestRendererWithMacrosDoesNotLeakOutputMarkerInErrors(t *testing.T) {
	t.Parallel()

	_, err := NewRendererWithMacros(Context{}, "{% macro unterminated() %}SELECT 1\n").Render("SELECT 1")
	require.Error(t, err)
	require.NotContains(t, err.Error(), macroOutputBoundary)
}

func TestRendererWithMacrosReportsQueryRelativeParserErrors(t *testing.T) {
	t.Parallel()

	macroContent := "{% macro unused() %}SELECT 1{% endmacro %}\n"
	queries := []string{
		"{% if true %}SELECT 1",
		"{% for item in [1] %}{{ item }}",
		"\n\n{% if true %}SELECT 1",
	}

	for _, query := range queries {
		t.Run(fmt.Sprintf("%q", query), func(t *testing.T) {
			t.Parallel()

			_, expectedErr := NewRenderer(Context{}).Render(query)
			require.Error(t, expectedErr)

			_, err := NewRendererWithMacros(Context{}, macroContent).Render(query)
			require.EqualError(t, err, expectedErr.Error())
			require.NotContains(t, err.Error(), macroOutputBoundary)
		})
	}
}

func TestRendererWithMacrosMatchesRenderingWithoutMacros(t *testing.T) {
	t.Parallel()

	macroContents := map[string]string{
		"trailing newline":            "{% macro unused() %}SELECT 1{% endmacro %}\n",
		"no trailing newline":         "{% macro unused() %}SELECT 1{% endmacro %}",
		"trailing whitespace control": "{% macro unused() %}SELECT 1{% endmacro -%}\n",
	}

	queries := []string{
		"",
		" ",
		"\n",
		"\n\n",
		"SELECT 1",
		"SELECT 1\n",
		"SELECT 1\n\n",
		"  SELECT 1  ",
		"SELECT 1\nUNION ALL\nSELECT 2\n",
		"{# comment #}",
		"{{ '' }}",
		"{% if false %}x{% endif %}",
		"SELECT 1 {% if true %}WHERE 1=1{% endif %}",
		"{%- if true %}SELECT 1{% endif %}",
		"{{- 'SELECT 1' }}",
		"  {%- if true %}SELECT 1{% endif %}",
		"\n{%- if true %}\nSELECT 1{% endif %}",
		"{%- set t = 'users' %}SELECT * FROM {{ t }}",
		"{%- set t = 'users' -%}\nSELECT * FROM {{ t }}",
		"{#- comment -#}SELECT 1",
	}

	for name, macroContent := range macroContents {
		for _, query := range queries {
			t.Run(fmt.Sprintf("%s/%q", name, query), func(t *testing.T) {
				t.Parallel()

				want, err := NewRenderer(Context{}).Render(query)
				require.NoError(t, err)

				got, err := NewRendererWithMacros(Context{}, macroContent).Render(query)
				require.NoError(t, err)

				// the macro preamble is an implementation detail, so it must not change a
				// single byte of what the query itself renders to.
				require.Equal(t, want, got)
			})
		}
	}
}
