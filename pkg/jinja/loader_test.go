package jinja

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
)

func TestRepositoryTemplates(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "parts"), 0o755))
	for name, source := range map[string]string{
		"shared.sql":       "shared-value",
		"parts/value.sql":  "repo-value",
		"parts/nested.sql": `{% include 'value.sql' %}`,
		"parts/parent.sql": `{% include '../shared.sql' %}`,
		"parts/macros.sql": `{% macro value() %}macro-value{% endmacro %}`,
		"parts/base.sql":   "before:{% block body %}base{% endblock %}:after",
	} {
		require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte(source), 0o600))
	}
	for source, expected := range map[string]string{
		`{% include 'parts/parent.sql' %}`:                                  "shared-value",
		`{% include 'parts/value.sql' %}`:                                   "repo-value",
		`{% include 'parts/nested.sql' %}`:                                  "repo-value",
		`{% import 'parts/macros.sql' as m %}{{ m.value() }}`:               "macro-value",
		`{% from 'parts/macros.sql' import value %}{{ value() }}`:           "macro-value",
		`{% extends 'parts/base.sql' %}{% block body %}child{% endblock %}`: "before:child:after",
	} {
		t.Run(source, func(t *testing.T) {
			t.Parallel()
			r := NewRenderer(Context{}).WithRepositoryRoot(root)
			actual, err := r.Render(source)
			require.NoError(t, err)
			require.Equal(t, expected, actual)
		})
	}
}

func TestCloneForAssetDoesNotRetainPreviousRepository(t *testing.T) {
	t.Parallel()
	var renderer RendererInterface = NewRenderer(Context{})
	for _, value := range []string{"first-repo", "second-repo"} {
		root := t.TempDir()
		require.NoError(t, os.Mkdir(filepath.Join(root, ".git"), 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(root, "value.sql"), []byte(value), 0o600))
		pipe := &pipeline.Pipeline{Name: value}
		pipe.DefinitionFile.Path = filepath.Join(root, "pipeline.yml")
		var err error
		renderer, err = renderer.CloneForAsset(t.Context(), pipe, &pipeline.Asset{})
		require.NoError(t, err)
		out, err := renderer.Render(`{% include 'value.sql' %}`)
		require.NoError(t, err)
		require.Equal(t, value, out)
	}
	renderer, err := renderer.CloneForAsset(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{})
	require.NoError(t, err)
	out, err := renderer.Render(`{% include 'value.sql' %}`)
	require.Error(t, err)
	require.Empty(t, out)
}

func TestTemplateFileAccessDenied(t *testing.T) {
	t.Parallel()
	parent := t.TempDir()
	root := filepath.Join(parent, "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(root, "parts"), 0o755))
	secret := filepath.Join(parent, "secret.sql")
	require.NoError(t, os.WriteFile(secret, []byte("private-sentinel"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "local.sql"), []byte("local-value"), 0o600))
	require.NoError(t, os.Symlink(secret, filepath.Join(root, "linked.sql")))
	require.NoError(t, os.Symlink(parent, filepath.Join(root, "linked-dir")))
	require.NoError(t, os.Symlink("local.sql", filepath.Join(root, "internal-link.sql")))
	require.NoError(t, os.Symlink("parts", filepath.Join(root, "internal-dir")))
	require.NoError(t, os.WriteFile(filepath.Join(root, "parts", "nested.sql"), []byte(`{% include '../../secret.sql' %}`), 0o600))
	paths := []string{secret, "/proc/self/environ", "../secret.sql", "parts/../../secret.sql", "linked.sql", "linked-dir/secret.sql", "internal-link.sql", "internal-dir/nested.sql"}
	for _, path := range paths {
		for _, directive := range []string{
			`{%% include %q %%}`, `{%% import %q as m %%}`, `{%% from %q import value %%}`, `{%% extends %q %%}`,
		} {
			source := fmt.Sprintf(directive, path)
			t.Run(source, func(t *testing.T) {
				t.Parallel()
				out, err := NewRenderer(Context{}).WithRepositoryRoot(root).Render(source)
				require.Error(t, err)
				require.Empty(t, out)
				require.NotContains(t, err.Error(), "private-sentinel")
			})
		}
	}
	out, err := NewRenderer(Context{}).WithRepositoryRoot(root).Render(`{% include 'parts/nested.sql' %}`)
	require.Error(t, err)
	require.Empty(t, out)
	rootLink := filepath.Join(parent, "repo-link")
	require.NoError(t, os.Symlink(root, rootLink))
	out, err = NewRenderer(Context{}).WithRepositoryRoot(rootLink).Render(`{% include 'local.sql' %}`)
	require.Error(t, err)
	require.Empty(t, out)
	out, err = NewRenderer(Context{}).WithRepositoryRoot(rootLink).Render(`SELECT {{ 2 + 3 }}`)
	require.NoError(t, err)
	require.Equal(t, "SELECT 5", out)
	for name, render := range map[string]pipeline.RenderFunc{"default": NewRenderer(Context{}).Render, "variant": VariantRendererFactory(nil, "test")} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			out, err := render(fmt.Sprintf("{%% include %q %%}", secret))
			require.Error(t, err)
			require.Empty(t, out)
			out, err = render("{{ 2 + 3 }}")
			require.NoError(t, err)
			require.Equal(t, "5", out)
		})
	}
	pipe := &pipeline.Pipeline{
		Name:     fmt.Sprintf("{%% include %q %%}", secret),
		Variants: pipeline.VariantSet{"test": {}},
	}
	require.Error(t, pipe.MaterializeVariant("test", VariantRendererFactory))
	require.NotContains(t, pipe.Name, "private-sentinel")
}

func TestCloneForAssetRepositoryScope(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, ".git"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(root, "value.sql"), []byte("repo-value"), 0o600))
	pipe := &pipeline.Pipeline{Name: "test"}
	pipe.DefinitionFile.Path = filepath.Join(root, "pipelines", "pipeline.yml")
	ctx := context.Background()
	now := time.Now()
	contexts := [4]context.Context{ctx}
	ctx = context.WithValue(ctx, pipeline.RunConfigStartDate, now)
	contexts[1] = ctx
	ctx = context.WithValue(ctx, pipeline.RunConfigEndDate, now)
	contexts[2] = ctx
	ctx = context.WithValue(ctx, pipeline.RunConfigExecutionDate, now)
	ctx = context.WithValue(ctx, pipeline.RunConfigRunID, "test")
	contexts[3] = ctx
	for dates, ctx := range contexts {
		t.Run(fmt.Sprintf("run_dates=%d", dates), func(t *testing.T) {
			t.Parallel()
			base := NewRenderer(Context{"start_date": "existing-date"})
			r, err := base.CloneForAsset(ctx, pipe, &pipeline.Asset{})
			require.NoError(t, err)
			out, err := r.Render(`{% include 'value.sql' %}:{{ start_date }}`)
			require.NoError(t, err)
			wantDate := "existing-date"
			if dates == 3 {
				wantDate = now.Format("2006-01-02")
			}
			require.Equal(t, "repo-value:"+wantDate, out)
			// Scoping the clone must not grant filesystem access to its parent.
			out, err = base.Render(`{% include 'value.sql' %}`)
			require.Error(t, err)
			require.Empty(t, out)
		})
	}
}
