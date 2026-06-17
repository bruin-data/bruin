package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestResolveAPIKey_FlagPriority(t *testing.T) {
	t.Setenv("BRUIN_CLOUD_API_KEY", "env-key")

	app := &cli.Command{
		Name: "test",
		Flags: []cli.Flag{
			apiKeyFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			key, err := resolveAPIKey(c)
			require.NoError(t, err)
			assert.Equal(t, "flag-key", key)
			return nil
		},
	}

	err := app.Run(t.Context(), []string{"test", "--api-key", "flag-key"})
	require.NoError(t, err)
}

func TestResolveAPIKey_EnvVarFallback(t *testing.T) {
	t.Setenv("BRUIN_CLOUD_API_KEY", "env-key")

	app := &cli.Command{
		Name: "test",
		Flags: []cli.Flag{
			apiKeyFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			key, err := resolveAPIKey(c)
			require.NoError(t, err)
			assert.Equal(t, "env-key", key)
			return nil
		},
	}

	err := app.Run(t.Context(), []string{"test"})
	require.NoError(t, err)
}

func TestResolveAPIKey_NoKeyError(t *testing.T) {
	t.Setenv("BRUIN_CLOUD_API_KEY", "")

	// Run from a temp directory so resolveAPIKey can't find a .bruin.yml in the repo.
	t.Chdir(t.TempDir())

	app := &cli.Command{
		Name: "test",
		Flags: []cli.Flag{
			apiKeyFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			_, err := resolveAPIKey(c)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "API key is required")
			return nil
		},
	}

	err := app.Run(t.Context(), []string{"test"})
	require.NoError(t, err)
}

func TestResolveProjectID_UsesExplicitProject(t *testing.T) {
	t.Parallel()

	called := false
	projectID, err := resolveProjectID("project-123", func() ([]bruincloud.Project, error) {
		called = true
		return nil, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "project-123", projectID)
	assert.False(t, called)
}

func TestResolveProjectID_UsesSingleAccessibleProject(t *testing.T) {
	t.Parallel()

	projectID, err := resolveProjectID("", func() ([]bruincloud.Project, error) {
		return []bruincloud.Project{{ID: "project-123", Name: "only-project"}}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, "project-123", projectID)
}

func TestResolveProjectID_NoProjects(t *testing.T) {
	t.Parallel()

	_, err := resolveProjectID("", func() ([]bruincloud.Project, error) {
		return []bruincloud.Project{}, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "no Bruin Cloud projects found")
}

func TestResolveProjectID_MultipleProjects(t *testing.T) {
	t.Parallel()

	_, err := resolveProjectID("", func() ([]bruincloud.Project, error) {
		return []bruincloud.Project{
			{ID: "project-1", Name: "first"},
			{ID: "project-2", Name: "second"},
		}, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "--project-id is required")
}

func TestResolveProjectID_ListProjectsError(t *testing.T) {
	t.Parallel()

	_, err := resolveProjectID("", func() ([]bruincloud.Project, error) {
		return nil, errors.New("boom")
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to list projects")
	assert.Contains(t, err.Error(), "boom")
}

func TestCloudCommand_Help(t *testing.T) {
	t.Parallel()
	isDebug := false
	cmd := Cloud(&isDebug)
	require.NotNil(t, cmd)
	assert.Equal(t, "cloud", cmd.Name)
	assert.Len(t, cmd.Commands, 7)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "projects")
	assert.Contains(t, subNames, "pipelines")
	assert.Contains(t, subNames, "runs")
	assert.Contains(t, subNames, "assets")
	assert.Contains(t, subNames, "instances")
	assert.Contains(t, subNames, "glossary")
	assert.Contains(t, subNames, "agents")
}

func TestCloudProjectsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudProjects()
	require.NotNil(t, cmd)
	assert.Equal(t, "projects", cmd.Name)
	require.Len(t, cmd.Commands, 1)
	assert.Equal(t, "list", cmd.Commands[0].Name)
}

func TestCloudPipelinesCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudPipelines()
	require.NotNil(t, cmd)
	assert.Equal(t, "pipelines", cmd.Name)
	require.Len(t, cmd.Commands, 6)
}

func TestCloudRunsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudRuns()
	require.NotNil(t, cmd)
	assert.Equal(t, "runs", cmd.Name)
	require.Len(t, cmd.Commands, 6)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "diagnose")
}

func TestCloudAssetsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudAssets()
	require.NotNil(t, cmd)
	assert.Equal(t, "assets", cmd.Name)
	require.Len(t, cmd.Commands, 2)
}

func TestCloudInstancesCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudInstances()
	require.NotNil(t, cmd)
	assert.Equal(t, "instances", cmd.Name)
	require.Len(t, cmd.Commands, 4)
}

func TestCloudGlossaryCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudGlossary()
	require.NotNil(t, cmd)
	assert.Equal(t, "glossary", cmd.Name)
	require.Len(t, cmd.Commands, 2)
}

func TestCloudAgentsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudAgents()
	require.NotNil(t, cmd)
	assert.Equal(t, "agents", cmd.Name)
	require.Len(t, cmd.Commands, 5)
}

func TestExtractErrorLines_ErrorLevel(t *testing.T) {
	t.Parallel()
	logJSON := json.RawMessage(`{
		"logs": {
			"sections": [{
				"rows": [
					{"level": "info", "message": "Starting task"},
					{"level": "ERROR", "message": "Something went wrong"},
					{"level": "info", "message": "Cleaning up"},
					{"level": "CRITICAL", "message": "Fatal failure"}
				]
			}]
		}
	}`)
	lines := extractErrorLines(logJSON)
	assert.Equal(t, []string{"Something went wrong", "Fatal failure"}, lines)
}

func TestExtractErrorLines_MessagePatterns(t *testing.T) {
	t.Parallel()
	logJSON := json.RawMessage(`{
		"logs": {
			"sections": [{
				"rows": [
					{"level": "info", "message": "Running Bruin v0.11.479"},
					{"level": "info", "message": "Query:"},
					{"level": "info", "message": "Result: 1 (expected: 999)"},
					{"level": "info", "message": "Error: custom check failed"},
					{"level": "info", "message": "Check Failed"}
				]
			}]
		}
	}`)
	lines := extractErrorLines(logJSON)
	assert.Equal(t, []string{"Result: 1 (expected: 999)", "Error: custom check failed"}, lines)
}

func TestExtractErrorLines_FallbackWhenNoErrors(t *testing.T) {
	t.Parallel()
	logJSON := json.RawMessage(`{
		"logs": {
			"sections": [{
				"rows": [
					{"level": "info", "message": "line1"},
					{"level": "info", "message": "line2"},
					{"level": "info", "message": "line3"},
					{"level": "info", "message": "line4"},
					{"level": "info", "message": "line5"},
					{"level": "info", "message": "line6"},
					{"level": "info", "message": "line7"}
				]
			}]
		}
	}`)
	lines := extractErrorLines(logJSON)
	assert.Equal(t, []string{"line3", "line4", "line5", "line6", "line7"}, lines)
}

func TestExtractErrorLines_StripsANSI(t *testing.T) {
	t.Parallel()
	logJSON := json.RawMessage(`{
		"logs": {
			"sections": [{
				"rows": [
					{"level": "ERROR", "message": "\u001b[31;1mred error\u001b[0m"}
				]
			}]
		}
	}`)
	lines := extractErrorLines(logJSON)
	assert.Equal(t, []string{"red error"}, lines)
}

func TestExtractErrorLines_InvalidJSON(t *testing.T) {
	t.Parallel()
	lines := extractErrorLines(json.RawMessage(`not json`))
	assert.Nil(t, lines)
}

func TestCloudRunsDiagnoseCommand_Flags(t *testing.T) {
	t.Parallel()
	cmd := CloudRuns()
	var diagnoseCmd *cli.Command
	for _, sub := range cmd.Commands {
		if sub.Name == "diagnose" {
			diagnoseCmd = sub
			break
		}
	}
	require.NotNil(t, diagnoseCmd)
	assert.Equal(t, "diagnose", diagnoseCmd.Name)

	flagNames := make([]string, len(diagnoseCmd.Flags))
	for i, f := range diagnoseCmd.Flags {
		flagNames[i] = f.Names()[0]
	}
	assert.Contains(t, flagNames, "api-key")
	assert.Contains(t, flagNames, "output")
	assert.Contains(t, flagNames, "project-id")
	assert.Contains(t, flagNames, "pipeline")
	assert.Contains(t, flagNames, "run-id")
	assert.Contains(t, flagNames, "latest")
}

func mkTriggerAsset(t *testing.T, name, id string, tags, downstream []string) bruincloud.Asset {
	t.Helper()
	tagsRaw, err := json.Marshal(tags)
	require.NoError(t, err)
	dsRaw, err := json.Marshal(downstream)
	require.NoError(t, err)
	return bruincloud.Asset{Name: name, ID: id, Tags: tagsRaw, Downstream: dsRaw}
}

func triggerAssetNames(selected []*bruincloud.Asset) []string {
	names := make([]string, 0, len(selected))
	for _, a := range selected {
		names = append(names, a.Name)
	}
	return names
}

func TestSelectTriggerAssets(t *testing.T) {
	t.Parallel()

	assets := []bruincloud.Asset{
		mkTriggerAsset(t, "s.raw", "id-raw", []string{"source"}, []string{"s.summary"}),
		mkTriggerAsset(t, "s.summary", "id-summary", []string{"summary"}, []string{"s.report"}),
		mkTriggerAsset(t, "s.report", "id-report", []string{"report"}, nil),
		mkTriggerAsset(t, "s.standalone", "id-standalone", []string{"report"}, nil),
	}

	tests := []struct {
		name        string
		assetInputs []string
		want        []string
		wantErr     bool
		wantNil     bool
	}{
		{name: "no selection runs whole pipeline", assetInputs: nil, wantNil: true},
		{name: "by asset name", assetInputs: []string{"s.summary"}, want: []string{"s.summary"}},
		{name: "by bare/file name", assetInputs: []string{"raw.sql"}, want: []string{"s.raw"}},
		{name: "by bare name without prefix", assetInputs: []string{"report"}, want: []string{"s.report"}},
		{name: "multiple assets", assetInputs: []string{"s.raw", "s.report"}, want: []string{"s.raw", "s.report"}},
		{name: "duplicate asset deduped", assetInputs: []string{"s.raw", "raw.sql"}, want: []string{"s.raw"}},
		{name: "unknown asset errors", assetInputs: []string{"nope"}, wantErr: true},
		{name: "one unknown among valid errors", assetInputs: []string{"s.raw", "nope"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := selectTriggerAssets(assets, tt.assetInputs)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.wantNil {
				assert.Nil(t, got)
				return
			}
			assert.Equal(t, tt.want, triggerAssetNames(got))
		})
	}
}

func TestValidateSplitFlags(t *testing.T) {
	t.Parallel()

	t.Run("no split, no chunk-size is valid", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, validateSplitFlags("", false, 1))
	})

	t.Run("chunk-size without split errors", func(t *testing.T) {
		t.Parallel()
		err := validateSplitFlags("", true, 7)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "--chunk-size requires --split")
	})

	t.Run("valid split unit", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, validateSplitFlags("month", true, 2))
	})

	t.Run("invalid split unit errors", func(t *testing.T) {
		t.Parallel()
		err := validateSplitFlags("fortnight", false, 1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid --split")
	})

	t.Run("chunk-size below one errors", func(t *testing.T) {
		t.Parallel()
		err := validateSplitFlags("day", true, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least 1")
	})
}

func TestParseRunVariables(t *testing.T) {
	t.Parallel()

	t.Run("empty input returns nil", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables(nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("quoted string value", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables([]string{`env="prod"`})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"env": "prod"}, got)
	})

	t.Run("boolean value", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables([]string{"debug=true"})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"debug": true}, got)
	})

	t.Run("json object form sets multiple keys", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables([]string{`{"region":"eu","retries":3}`})
		require.NoError(t, err)
		assert.Equal(t, "eu", got["region"])
		assert.EqualValues(t, 3, got["retries"])
	})

	t.Run("multiple flags are merged", func(t *testing.T) {
		t.Parallel()
		got, err := parseRunVariables([]string{`a="x"`, "b=true"})
		require.NoError(t, err)
		assert.Equal(t, map[string]any{"a": "x", "b": true}, got)
	})

	t.Run("unquoted bareword value errors", func(t *testing.T) {
		t.Parallel()
		_, err := parseRunVariables([]string{"env=prod"})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid variable override")
	})
}

func TestApplyAssetSelection(t *testing.T) {
	t.Parallel()

	assets := []bruincloud.Asset{
		mkTriggerAsset(t, "s.raw", "id-raw", nil, nil),
		mkTriggerAsset(t, "s.summary", "id-summary", nil, nil),
		mkTriggerAsset(t, "s.report", "id-report", nil, nil),
	}
	// A concrete selection (as selectTriggerAssets would return): raw + report.
	selected := []*bruincloud.Asset{&assets[0], &assets[2]}

	t.Run("no selection no full refresh leaves opts empty", func(t *testing.T) {
		t.Parallel()
		var opts bruincloud.TriggerRunOptions
		applyAssetSelection(&opts, nil, assets, false)
		assert.Nil(t, opts.Whitelist)
		assert.Nil(t, opts.AssetOverrides)
	})

	t.Run("selection sets whitelist by id without overrides", func(t *testing.T) {
		t.Parallel()
		var opts bruincloud.TriggerRunOptions
		applyAssetSelection(&opts, selected, assets, false)
		assert.Equal(t, []string{"id-raw", "id-report"}, opts.Whitelist)
		assert.Nil(t, opts.AssetOverrides)
	})

	t.Run("full refresh without selection covers whole pipeline", func(t *testing.T) {
		t.Parallel()
		var opts bruincloud.TriggerRunOptions
		applyAssetSelection(&opts, nil, assets, true)
		assert.Nil(t, opts.Whitelist) // empty whitelist => whole pipeline
		assert.Len(t, opts.AssetOverrides, 3)
		for _, a := range assets {
			assert.Equal(t, map[string]any{"FULL_REFRESH": 1}, opts.AssetOverrides[a.Name])
		}
	})

	t.Run("full refresh with selection covers only selected", func(t *testing.T) {
		t.Parallel()
		var opts bruincloud.TriggerRunOptions
		applyAssetSelection(&opts, selected, assets, true)
		assert.Equal(t, []string{"id-raw", "id-report"}, opts.Whitelist)
		assert.Len(t, opts.AssetOverrides, 2)
		assert.Equal(t, map[string]any{"FULL_REFRESH": 1}, opts.AssetOverrides["s.raw"])
		assert.Equal(t, map[string]any{"FULL_REFRESH": 1}, opts.AssetOverrides["s.report"])
		assert.NotContains(t, opts.AssetOverrides, "s.summary")
	})
}

func TestMatchTriggerAsset(t *testing.T) {
	t.Parallel()

	assets := []bruincloud.Asset{
		mkTriggerAsset(t, "marketing.report", "id-mkt", nil, nil),
		mkTriggerAsset(t, "finance.report", "id-fin", nil, nil),
		mkTriggerAsset(t, "core.events", "id-events", nil, nil),
	}

	t.Run("exact full name", func(t *testing.T) {
		t.Parallel()
		got, err := matchTriggerAsset(assets, "finance.report")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "id-fin", got.ID)
	})

	t.Run("unique leaf name", func(t *testing.T) {
		t.Parallel()
		got, err := matchTriggerAsset(assets, "events")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "id-events", got.ID)
	})

	t.Run("filename extension stripped", func(t *testing.T) {
		t.Parallel()
		got, err := matchTriggerAsset(assets, "events.sql")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "id-events", got.ID)
	})

	t.Run("ambiguous leaf name errors", func(t *testing.T) {
		t.Parallel()
		got, err := matchTriggerAsset(assets, "report")
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "ambiguous")
		assert.Contains(t, err.Error(), "marketing.report")
		assert.Contains(t, err.Error(), "finance.report")
	})

	t.Run("exact name wins over ambiguous leaf", func(t *testing.T) {
		t.Parallel()
		got, err := matchTriggerAsset(assets, "marketing.report")
		require.NoError(t, err)
		require.NotNil(t, got)
		assert.Equal(t, "id-mkt", got.ID)
	})

	t.Run("no match returns nil without error", func(t *testing.T) {
		t.Parallel()
		got, err := matchTriggerAsset(assets, "nope")
		require.NoError(t, err)
		assert.Nil(t, got)
	})
}
