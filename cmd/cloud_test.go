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
	assert.Len(t, cmd.Commands, 10)

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
	assert.Contains(t, subNames, "connections")
	assert.Contains(t, subNames, "dashboards")
	assert.Contains(t, subNames, "scheduled-agents")
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
	require.Len(t, cmd.Commands, 10)
}

func TestCloudDashboardsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudDashboards()
	require.NotNil(t, cmd)
	assert.Equal(t, "dashboards", cmd.Name)
	require.Len(t, cmd.Commands, 4)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "list")
	assert.Contains(t, subNames, "get")
	assert.Contains(t, subNames, "create")
	assert.Contains(t, subNames, "update")
}

func TestCloudScheduledAgentsCommand_Help(t *testing.T) {
	t.Parallel()
	cmd := CloudScheduledAgents()
	require.NotNil(t, cmd)
	assert.Equal(t, "scheduled-agents", cmd.Name)
	require.Len(t, cmd.Commands, 4)

	subNames := make([]string, len(cmd.Commands))
	for i, sub := range cmd.Commands {
		subNames[i] = sub.Name
	}
	assert.Contains(t, subNames, "list")
	assert.Contains(t, subNames, "get")
	assert.Contains(t, subNames, "create")
	assert.Contains(t, subNames, "update")
}

func TestMessagePairIDFromEnv(t *testing.T) {
	t.Run("reads a numeric value", func(t *testing.T) {
		t.Setenv("BRUIN_MESSAGE_PAIR_ID", " 42 ")
		id, ok := messagePairIDFromEnv()
		require.True(t, ok)
		assert.Equal(t, 42, id)
	})

	t.Run("skips when unset", func(t *testing.T) {
		t.Setenv("BRUIN_MESSAGE_PAIR_ID", "")
		_, ok := messagePairIDFromEnv()
		assert.False(t, ok)
	})

	t.Run("skips a non-numeric value", func(t *testing.T) {
		t.Setenv("BRUIN_MESSAGE_PAIR_ID", "abc")
		_, ok := messagePairIDFromEnv()
		assert.False(t, ok)
	})
}

func TestParseDashboardState(t *testing.T) {
	t.Parallel()

	t.Run("parses a JSON object", func(t *testing.T) {
		t.Parallel()
		got, err := parseJSONOrYAMLObject([]byte(`{"name":"d","rows":[]}`))
		require.NoError(t, err)
		assert.Equal(t, "d", got["name"])
	})

	t.Run("parses a YAML object", func(t *testing.T) {
		t.Parallel()
		got, err := parseJSONOrYAMLObject([]byte("name: d\nrows: []\n"))
		require.NoError(t, err)
		assert.Equal(t, "d", got["name"])
	})

	t.Run("preserves an unquoted date-like scalar as a string", func(t *testing.T) {
		t.Parallel()
		got, err := parseJSONOrYAMLObject([]byte("default: 2024-01-01\n"))
		require.NoError(t, err)
		assert.Equal(t, "2024-01-01", got["default"])
	})

	t.Run("returns nil for non-object documents", func(t *testing.T) {
		t.Parallel()
		for _, in := range []string{"", "null", `"scalar"`, "[1, 2]"} {
			got, err := parseJSONOrYAMLObject([]byte(in))
			require.NoError(t, err, "input %q", in)
			assert.Nil(t, got, "input %q", in)
		}
	})

	t.Run("errors on malformed YAML", func(t *testing.T) {
		t.Parallel()
		_, err := parseJSONOrYAMLObject([]byte("name: [unclosed"))
		require.Error(t, err)
	})
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
