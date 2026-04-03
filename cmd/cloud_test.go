package cmd

import (
	"context"
	"encoding/json"
	"testing"

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
