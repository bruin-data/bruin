package cmd

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestCloudPipelineTrigger(t *testing.T) {
	for _, command := range []string{"get", "set", "delete"} {
		for _, output := range []string{"json", "plain"} {
			t.Run(command+"/"+output, func(t *testing.T) {
				requests := 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					requests++
					assert.Equal(t, map[string]string{"get": "GET", "set": "PUT", "delete": "DELETE"}[command], r.Method)
					assert.Equal(t, "/scheduled-agents/42/pipeline-trigger", r.URL.Path)
					assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
					assert.Equal(t, "analytics-team", r.Header.Get("X-Bruin-Team"))
					if command == "set" {
						var body map[string]any
						assert.NoError(t, json.NewDecoder(r.Body).Decode(&body))
						assert.Equal(t, map[string]any{"id": "daily-etl", "project_id": "analytics"}, body)
					}
					w.Header().Set("Content-Type", "application/json")
					if command == "delete" {
						_, _ = io.WriteString(w, `{"pipeline_trigger":null}`)
					} else {
						_, _ = io.WriteString(w, `{"pipeline_trigger":{"id":"daily-etl","project_id":"analytics"}}`)
					}
				}))
				defer server.Close()
				t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
				args := []string{"cloud", "scheduled-agents", "pipeline-trigger", command, "--scheduled-agent-id", "42", "--api-key", "test-key", "--team", "analytics-team", "--output", output}
				if command == "set" {
					args = append(args, "--project-id", "analytics", "--pipeline", "daily-etl")
				}
				reader, writer, err := os.Pipe()
				require.NoError(t, err)
				original := os.Stdout
				os.Stdout = writer
				defer func() { os.Stdout = original }()
				debug := false
				runErr := runCLI(t.Context(), Cloud(&debug), args)
				require.NoError(t, writer.Close())
				os.Stdout = original
				data, err := io.ReadAll(reader)
				require.NoError(t, reader.Close())
				require.NoError(t, err)
				require.NoError(t, runErr)
				assert.Equal(t, 1, requests)
				if output == "json" {
					if command == "delete" {
						assert.JSONEq(t, `{"pipeline_trigger":null}`, string(data))
					} else {
						assert.JSONEq(t, `{"pipeline_trigger":{"id":"daily-etl","project_id":"analytics"}}`, string(data))
					}
				}
			})
		}
	}
}

func TestCloudPipelineTriggerErrors(t *testing.T) {
	for _, status := range []int{403, 404, 422} {
		for _, command := range []string{"get", "set", "delete"} {
			t.Run(http.StatusText(status)+"/"+command, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"message":"Cannot manage this trigger","error":"rejected"}`)
				}))
				defer server.Close()
				t.Setenv("BRUIN_CLOUD_BASE_URL", server.URL)
				cmd := cloudPipelineTriggerCommand(command, "test")
				cmd.ExitErrHandler = func(context.Context, *cli.Command, error) {}
				args := []string{command, "--scheduled-agent-id", "42", "--api-key", "test-key"}
				if command == "set" {
					args = append(args, "--project-id", "analytics", "--pipeline", "daily-etl")
				}
				err := runCLI(t.Context(), cmd, args)
				require.Error(t, err)
				var exit cli.ExitCoder
				require.ErrorAs(t, err, &exit)
				assert.Equal(t, 1, exit.ExitCode())
			})
		}
	}
}

func TestCloudPipelineTriggerInvalidFlags(t *testing.T) {
	t.Parallel()
	for _, args := range [][]string{
		{"get"},
		{"get", "--scheduled-agent-id", "0"},
		{"delete", "--scheduled-agent-id", "-1"},
		{"set", "--scheduled-agent-id", "42", "--project-id", "analytics"},
		{"set", "--scheduled-agent-id", "42", "--pipeline", "daily-etl"},
		{"set", "--scheduled-agent-id", "42", "--pipeline", " ", "--project-id", "analytics"},
	} {
		cmd := cloudPipelineTriggerCommand(args[0], "test")
		cmd.ExitErrHandler = func(context.Context, *cli.Command, error) {}
		require.Error(t, runCLI(t.Context(), cmd, args))
	}
}

func TestScheduledAgentPlanRejectsPipelineTrigger(t *testing.T) {
	t.Parallel()
	for _, state := range []string{`{"pipeline_trigger":null}`, `{"pipeline_trigger":{"id":"daily-etl","project_id":"analytics"}}`} {
		cmd := &cli.Command{
			Name:  "plan",
			Flags: scheduledAgentPlanFlags(),
			Action: func(_ context.Context, c *cli.Command) error {
				_, err := buildScheduledAgentFields(c)
				return err
			},
		}
		require.ErrorContains(t, runCLI(t.Context(), cmd, []string{"plan", "--state", state}), "pipeline-trigger set or delete")
	}
}
