package cmd

import (
	"bytes"
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestRenderCurlArgs(t *testing.T) {
	t.Parallel()

	connections := map[string]any{
		"warehouse": &config.PostgresConnection{
			ConnectionMetadata: config.ConnectionMetadata{Name: "warehouse"},
			Host:               "db.internal",
			Username:           "api-user",
			Password:           "secret-token",
			Port:               5432,
		},
		"auth-api": map[string]any{
			"credentials": map[string]any{"token": "nested-secret"},
		},
	}
	lookupCounts := make(map[string]int)
	lookup := func(name string) (any, error) {
		lookupCounts[name]++
		return connections[name], nil
	}

	actual, err := renderCurlArgs([]string{
		"--url",
		`https://{{ bruin.connection("warehouse").host }}:{{ bruin.connection("warehouse").port }}/health`,
		"--header",
		`Authorization: Bearer {{ bruin.connection("auth-api").credentials.token }}`,
		`--user={{ bruin.connection("warehouse").username }}:{{ bruin.connection("warehouse").password }}`,
		`X-Slug: {{ bruin.slugify("Hello World") }}`,
	}, lookup)

	require.NoError(t, err)
	assert.Equal(t, []string{
		"--url",
		"https://db.internal:5432/health",
		"--header",
		"Authorization: Bearer nested-secret",
		"--user=api-user:secret-token",
		"X-Slug: hello_world",
	}, actual)
	assert.Equal(t, map[string]int{"auth-api": 1, "warehouse": 1}, lookupCounts)
}

func TestRenderCurlArgsRendersEveryArgumentAndPreservesCurlVariables(t *testing.T) {
	t.Parallel()

	actual, err := renderCurlArgs([]string{
		`--{{ bruin.connection("api").option }}`,
		`https://{{ bruin.connection("api").host }}/{{path:trim:url}}/{{missing}}`,
		"__BRUIN_CURL_VARIABLE_0__ {{path}}",
	}, func(name string) (any, error) {
		assert.Equal(t, "api", name)
		return map[string]any{"option": "expand-url", "host": "example.com"}, nil
	})

	require.NoError(t, err)
	assert.Equal(t, []string{
		"--expand-url",
		"https://example.com/{{path:trim:url}}/{{missing}}",
		"__BRUIN_CURL_VARIABLE_0__ {{path}}",
	}, actual)
}

func TestRenderCurlArgsReturnsArgumentIndexWithoutLeakingOtherArguments(t *testing.T) {
	t.Parallel()

	_, err := renderCurlArgs([]string{
		"Authorization: Bearer private-value",
		`{{ bruin.connection("api").missing }}`,
	}, func(string) (any, error) {
		return map[string]any{"token": "private-value"}, nil
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to render curl argument 2")
	assert.NotContains(t, err.Error(), "private-value")
}

func TestRenderCurlArgsReportsConnectionLookupErrors(t *testing.T) {
	t.Parallel()

	_, err := renderCurlArgs([]string{
		`https://{{ bruin.connection("missing-api").host }}/health`,
	}, func(name string) (any, error) {
		return nil, config.NewConnectionNotFoundError(t.Context(), "", name)
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to render curl argument 1")
	assert.Contains(t, err.Error(), "connection 'missing-api' not found")
	assert.NotContains(t, err.Error(), "bug in the jinja renderer")
}

func TestRenderCurlArgsDoesNotLookUpConnectionsForBuiltinOnlyTemplates(t *testing.T) {
	t.Parallel()

	actual, err := renderCurlArgs([]string{
		`https://example.com/{{ bruin.slugify("Account Name") }}`,
	}, func(string) (any, error) {
		t.Fatal("connection lookup should not be called")
		return nil, nil
	})

	require.NoError(t, err)
	assert.Equal(t, []string{"https://example.com/account_name"}, actual)
}

func TestCurlCommandPassesAllCurlArgumentsAfterSeparator(t *testing.T) { //nolint:paralleltest // cli mutates the global help flag
	var executedArgs []string
	var gotStdin io.Reader
	var gotStdout, gotStderr io.Writer
	resolver := func(_ context.Context, c *cli.Command) (curlConnectionLookup, error) {
		assert.Equal(t, "production", c.String("environment"))
		return func(name string) (any, error) {
			connections := map[string]any{
				"service": map[string]any{"host": "example.com"},
				"auth":    map[string]any{"token": "resolved-secret"},
			}
			return connections[name], nil
		}, nil
	}
	executor := func(_ context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
		executedArgs = args
		gotStdin = stdin
		gotStdout = stdout
		gotStderr = stderr
		return nil
	}

	stdin := bytes.NewBufferString("request body")
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	command := newCurlCommand(resolver, executor)
	command.Reader = stdin
	command.Writer = stdout
	command.ErrWriter = stderr

	err := command.Run(t.Context(), []string{
		"curl",
		"--environment", "production",
		"--",
		"--request", "POST",
		"--header", `Authorization: Bearer {{ bruin.connection("auth").token }}`,
		`https://{{ bruin.connection("service").host }}`,
	})

	require.NoError(t, err)
	assert.Equal(t, []string{
		"--request", "POST",
		"--header", "Authorization: Bearer resolved-secret",
		"https://example.com",
	}, executedArgs)
	assert.Same(t, stdin, gotStdin)
	assert.Same(t, stdout, gotStdout)
	assert.Same(t, stderr, gotStderr)
}

func TestCurlCommandRequiresCurlArguments(t *testing.T) { //nolint:paralleltest // cli mutates the global help flag
	command := newCurlCommand(
		func(context.Context, *cli.Command) (curlConnectionLookup, error) {
			t.Fatal("resolver should not be called")
			return nil, nil
		},
		func(context.Context, []string, io.Reader, io.Writer, io.Writer) error {
			t.Fatal("executor should not be called")
			return nil
		},
	)
	stderr := &bytes.Buffer{}
	command.ErrWriter = stderr
	// cli exits the process for ExitCoder errors, which would kill the test binary.
	command.ExitErrHandler = func(context.Context, *cli.Command, error) {}

	err := command.Run(t.Context(), []string{"curl", "--"})

	var exitErr cli.ExitCoder
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())
	assert.Contains(t, stderr.String(), "at least one curl option or URL is required after --")
}

func TestCurlCommandReportsRenderFailuresOnStderr(t *testing.T) { //nolint:paralleltest // cli mutates the global help flag
	command := newCurlCommand(
		func(context.Context, *cli.Command) (curlConnectionLookup, error) {
			return func(string) (any, error) {
				return map[string]any{}, nil
			}, nil
		},
		func(context.Context, []string, io.Reader, io.Writer, io.Writer) error {
			t.Fatal("executor should not be called")
			return nil
		},
	)
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	command.Writer = stdout
	command.ErrWriter = stderr
	command.ExitErrHandler = func(context.Context, *cli.Command, error) {}

	err := command.Run(t.Context(), []string{
		"curl", "--", `https://example.com/{{ bruin.connection("service").missing }}`,
	})

	var exitErr cli.ExitCoder
	require.ErrorAs(t, err, &exitErr)
	assert.Equal(t, 1, exitErr.ExitCode())
	assert.Contains(t, stderr.String(), "failed to render curl argument 1")
	assert.Empty(t, stdout.String())
}

func TestExecuteCurlPassesArgumentsUnchanged(t *testing.T) { //nolint:paralleltest // changes PATH
	if runtime.GOOS == osWindows {
		t.Skip("uses a POSIX test executable")
	}

	binDir := t.TempDir()
	curlPath := filepath.Join(binDir, "curl")
	require.NoError(t, os.WriteFile(curlPath, []byte(`#!/bin/sh
for arg in "$@"; do
  printf '<%s>\n' "$arg"
done
`), 0o700))
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

	args := []string{
		"--future-option=value",
		"--unknown-option",
		"value with spaces",
		"-hall",
		"https://example.com",
	}
	var stdout, stderr bytes.Buffer

	err := executeCurl(t.Context(), args, strings.NewReader("request body"), &stdout, &stderr)

	require.NoError(t, err)
	assert.Equal(t, "<"+strings.Join(args, ">\n<")+">\n", stdout.String())
	assert.Empty(t, stderr.String())
}

// A signalled process has no exit code of its own, so Bruin reports the 128+signal
// value a shell would have reported had it run curl directly.
func TestCurlExitCode(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == osWindows {
		t.Skip("relies on POSIX signals")
	}

	failed := exec.CommandContext(t.Context(), "sh", "-c", "exit 7").Run()
	var failedExit *exec.ExitError
	require.ErrorAs(t, failed, &failedExit)
	assert.Equal(t, 7, curlExitCode(failedExit))

	signalled := exec.CommandContext(t.Context(), "sh", "-c", "kill -TERM $$")
	require.NoError(t, signalled.Start())
	var signalledExit *exec.ExitError
	require.ErrorAs(t, signalled.Wait(), &signalledExit)
	assert.Equal(t, 128+int(syscall.SIGTERM), curlExitCode(signalledExit))
}
