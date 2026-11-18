package dac

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const helperModeEnv = "BRUIN_DAC_TEST_HELPER"

// TestMain lets the test binary stand in for dac, or for a second Bruin process
// installing dac, when started by helperCommand.
func TestMain(m *testing.M) {
	switch os.Getenv(helperModeEnv) {
	case "":
		os.Exit(m.Run())
	case "fake-dac":
		os.Exit(fakeDac())
	case "install":
		os.Exit(installFromTestServer())
	default:
		os.Exit(2)
	}
}

func helperCommand(t *testing.T, mode string, env ...string) *exec.Cmd {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), executable) //nolint:gosec
	cmd.Env = append(append(os.Environ(), helperModeEnv+"="+mode), env...)
	return cmd
}

// fakeDac reports what it was started with: its arguments and the dac variables
// as JSON on stdout, and then stdin echoed back. FAKE_DAC_EXIT sets its exit
// code; FAKE_DAC_WAIT_FOR_SIGNAL makes it wait until a signal arrives.
func fakeDac() int {
	report := map[string]any{"args": os.Args[1:]}
	for _, key := range []string{"DAC_BRUIN_PATH", "DAC_HOST", "DAC_NO_UPDATE_CHECK", "TELEMETRY_OPTOUT", "USER_VARIABLE"} {
		if value, ok := os.LookupEnv(key); ok {
			report[key] = value
		}
	}
	if err := json.NewEncoder(os.Stdout).Encode(report); err != nil {
		return 2
	}

	if os.Getenv("FAKE_DAC_WAIT_FOR_SIGNAL") != "" {
		return waitForSignal()
	}

	_, _ = io.Copy(os.Stdout, os.Stdin)
	_, _ = fmt.Fprintln(os.Stderr, "fake dac stderr")
	code, _ := strconv.Atoi(os.Getenv("FAKE_DAC_EXIT"))
	return code
}

func installFromTestServer() int {
	installer := &Installer{
		Release: Release{Version: testVersion, Archives: map[string]string{
			"dac_Linux_x86_64.tar.gz": os.Getenv("DAC_TEST_SHA"),
		}},
		HomeDir:    os.Getenv("DAC_TEST_HOME"),
		GOOS:       "linux",
		GOARCH:     "amd64",
		BaseURL:    os.Getenv("DAC_TEST_SERVER"),
		HTTPClient: http.DefaultClient,
		Output:     os.Stderr,
	}
	if _, err := installer.EnsureInstalled(context.Background()); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func TestEnvironment(t *testing.T) {
	t.Parallel()

	base := []string{"PATH=/usr/bin", "DAC_HOST=standalone"}
	env := Environment(base, "/opt/bruin/bruin", false)
	assert.Equal(t, []string{
		"PATH=/usr/bin",
		"DAC_HOST=standalone",
		"DAC_BRUIN_PATH=/opt/bruin/bruin",
		"DAC_HOST=bruin",
		"DAC_NO_UPDATE_CHECK=1",
	}, env)
	assert.Equal(t, []string{"PATH=/usr/bin", "DAC_HOST=standalone"}, base, "the caller's slice must not change")

	assert.Equal(t, "TELEMETRY_OPTOUT=1", Environment(base, "/opt/bruin/bruin", true)[5])
}

func TestRun_PassesArgsStdioEnvAndExitCode(t *testing.T) {
	t.Parallel()

	executable, err := os.Executable()
	require.NoError(t, err)

	for _, tc := range []struct {
		name     string
		exitCode int
		optOut   bool
	}{
		{name: "success", exitCode: 0},
		{name: "failure exit code", exitCode: 7, optOut: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base := append(
				os.Environ(),
				helperModeEnv+"=fake-dac",
				"FAKE_DAC_EXIT="+strconv.Itoa(tc.exitCode),
				"USER_VARIABLE=kept",
				"DAC_HOST=wrong",
			)
			args := []string{"serve", "--dir", "my project", "--help", "--", "-x"}
			var stdout, stderr bytes.Buffer

			err := Run(context.Background(), executable, args, Environment(base, "/path/to/bruin", tc.optOut), strings.NewReader("from stdin"), &stdout, &stderr, nil)
			if tc.exitCode == 0 {
				require.NoError(t, err)
			} else {
				var exitError *exec.ExitError
				require.ErrorAs(t, err, &exitError)
				assert.Equal(t, tc.exitCode, exitError.ExitCode())
			}

			decoder := json.NewDecoder(&stdout)
			var report map[string]any
			require.NoError(t, decoder.Decode(&report))
			expected := map[string]any{
				"args":                []any{"serve", "--dir", "my project", "--help", "--", "-x"},
				"DAC_BRUIN_PATH":      "/path/to/bruin",
				"DAC_HOST":            "bruin",
				"DAC_NO_UPDATE_CHECK": "1",
				"USER_VARIABLE":       "kept",
			}
			if tc.optOut {
				expected["TELEMETRY_OPTOUT"] = "1"
			} else if value, ok := os.LookupEnv("TELEMETRY_OPTOUT"); ok {
				expected["TELEMETRY_OPTOUT"] = value
			}
			assert.Equal(t, expected, report)
			rest, err := io.ReadAll(decoder.Buffered())
			require.NoError(t, err)
			assert.Equal(t, "from stdin", strings.TrimSpace(string(rest)))
			assert.Equal(t, "fake dac stderr\n", stderr.String())
		})
	}
}

func TestRun_MissingBinary(t *testing.T) {
	t.Parallel()

	err := Run(context.Background(), "/does/not/exist/dac", nil, nil, nil, io.Discard, io.Discard, nil)
	require.Error(t, err)
	var exitError *exec.ExitError
	assert.NotErrorAs(t, err, &exitError)
}
