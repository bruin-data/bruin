//go:build !windows

package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

// fakeDacScript prints its arguments one per line, then the dac variables, and
// exits with FAKE_DAC_EXIT.
const fakeDacScript = `#!/bin/sh
for arg in "$@"; do printf 'arg=%s\n' "$arg"; done
printf 'DAC_BRUIN_PATH=%s\n' "$DAC_BRUIN_PATH"
printf 'DAC_HOST=%s\n' "$DAC_HOST"
printf 'DAC_NO_UPDATE_CHECK=%s\n' "$DAC_NO_UPDATE_CHECK"
printf 'TELEMETRY_OPTOUT=%s\n' "$TELEMETRY_OPTOUT"
printf 'stdin=%s\n' "$(cat)"
echo "dac stderr" >&2
exit "${FAKE_DAC_EXIT:-0}"
`

func runBruinDac(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	app := &cli.Command{
		Name:      "bruin",
		Reader:    strings.NewReader("piped input"),
		Writer:    &stdout,
		ErrWriter: &stderr,
		Commands:  []*cli.Command{Dac()},
		// cli exits the process for ExitCoder errors, which would kill the test binary.
		ExitErrHandler: func(context.Context, *cli.Command, error) {},
	}
	err := app.Run(context.Background(), append([]string{"bruin", "dac"}, args...))
	return stdout.String(), stderr.String(), err
}

func useFakeDac(t *testing.T) {
	t.Helper()
	fake := filepath.Join(t.TempDir(), "dac")
	require.NoError(t, os.WriteFile(fake, []byte(fakeDacScript), 0o755)) //nolint:gosec
	t.Setenv(dacBinaryOverrideEnv, fake)
	t.Setenv("FAKE_DAC_EXIT", "")
	t.Setenv("TELEMETRY_OPTOUT", "")
}

func TestDac_PassesArgumentsUnparsed(t *testing.T) {
	useFakeDac(t)

	for _, args := range [][]string{
		{"serve", "--dir", "my project", "--port", "8321"},
		{"--help"},
		{"help"},
		{"-c", ".bruin.yml", "check", "--", "--not-a-bruin-flag"},
	} {
		stdout, stderr, err := runBruinDac(t, args...)
		require.NoError(t, err)

		var got []string
		for _, line := range strings.Split(stdout, "\n") {
			if value, ok := strings.CutPrefix(line, "arg="); ok {
				got = append(got, value)
			}
		}
		assert.Equal(t, args, got)
		assert.Contains(t, stdout, "stdin=piped input\n")
		assert.Equal(t, "dac stderr\n", stderr)
	}
}

func TestDac_SetsHostEnvironment(t *testing.T) {
	useFakeDac(t)
	bruinPath, err := os.Executable()
	require.NoError(t, err)

	stdout, _, err := runBruinDac(t, "check")
	require.NoError(t, err)
	assert.Contains(t, stdout, "DAC_BRUIN_PATH="+bruinPath+"\n")
	assert.Contains(t, stdout, "DAC_HOST=bruin\n")
	assert.Contains(t, stdout, "DAC_NO_UPDATE_CHECK=1\n")
	assert.Contains(t, stdout, "TELEMETRY_OPTOUT=\n")
}

func TestDac_TelemetryOptOutReachesDac(t *testing.T) {
	useFakeDac(t)
	previous := telemetry.OptOut
	telemetry.OptOut = true
	t.Cleanup(func() { telemetry.OptOut = previous })

	stdout, _, err := runBruinDac(t, "check")
	require.NoError(t, err)
	assert.Contains(t, stdout, "TELEMETRY_OPTOUT=1\n")
}

func TestDac_PassesExitCode(t *testing.T) {
	useFakeDac(t)
	t.Setenv("FAKE_DAC_EXIT", "4")

	_, stderr, err := runBruinDac(t, "check")
	var exitCoder cli.ExitCoder
	require.ErrorAs(t, err, &exitCoder)
	assert.Equal(t, 4, exitCoder.ExitCode())
	assert.Empty(t, exitCoder.Error(), "Bruin must not add its own message to dac's failure")
	assert.Equal(t, "dac stderr\n", stderr)
}

func TestDac_ReportsMissingBinary(t *testing.T) {
	t.Setenv(dacBinaryOverrideEnv, filepath.Join(t.TempDir(), "missing-dac"))

	stdout, stderr, err := runBruinDac(t, "check")
	var exitCoder cli.ExitCoder
	require.ErrorAs(t, err, &exitCoder)
	assert.Equal(t, 1, exitCoder.ExitCode())
	assert.Empty(t, stdout)
	assert.Contains(t, stderr, "failed to run dac at ")
}
