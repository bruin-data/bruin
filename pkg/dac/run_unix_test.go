//go:build !windows

package dac

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitForSignal() int {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	_, _ = fmt.Println("ready")
	sig := <-signals
	_, _ = fmt.Printf("stopped by %s\n", sig)
	return 3
}

func TestRun_ForwardsSignalsAndWaitsForDac(t *testing.T) {
	t.Parallel()

	executable, err := os.Executable()
	require.NoError(t, err)

	for _, sig := range []syscall.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP} {
		t.Run(sig.String(), func(t *testing.T) {
			t.Parallel()

			env := append(os.Environ(), helperModeEnv+"=fake-dac", "FAKE_DAC_WAIT_FOR_SIGNAL=1")
			stdoutReader, stdoutWriter := io.Pipe()
			signals := make(chan os.Signal, 1)
			done := make(chan error, 1)
			go func() {
				done <- Run(executable, nil, env, nil, stdoutWriter, io.Discard, signals)
				_ = stdoutWriter.Close()
			}()

			lines := bufio.NewScanner(stdoutReader)
			require.True(t, lines.Scan()) // the JSON report
			require.True(t, lines.Scan())
			require.Equal(t, "ready", lines.Text())

			signals <- sig
			require.True(t, lines.Scan())
			assert.Equal(t, "stopped by "+sig.String(), lines.Text())

			select {
			case err := <-done:
				var exitError *exec.ExitError
				require.ErrorAs(t, err, &exitError)
				assert.Equal(t, 3, exitError.ExitCode(), "Run must wait for dac and report its exit code")
			case <-time.After(30 * time.Second):
				t.Fatal("Run did not return after dac exited")
			}
			_, _ = io.Copy(io.Discard, stdoutReader)
		})
	}
}
