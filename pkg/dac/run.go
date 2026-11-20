package dac

import (
	"context"
	"io"
	"os"
	"os/exec"
)

// Environment returns the environment dac runs with under `bruin dac`: the
// caller's environment plus the variables that make dac query through bruinPath,
// present itself as `bruin dac` and skip its own update check.
func Environment(base []string, bruinPath string, telemetryOptOut bool) []string {
	env := append([]string{}, base...)
	env = append(
		env,
		"DAC_BRUIN_PATH="+bruinPath,
		"DAC_HOST=bruin",
		"DAC_NO_UPDATE_CHECK=1",
	)
	if telemetryOptOut {
		env = append(env, "TELEMETRY_OPTOUT=1")
	}
	return env
}

// Run starts binary with args and env, connects it to the given stdio, and waits
// for it to exit. Signals received on signals while dac runs are passed on to it
// (see forwardSignal); cancelling ctx kills it. A non-zero exit is returned as
// *exec.ExitError.
func Run(ctx context.Context, binary string, args, env []string, stdin io.Reader, stdout, stderr io.Writer, signals <-chan os.Signal) error {
	cmd := exec.CommandContext(ctx, binary, args...) //nolint:gosec // running the dac binary is the point
	cmd.Env = env
	cmd.Stdin = stdin
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		return err
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	for {
		select {
		case err := <-done:
			return err
		case sig := <-signals:
			forwardSignal(cmd.Process, sig)
		}
	}
}
