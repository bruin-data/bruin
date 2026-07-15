//go:build !windows

package python

import (
	"os/exec"
	"syscall"
	"time"
)

// managedStopGrace is how long a streaming child is given to flush and exit after
// it receives SIGTERM before the process is force-killed.
const managedStopGrace = 30 * time.Second

// configureManagedCmd prepares a long-running streaming command for graceful
// teardown. ingestr is launched via `uv tool run`, so the process we start (uv)
// forks the real ingestr/python child; signalling only uv's PID would orphan
// that child (and, for CDC, leave a replication slot open). Putting the child in
// its own process group lets us signal the whole tree at once.
//
// On context cancellation exec.Cmd calls Cancel, which sends SIGTERM to the
// group. ingestr treats SIGTERM as its normal stop: it flushes buffered records,
// advances its offset, and exits 0. WaitDelay bounds how long we wait for that
// clean exit before the runtime force-kills the process.
func configureManagedCmd(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		// Negative PID targets the whole process group (uv + ingestr/python).
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM); err != nil {
			// Fall back to signalling just the process we started.
			return cmd.Process.Signal(syscall.SIGTERM)
		}
		return nil
	}
	cmd.WaitDelay = managedStopGrace
}
