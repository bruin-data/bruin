//go:build windows

package python

import (
	"os/exec"
	"time"
)

// managedStopGrace is how long a streaming child is given to exit after the
// context is cancelled before the runtime force-kills it.
const managedStopGrace = 30 * time.Second

// configureManagedCmd ties a long-running streaming command to the run context.
// Windows has no POSIX process groups, so we rely on exec.Cmd's context-based
// cancellation (which terminates the process) plus WaitDelay as a backstop.
func configureManagedCmd(cmd *exec.Cmd) {
	cmd.WaitDelay = managedStopGrace
}
