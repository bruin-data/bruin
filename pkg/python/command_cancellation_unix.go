//go:build !windows

package python

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// configureCommandCancellation puts the command and its descendants in a new
// process group so a cancelled asset does not leave child processes running.
func configureCommandCancellation(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
}
