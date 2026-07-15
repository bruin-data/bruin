//go:build windows

package python

import (
	"os"
	"os/exec"
	"strconv"
	"syscall"
)

// configureCommandCancellation starts a new process group and uses taskkill to
// terminate the full command tree when an asset is cancelled.
func configureCommandCancellation(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
	cmd.Cancel = func() error {
		kill := exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(cmd.Process.Pid)) //nolint:gosec
		if err := kill.Run(); err != nil {
			if cmd.ProcessState != nil && cmd.ProcessState.Exited() {
				return os.ErrProcessDone
			}
			return err
		}
		return nil
	}
}
