//go:build !windows

package cmd

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func configureBackfillChild(child *exec.Cmd) {
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	child.Cancel = func() error {
		err := syscall.Kill(-child.Process.Pid, syscall.SIGINT)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	child.WaitDelay = 30 * time.Second
}
