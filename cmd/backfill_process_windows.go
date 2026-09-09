package cmd

import (
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

func configureBackfillChild(child *exec.Cmd) {
	// Windows does not support os.Interrupt; terminate the child process tree.
	child.SysProcAttr = &syscall.SysProcAttr{CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP}
	child.Cancel = func() error {
		return exec.Command("taskkill", "/T", "/F", "/PID", strconv.Itoa(child.Process.Pid)).Run()
	}
	child.WaitDelay = 30 * time.Second
}
