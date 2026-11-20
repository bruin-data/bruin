//go:build !windows

package dac

import (
	"os"
	"syscall"
)

// Signals are the signals `bruin dac` catches and passes on to dac.
var Signals = []os.Signal{syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP}

// forwardSignal passes sig on to dac. dac stays in Bruin's process group so it
// can still read from the terminal; a Ctrl-C in the terminal therefore reaches
// dac directly as well, and a second SIGINT is harmless.
func forwardSignal(process *os.Process, sig os.Signal) {
	_ = process.Signal(sig)
}
