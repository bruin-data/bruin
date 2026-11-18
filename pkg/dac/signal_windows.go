//go:build windows

package dac

import (
	"os"
	"syscall"
)

// Signals are the console events `bruin dac` catches so Bruin keeps waiting for
// dac to exit instead of exiting first.
var Signals = []os.Signal{os.Interrupt, syscall.SIGTERM}

// forwardSignal does nothing on Windows: Ctrl-C, Ctrl-Break and console close
// events go to every process attached to the console, so dac already has them.
func forwardSignal(*os.Process, os.Signal) {}
