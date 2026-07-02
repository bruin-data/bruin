package cmd

import (
	"io"
	"os"
	"strings"
	"testing"
)

// TestLoggerLateBindsToStdout ensures zap writes to whatever os.Stdout is at
// write time, so logOutput's masking pipe can redact zap output.
func TestLoggerLateBindsToStdout(t *testing.T) { //nolint:paralleltest // mutates global os.Stdout
	orig := os.Stdout
	defer func() { os.Stdout = orig }()

	// Build the logger while os.Stdout is still the original.
	log := makeLogger(true)

	// Redirect os.Stdout AFTER building (mimics logOutput swapping in the pipe).
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w

	log.Debugf("late-bind-marker-42")
	_ = w.Close()

	data, _ := io.ReadAll(r)
	os.Stdout = orig

	if !strings.Contains(string(data), "late-bind-marker-42") {
		t.Fatalf("zap output did not follow the redirected os.Stdout; got %q", string(data))
	}
}
