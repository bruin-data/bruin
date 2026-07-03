package cmd

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/mask"
)

// TestLogOutputMasksLogFile proves the sink masks anything written to os.Stdout
// before it reaches the log file. Not parallel: it swaps global os.Stdout/Stderr.
func TestLogOutputMasksLogFile(t *testing.T) { //nolint:paralleltest // mutates global os.Stdout
	origOut, origErr := os.Stdout, os.Stderr
	defer func() {
		os.Stdout, os.Stderr = origOut, origErr
		log.SetOutput(origErr)
	}()

	logPath := filepath.Join(t.TempDir(), "run.log")
	masker := mask.New([]string{"supersecret-value"})

	cleanup, err := logOutput(logPath, io.Discard, masker)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("connecting with token=supersecret-value now")
	cleanup() // closes the pipe, flushes, waits for the copy goroutine

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	if strings.Contains(got, "supersecret-value") {
		t.Fatalf("secret leaked to log file: %q", got)
	}
	if !strings.Contains(got, mask.Mask) {
		t.Fatalf("mask not applied in log file: %q", got)
	}
}

// TestLogOutputMasksTerminalWithoutLogFile proves the --no-log-file path (empty
// logPath) still masks terminal output and writes no file.
func TestLogOutputMasksTerminalWithoutLogFile(t *testing.T) { //nolint:paralleltest // mutates global os.Stdout
	origOut, origErr := os.Stdout, os.Stderr
	defer func() {
		os.Stdout, os.Stderr = origOut, origErr
		log.SetOutput(origErr)
	}()

	var term bytes.Buffer
	masker := mask.New([]string{"supersecret-value"})

	cleanup, err := logOutput("", &term, masker) // empty logPath => no file
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("connecting with token=supersecret-value now")
	cleanup() // closes the pipe, flushes, waits for the copy goroutine

	got := term.String()
	if strings.Contains(got, "supersecret-value") {
		t.Fatalf("secret leaked to terminal: %q", got)
	}
	if !strings.Contains(got, mask.Mask) {
		t.Fatalf("mask not applied to terminal: %q", got)
	}
}
