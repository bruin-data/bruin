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

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/mask"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// fakeConnGetter is a config.ConnectionDetailsGetter that returns preconfigured
// connection details by name.
type fakeConnGetter struct{ conns map[string]any }

func (f fakeConnGetter) GetConnectionDetails(name string) any { return f.conns[name] }
func (f fakeConnGetter) GetConnectionType(string) string      { return "" }

// TestCollectRunSecrets_ScopesFileReadsToUsedConnections proves the memory/leak
// balance: inline secrets are collected from every configured connection, but
// credential-file CONTENTS are read only for connections the run actually uses —
// an unused connection's file is never opened.
func TestCollectRunSecrets_ScopesFileReadsToUsedConnections(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	usedFile := dir + "/used.json"
	unusedFile := dir + "/unused.json"
	if err := os.WriteFile(usedFile, []byte("USED_FILE_SECRET"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(unusedFile, []byte("UNUSED_FILE_SECRET"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		SelectedEnvironment: &config.Environment{
			Connections: &config.Connections{
				GoogleCloudPlatform: []config.GoogleCloudPlatformConnection{
					{ConnectionMetadata: config.ConnectionMetadata{Name: "used-gcp"}, ServiceAccountJSON: "USED_INLINE", ServiceAccountFile: usedFile},
					{ConnectionMetadata: config.ConnectionMetadata{Name: "unused-gcp"}, ServiceAccountJSON: "UNUSED_INLINE", ServiceAccountFile: unusedFile},
				},
			},
		},
	}
	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name:       "a",
				Type:       pipeline.AssetTypeIngestr,
				Connection: "used-gcp",
				Parameters: pipeline.ParameterMap{"source_connection": "used-gcp"},
			},
		},
	}
	connMgr := fakeConnGetter{conns: map[string]any{
		"used-gcp": &config.GoogleCloudPlatformConnection{ConnectionMetadata: config.ConnectionMetadata{Name: "used-gcp"}, ServiceAccountJSON: "USED_INLINE", ServiceAccountFile: usedFile},
	}}

	secrets, unreadable := collectRunSecrets(p, cfg, connMgr)
	got := map[string]bool{}
	for _, s := range secrets {
		got[s] = true
	}

	// Inline secrets from every configured connection (used and unused).
	for _, want := range []string{"USED_INLINE", "UNUSED_INLINE"} {
		if !got[want] {
			t.Errorf("inline secret %q not collected; got %v", want, secrets)
		}
	}
	// The used connection's credential file is read and collected.
	if !got["USED_FILE_SECRET"] {
		t.Errorf("used connection's file contents not collected; got %v", secrets)
	}
	// The unused connection's file is never read.
	if got["UNUSED_FILE_SECRET"] {
		t.Errorf("unused connection's file was read; it should be skipped; got %v", secrets)
	}
	if len(unreadable) != 0 {
		t.Errorf("expected no unreadable files, got %v", unreadable)
	}
}

// TestCollectRunSecrets_ReportsUnreadableUsedFile proves a used connection whose
// credential file cannot be read is reported (not silently dropped).
func TestCollectRunSecrets_ReportsUnreadableUsedFile(t *testing.T) {
	t.Parallel()
	missing := t.TempDir() + "/missing.json" // absolute, does not exist
	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name:       "a",
				Type:       pipeline.AssetTypeIngestr,
				Connection: "gcp",
				Parameters: pipeline.ParameterMap{"source_connection": "gcp"},
			},
		},
	}
	connMgr := fakeConnGetter{conns: map[string]any{
		"gcp": &config.GoogleCloudPlatformConnection{ConnectionMetadata: config.ConnectionMetadata{Name: "gcp"}, ServiceAccountFile: missing},
	}}

	secrets, unreadable := collectRunSecrets(p, nil, connMgr)
	if len(secrets) != 0 {
		t.Errorf("expected no secrets for missing file, got %v", secrets)
	}
	if len(unreadable) != 1 || unreadable[0] != missing {
		t.Errorf("expected unreadable [%s], got %v", missing, unreadable)
	}
}

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
