package cmd

import (
	"os"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
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
