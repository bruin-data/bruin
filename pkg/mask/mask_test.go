package mask

import (
	"bytes"
	"encoding/base64"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestSensitiveValues(t *testing.T) {
	t.Parallel()
	type conn struct {
		Name     string `mapstructure:"name"`
		Password string `mapstructure:"password" sensitive:"true"`
		Host     string `mapstructure:"host"`
		APIKey   string `mapstructure:"api_key" sensitive:"true"`
		Empty    string `mapstructure:"empty" sensitive:"true"`
		unexp    string //nolint:unused
	}
	got := SensitiveValues(&conn{Name: "c", Password: "p@ss", Host: "h", APIKey: "abc", Empty: "", unexp: "x"})
	want := map[string]bool{"p@ss": true, "abc": true}
	if len(got) != len(want) {
		t.Fatalf("got %v, want keys %v", got, want)
	}
	for _, g := range got {
		if !want[g] {
			t.Errorf("unexpected sensitive value %q", g)
		}
	}
}

func TestMask(t *testing.T) {
	t.Parallel()
	pw := "p@ss/w0rd:x"
	apiKey := "sk_live_ABC123"
	saJSON := `{"type":"service_account","private_key":"-----BEGIN-----"}`
	r := New([]string{pw, apiKey, saJSON})

	tests := []struct {
		name string
		in   string
		want string // substring that must be present
		gone string // substring that must NOT be present
	}{
		{
			name: "authority shape, percent-encoded password",
			in:   "Running: uv ... --source-uri postgres://user:" + url.QueryEscape(pw) + "@host:5432/db",
			want: "postgres://user:" + Mask + "@host",
			gone: url.QueryEscape(pw),
		},
		{
			name: "query-param shape api_key",
			in:   "adjust://?api_key=" + apiKey + "&league=39",
			want: "api_key=" + Mask + "&league=39",
			gone: apiKey,
		},
		{
			name: "base64 credentials param",
			in:   "googleads://?credentials_base64=" + base64.StdEncoding.EncodeToString([]byte(saJSON)) + "&dev_token=x",
			want: "credentials_base64=" + Mask,
			gone: base64.StdEncoding.EncodeToString([]byte(saJSON)),
		},
		{
			name: "loose text verbatim secret",
			in:   "authenticating with token " + apiKey + " now",
			want: "token " + Mask + " now",
			gone: apiKey,
		},
		{
			name: "non-secret query value untouched",
			in:   "kinesis://?region_name=us-east-1",
			want: "region_name=us-east-1",
			gone: "\x00never",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := r.Mask(tt.in)
			if !strings.Contains(out, tt.want) {
				t.Errorf("want %q in output\n got: %s", tt.want, out)
			}
			if tt.gone != "" && strings.Contains(out, tt.gone) {
				t.Errorf("secret %q leaked\n got: %s", tt.gone, out)
			}
		})
	}
}

func TestMaskEmptyMasker(t *testing.T) {
	t.Parallel()
	r := New(nil)
	in := "postgres://user:pass@host"
	if got := r.Mask(in); got != in {
		t.Errorf("empty masker changed output: %q", got)
	}
}

func TestWriter(t *testing.T) {
	t.Parallel()
	r := New([]string{"topsecret"})
	var buf bytes.Buffer
	w := r.Writer(&buf)
	n, err := w.Write([]byte("connecting with topsecret done\n"))
	if err != nil {
		t.Fatal(err)
	}
	if n != len("connecting with topsecret done\n") {
		t.Errorf("Write returned %d, want original length", n)
	}
	if strings.Contains(buf.String(), "topsecret") {
		t.Errorf("secret leaked through writer: %s", buf.String())
	}
	if !strings.Contains(buf.String(), Mask) {
		t.Errorf("mask not present: %s", buf.String())
	}
}

func TestWriterMultiLineSecret(t *testing.T) {
	t.Parallel()
	secret := "-----BEGIN PRIVATE KEY-----\nLINE1abc\nLINE2def\n-----END PRIVATE KEY-----"
	r := New([]string{secret})
	var buf bytes.Buffer
	w := r.Writer(&buf)
	// The whole multi-line secret arrives in one write, terminated by a newline.
	if _, err := w.Write([]byte("key=" + secret + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, frag := range []string{"LINE1abc", "LINE2def"} {
		if strings.Contains(buf.String(), frag) {
			t.Errorf("multi-line secret leaked fragment %q: %s", frag, buf.String())
		}
	}
	if !strings.Contains(buf.String(), Mask) {
		t.Errorf("mask not present: %s", buf.String())
	}
}

func TestSensitiveValues_FileTooLarge(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fpath := dir + "/big.json"
	if err := os.WriteFile(fpath, bytes.Repeat([]byte("x"), maxSecretFileSize+1), 0o600); err != nil {
		t.Fatal(err)
	}
	type conn struct {
		SAFile string `mapstructure:"service_account_file" sensitive_file:"true"`
	}
	if vals := SensitiveValues(&conn{SAFile: fpath}); len(vals) != 0 {
		t.Errorf("oversized file should be skipped, got %d values", len(vals))
	}
}

func TestSensitiveValues_File(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	fpath := dir + "/sa.json"
	if err := os.WriteFile(fpath, []byte("FILE_CONTENTS_SECRET_ABC"), 0o600); err != nil {
		t.Fatal(err)
	}
	type conn struct {
		Name   string `mapstructure:"name"`
		SAFile string `mapstructure:"service_account_file" sensitive_file:"true"`
	}
	c := &conn{Name: "c", SAFile: fpath}
	got := map[string]bool{}
	for _, v := range SensitiveValues(c) {
		got[v] = true
	}
	if !got["FILE_CONTENTS_SECRET_ABC"] {
		t.Errorf("sensitive_file: file contents not collected; got %v", got)
	}
}
