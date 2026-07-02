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
			name: "base64 secret in a path segment",
			in:   "scheme://host/" + url.PathEscape(base64.StdEncoding.EncodeToString([]byte(saJSON))) + "/x",
			want: "host/" + Mask + "/x",
			gone: url.PathEscape(base64.StdEncoding.EncodeToString([]byte(saJSON))),
		},
		{
			name: "base64 secret in userinfo",
			in:   "scheme://" + strings.TrimPrefix(url.UserPassword("", base64.StdEncoding.EncodeToString([]byte(saJSON))).String(), ":") + "@host",
			want: Mask + "@host",
			gone: strings.TrimPrefix(url.UserPassword("", base64.StdEncoding.EncodeToString([]byte(saJSON))).String(), ":"),
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
	if err := w.Flush(); err != nil {
		t.Fatal(err)
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

func TestWriterMultiLineSecretSplitAcrossWrites(t *testing.T) {
	t.Parallel()
	secret := "-----BEGIN PRIVATE KEY-----\nLINE1abc\nLINE2def\n-----END PRIVATE KEY-----"
	r := New([]string{secret})
	var buf bytes.Buffer
	w := r.Writer(&buf)
	// Split the secret in the middle, across two writes, as a pipe read would.
	full := "key=" + secret + "\n"
	mid := len("key=" + "-----BEGIN PRIVATE KEY-----\nLINE1abc")
	for _, chunk := range []string{full[:mid], full[mid:]} {
		if _, err := w.Write([]byte(chunk)); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, frag := range []string{"LINE1abc", "LINE2def", "BEGIN PRIVATE KEY"} {
		if strings.Contains(buf.String(), frag) {
			t.Errorf("split multi-line secret leaked %q: %s", frag, buf.String())
		}
	}
	if !strings.Contains(buf.String(), Mask) {
		t.Errorf("mask not present: %s", buf.String())
	}
}

func TestWriterLongSecretSplitAcrossWrites(t *testing.T) {
	t.Parallel()
	// A long single-line secret (e.g. a base64 token) split into many small
	// chunks, as io.Copy from the pipe would deliver it.
	secret := "sk_live_" + strings.Repeat("aB3xZ9qP", 40) // ~328 bytes, no newlines
	r := New([]string{secret})
	var buf bytes.Buffer
	w := r.Writer(&buf)
	line := "Authorization: Bearer " + secret + "\n"
	for i := 0; i < len(line); i += 7 { // 7-byte chunks straddle the secret repeatedly
		end := i + 7
		if end > len(line) {
			end = len(line)
		}
		if _, err := w.Write([]byte(line[i:end])); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), secret) || strings.Contains(buf.String(), secret[:40]) {
		t.Errorf("long secret leaked across chunked writes: %s", buf.String())
	}
	if !strings.Contains(buf.String(), "Bearer "+Mask) {
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

// userinfoForm renders a secret the way it appears in a user:pass@host authority.
func userinfoForm(secret string) string {
	return strings.TrimPrefix(url.UserPassword("", secret).String(), ":")
}

// TestMaskEachFormType documents and verifies every shape a secret can take in
// output: the {raw, base64} x {plain, query-, path-, userinfo-escaped} matrix.
func TestMaskEachFormType(t *testing.T) {
	t.Parallel()
	// Characters that encode differently in each position (space, @, /, +, =).
	secret := "p@ss w/rd+x=y"
	b64 := base64.StdEncoding.EncodeToString([]byte(secret))
	r := New([]string{secret})

	cases := []struct {
		name  string
		shape string // secret rendered in this form, embedded in a URI-ish string
	}{
		{"raw verbatim", "token=" + secret},
		{"query-escaped", "conn://?password=" + url.QueryEscape(secret) + "&x=1"},
		{"path-escaped", "conn://host/" + url.PathEscape(secret) + "/tail"},
		{"userinfo user:pass@", "conn://user:" + userinfoForm(secret) + "@host"},
		{"base64", "conn://?credentials_base64=" + b64},
		{"base64 query-escaped", "conn://?credentials_base64=" + url.QueryEscape(b64) + "&x=1"},
		{"base64 path-escaped", "conn://host/" + url.PathEscape(b64) + "/tail"},
		{"base64 userinfo", "conn://user:" + userinfoForm(b64) + "@host"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := r.Mask(tc.shape)
			if !strings.Contains(out, Mask) {
				t.Errorf("%s: no mask in %q", tc.name, out)
			}
			if strings.Contains(out, secret) {
				t.Errorf("%s: raw secret leaked in %q", tc.name, out)
			}
			if strings.Contains(out, b64) {
				t.Errorf("%s: base64 secret leaked in %q", tc.name, out)
			}
		})
	}
}

// TestMaskCoversEveryGeneratedForm is the exhaustive invariant: for several kinds
// of secret, every form New()/forms() produces must actually be redacted by Mask.
// If forms() ever emits a shape Mask can't match, this fails.
func TestMaskCoversEveryGeneratedForm(t *testing.T) {
	t.Parallel()
	secrets := []string{
		"p@ss w/rd+x=y",  // special characters
		"sk_live_ABC123", // plain token
		`{"type":"service_account","private_key":"KEYMATERIAL"}`,                // inline json
		"-----BEGIN PRIVATE KEY-----\nMIIabc\ndef==\n-----END PRIVATE KEY-----", // multiline pem
	}
	for _, secret := range secrets {
		r := New([]string{secret})
		for i, f := range forms(secret) {
			in := "left|" + f + "|right"
			out := r.Mask(in)
			if strings.Contains(out, f) {
				t.Errorf("form %d %q not masked (secret %q) -> %q", i, f, secret, out)
			}
			if !strings.Contains(out, Mask) {
				t.Errorf("form %d of secret %q produced no mask -> %q", i, secret, out)
			}
		}
	}
}

// TestSensitiveValuesInputMethods covers every way a credential is provided:
// raw inline, base64 inline, inline JSON, a file path whose contents are the
// secret, and a pass-through path that must NOT be collected.
func TestSensitiveValuesInputMethods(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	saFile := dir + "/sa.json"
	saFileContents := `{"type":"service_account","private_key":"FILE_KEY_MATERIAL"}`
	if err := os.WriteFile(saFile, []byte(saFileContents), 0o600); err != nil {
		t.Fatal(err)
	}

	type nested struct {
		Password string `mapstructure:"password" sensitive:"true"`
	}
	type conn struct {
		Name      string  `mapstructure:"name"`                                       // non-secret
		Host      string  `mapstructure:"host"`                                       // non-secret
		Password  string  `mapstructure:"password" sensitive:"true"`                  // raw inline
		KeyBase64 string  `mapstructure:"key_base64" sensitive:"true"`                // base64 inline
		SAJSON    string  `mapstructure:"service_account_json" sensitive:"true"`      // inline json
		SAFile    string  `mapstructure:"service_account_file" sensitive_file:"true"` // path -> contents
		KeyPath   string  `mapstructure:"key_path"`                                   // pass-through path
		Token     string  `mapstructure:"token" sensitive:"true"`                     // empty -> skipped
		Nested    nested  `mapstructure:"nested"`                                     // nested struct
		Ptr       *nested `mapstructure:"ptr"`                                        // pointer to struct
	}
	base64Val := base64.StdEncoding.EncodeToString([]byte("DECODED_KEY"))
	c := &conn{
		Name: "prod", Host: "db.example.com",
		Password:  "RAW_PASSWORD",
		KeyBase64: base64Val,
		SAJSON:    `{"type":"service_account","private_key":"INLINE_KEY_MATERIAL"}`,
		SAFile:    saFile,
		KeyPath:   "/etc/keys/app.p8",
		Token:     "",
		Nested:    nested{Password: "NESTED_PASSWORD"},
		Ptr:       &nested{Password: "PTR_PASSWORD"},
	}
	got := map[string]bool{}
	for _, v := range SensitiveValues(c) {
		got[v] = true
	}

	for _, want := range []string{
		"RAW_PASSWORD", // raw inline
		base64Val,      // base64 inline (stored value)
		`{"type":"service_account","private_key":"INLINE_KEY_MATERIAL"}`, // inline json
		saFileContents,    // file contents
		"NESTED_PASSWORD", // nested struct
		"PTR_PASSWORD",    // pointer
	} {
		if !got[want] {
			t.Errorf("expected sensitive value not collected: %q; got %v", want, got)
		}
	}
	for _, notWant := range []string{
		"prod",             // name
		"db.example.com",   // host
		"/etc/keys/app.p8", // pass-through key_path (bruin never reads it)
		"",                 // empty
	} {
		if got[notWant] {
			t.Errorf("non-secret value wrongly collected: %q", notWant)
		}
	}
}

// TestSensitiveValuesContainers verifies collection recurses through the
// slice/map/pointer shapes that the real config uses.
func TestSensitiveValuesContainers(t *testing.T) {
	t.Parallel()
	type conn struct {
		Password string `mapstructure:"password" sensitive:"true"`
	}
	got := map[string]bool{}
	for _, v := range SensitiveValues([]*conn{{Password: "S1"}, {Password: "S2"}, nil}) {
		got[v] = true
	}
	for _, v := range SensitiveValues(map[string]conn{"a": {Password: "S3"}}) {
		got[v] = true
	}
	for _, want := range []string{"S1", "S2", "S3"} {
		if !got[want] {
			t.Errorf("value %q not collected from container; got %v", want, got)
		}
	}
}

// TestMaskConnectionEndToEnd ties collection + forms + masking together: build a
// masker from a realistic connection that mixes every input method, then mask a
// simulated ingestr command echo plus raw-value error lines. Secrets in every
// shape must vanish; non-secret structure must remain.
func TestMaskConnectionEndToEnd(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	keyFile := dir + "/key.pem"
	pemContents := "-----BEGIN PRIVATE KEY-----\nFILE_KEY_MATERIAL\n-----END PRIVATE KEY-----"
	if err := os.WriteFile(keyFile, []byte(pemContents), 0o600); err != nil {
		t.Fatal(err)
	}
	saJSON := "{\n  \"type\": \"service_account\",\n  \"private_key\": \"INLINE_KEY_MATERIAL\"\n}"

	type conn struct {
		Host       string `mapstructure:"host"`
		Password   string `mapstructure:"password" sensitive:"true"`              // raw
		APIKey     string `mapstructure:"api_key" sensitive:"true"`               // raw
		SAJSON     string `mapstructure:"service_account_json" sensitive:"true"`  // inline json
		PrivKeyPth string `mapstructure:"private_key_path" sensitive_file:"true"` // path -> contents
	}
	c := &conn{
		Host:       "db.example.com",
		Password:   "p@ss w/rd",
		APIKey:     "sk_live_XYZ",
		SAJSON:     saJSON,
		PrivKeyPth: keyFile,
	}
	r := New(SensitiveValues(c))

	log := "uv tool run ingestr ingest" +
		" --source-uri postgres://user:" + userinfoForm(c.Password) + "@db.example.com:5432/app" +
		" --dest-uri bigquery://proj?credentials_base64=" + base64.StdEncoding.EncodeToString([]byte(saJSON)) +
		" --api-key " + c.APIKey +
		" --key-b64 " + base64.StdEncoding.EncodeToString([]byte(pemContents)) +
		"\nERROR: failed to parse service account " + saJSON +
		"\nERROR: bad key " + pemContents
	out := r.Mask(log)

	for name, leak := range map[string]string{
		"raw password":         c.Password,
		"userinfo password":    userinfoForm(c.Password),
		"api key":              c.APIKey,
		"inline json":          saJSON,
		"inline key material":  "INLINE_KEY_MATERIAL",
		"base64 json":          base64.StdEncoding.EncodeToString([]byte(saJSON)),
		"raw file contents":    pemContents,
		"file key material":    "FILE_KEY_MATERIAL",
		"base64 file contents": base64.StdEncoding.EncodeToString([]byte(pemContents)),
	} {
		if strings.Contains(out, leak) {
			t.Errorf("%s leaked:\n%s", name, out)
		}
	}
	for _, visible := range []string{
		"ingestr ingest",
		"postgres://user:" + Mask + "@db.example.com:5432/app",
		"bigquery://proj?credentials_base64=" + Mask,
		"--api-key " + Mask,
	} {
		if !strings.Contains(out, visible) {
			t.Errorf("expected %q visible in:\n%s", visible, out)
		}
	}
}
