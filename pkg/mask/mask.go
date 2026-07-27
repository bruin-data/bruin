// Package mask redacts credential values from log output by searching for each
// secret in every form it can appear as (raw, escaped, base64) and replacing it.
package mask

import (
	"encoding/base64"
	"io"
	"net/url"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
)

// Mask is the placeholder written in place of a credential value.
const Mask = "****"

// forms returns the distinct string forms a secret can appear as in output.
func forms(secret string) []string {
	seen := map[string]struct{}{}
	var out []string
	add := func(s string) {
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	b64 := base64.StdEncoding.EncodeToString([]byte(secret))
	// The full matrix of {raw, base64} x {plain, query-, path-, userinfo-escaped},
	// so a secret is masked in any URI position whether or not it was base64'd.
	add(secret)
	add(url.QueryEscape(secret))
	add(url.PathEscape(secret))
	add(strings.TrimPrefix(url.UserPassword("", secret).String(), ":"))
	add(b64)
	add(url.QueryEscape(b64))
	add(url.PathEscape(b64))
	add(strings.TrimPrefix(url.UserPassword("", b64).String(), ":"))
	return out
}

// SensitiveValues returns inline `sensitive:"true"` values and the CONTENTS of
// `sensitive_file:"true"` paths in conn; unreadable lists set-but-unreadable paths.
func SensitiveValues(conn any) (values, unreadable []string) {
	c := collector{readFiles: true}
	c.walk(reflect.ValueOf(conn))
	return c.values, c.unreadable
}

// InlineSensitiveValues returns only inline `sensitive:"true"` values, without
// reading any sensitive_file paths — used to mask every configured connection
// cheaply while leaving credential files to the used-connection pass.
func InlineSensitiveValues(conn any) []string {
	c := collector{readFiles: false}
	c.walk(reflect.ValueOf(conn))
	return c.values
}

type collector struct {
	readFiles  bool
	values     []string
	unreadable []string
}

func (c *collector) walk(v reflect.Value) {
	switch v.Kind() { //nolint:exhaustive
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			c.walk(v.Elem())
		}
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			c.walk(v.Index(i))
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			c.walk(v.MapIndex(k))
		}
	case reflect.Struct:
		t := v.Type()
		for i := range t.NumField() {
			field := t.Field(i)
			if !field.IsExported() {
				continue
			}
			fv := v.Field(i)
			if fv.Kind() == reflect.String {
				c.sl(field, fv.String())
				continue // strings never hold nested structs
			}
			if fv.Kind() == reflect.Map && field.Tag.Get("sensitive") == "true" {
				c.sensitiveMap(field, fv)
				continue
			}
			c.walk(fv)
		}
	}
}

// credentialKeyMarkers name the option keys whose values are treated as secrets
// inside a map tagged sensitive:"true". They are matched against the key with
// every separator removed, so they carry none themselves: driver option
// namespaces mix dots, hyphens and underscores (`fs.azure.account.key.…`,
// `spark.ingest.s3.access-key`), and a separator-sensitive marker would miss
// most real spellings.
var credentialKeyMarkers = []string{
	"password", "passwd", "pwd", "passphrase", "secret", "token", "credential",
	"apikey", "accountkey", "accesskey", "privatekey", "sessionkey", "signature",
}

// minCredentialLength is the shortest map value that is treated as a secret.
// Masking is a plain find-and-replace over the whole run output, so a short
// value such as "true" or "none" — plausible even under a credential-shaped key
// like `…token_enabled` — would redact an ordinary word from every log line.
const minCredentialLength = 8

// sensitiveMap collects the credential values of a map field tagged
// sensitive:"true".
//
// Only values whose key names a credential are collected. Such maps hold
// arbitrary driver options, most of which are ordinary settings, and collecting
// a value like "true" from `spark.ingest.s3.use_path_style` would redact every
// occurrence of that substring in every log line.
func (c *collector) sensitiveMap(field reflect.StructField, m reflect.Value) {
	for _, key := range m.MapKeys() {
		value := m.MapIndex(key)
		if value.Kind() != reflect.String {
			c.walk(value) // a nested struct may carry its own sensitive fields
			continue
		}
		if key.Kind() != reflect.String || len(value.String()) < minCredentialLength {
			continue
		}
		name := stripKeySeparators(key.String())
		for _, marker := range credentialKeyMarkers {
			if strings.Contains(name, marker) {
				c.sl(field, value.String())
				break
			}
		}
	}
}

// stripKeySeparators lowercases an option key and drops everything that is not
// a letter or a digit, so `FS.Azure.Account.Key` and `access-key` normalize to
// the separator-free forms the markers are written in.
func stripKeySeparators(key string) string {
	var b strings.Builder
	b.Grow(len(key))
	for _, r := range strings.ToLower(key) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func (c *collector) sl(field reflect.StructField, s string) {
	if s == "" {
		return
	}
	// Inline secret value.
	if field.Tag.Get("sensitive") == "true" {
		c.values = append(c.values, s)
	}
	// Path whose file CONTENTS are the secret (service_account_file,
	// private_key_path); only read when this pass reads files.
	if c.readFiles && field.Tag.Get("sensitive_file") == "true" {
		c.readSecretFile(s)
	}
}

// readSecretFile reads a sensitive_file path in full, as stored — matching the
// embedder, so any-size credentials are masked. Unreadable paths are recorded.
func (c *collector) readSecretFile(path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		c.unreadable = append(c.unreadable, path)
		return
	}
	if len(b) > 0 {
		c.values = append(c.values, string(b))
	}
}

// Masker masks a fixed set of secret forms in arbitrary text.
type Masker struct {
	ordered []string // all secret forms, longest-first
	maxLen  int      // length of the longest form
}

// New builds a Masker from raw secret values, expanding each into the forms
// it can appear as (raw, query/path/userinfo-escaped, base64).
func New(values []string) *Masker {
	seen := map[string]struct{}{}
	var ordered []string
	for _, v := range values {
		for _, f := range forms(v) {
			if _, ok := seen[f]; ok {
				continue
			}
			seen[f] = struct{}{}
			ordered = append(ordered, f)
		}
	}
	// Longest first so a longer form is replaced before any shorter overlap.
	sort.Slice(ordered, func(i, j int) bool { return len(ordered[i]) > len(ordered[j]) })
	maxLen := 0
	if len(ordered) > 0 {
		maxLen = len(ordered[0])
	}
	return &Masker{ordered: ordered, maxLen: maxLen}
}

// Empty reports whether there is nothing to mask.
func (r *Masker) Empty() bool { return r == nil || len(r.ordered) == 0 }

// Mask replaces every known secret form found in s with the placeholder.
func (r *Masker) Mask(s string) string {
	if r.Empty() || s == "" {
		return s
	}
	for _, f := range r.ordered {
		if strings.Contains(s, f) {
			s = strings.ReplaceAll(s, f, Mask)
		}
	}
	return s
}

// Writer wraps w in a masking writer. Call Flush once writing is done to emit
// the retained trailing bytes.
func (r *Masker) Writer(w io.Writer) *LineWriter {
	return &LineWriter{r: r, w: w}
}

// LineWriter masks output, always holding back a trailing window the width of the
// longest secret form so a secret split across writes is masked whole, not leaked.
type LineWriter struct {
	r   *Masker
	w   io.Writer
	mu  sync.Mutex
	buf []byte
}

func (lw *LineWriter) Write(p []byte) (int, error) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	lw.buf = append(lw.buf, p...)
	// Mask the whole buffer and emit all but a trailing window that could still be
	// the start of a later secret. Retained bytes are masked; Mask is idempotent.
	masked := []byte(lw.r.Mask(string(lw.buf)))
	keep := lw.r.maxLen - 1
	if keep < 0 {
		keep = 0
	}
	if len(masked) > keep {
		if _, err := lw.w.Write(masked[:len(masked)-keep]); err != nil {
			return 0, err
		}
		lw.buf = append(lw.buf[:0], masked[len(masked)-keep:]...)
	} else {
		lw.buf = append(lw.buf[:0], masked...)
	}
	return len(p), nil
}

// Flush masks and writes the retained trailing bytes.
func (lw *LineWriter) Flush() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if len(lw.buf) == 0 {
		return nil
	}
	_, err := io.WriteString(lw.w, lw.r.Mask(string(lw.buf)))
	lw.buf = lw.buf[:0]
	return err
}
