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

// maxSecretFileSize caps sensitive_file reads; real credential files (keys,
// service-account JSON) are a few KB, so anything larger is skipped.
const maxSecretFileSize = 1 << 20

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
	c := collector{}
	c.walk(reflect.ValueOf(conn))
	return c.values, c.unreadable
}

type collector struct {
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
				c.stringField(field, fv.String())
				continue // strings never hold nested structs
			}
			c.walk(fv)
		}
	}
}

func (c *collector) stringField(field reflect.StructField, s string) {
	if s == "" {
		return
	}
	// Inline secret value.
	if field.Tag.Get("sensitive") == "true" {
		c.values = append(c.values, s)
	}
	// Path whose file CONTENTS are the secret (service_account_file,
	// private_key_path).
	if field.Tag.Get("sensitive_file") == "true" {
		c.readSecretFile(s)
	}
}

// readSecretFile reads a sensitive_file path as stored (matching the embedder),
// recording unreadable paths; empty or over-cap files are skipped.
func (c *collector) readSecretFile(path string) {
	fi, err := os.Stat(path)
	if err != nil {
		c.unreadable = append(c.unreadable, path)
		return
	}
	if fi.Size() == 0 || fi.Size() > maxSecretFileSize {
		return
	}
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 {
		c.unreadable = append(c.unreadable, path)
		return
	}
	c.values = append(c.values, string(b))
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
