// Package mask redacts credential values from log output by searching for each
// secret in every form it can appear as (raw, escaped, base64) and replacing it.
package mask

import (
	"bytes"
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
	add(secret)
	add(url.QueryEscape(secret))
	add(url.PathEscape(secret))
	add(strings.TrimPrefix(url.UserPassword("", secret).String(), ":"))
	add(b64)
	add(url.QueryEscape(b64))
	return out
}

// SensitiveValues returns the non-empty values of every `sensitive:"true"`
// string field in conn, recursing into nested structs, pointers, and slices.
func SensitiveValues(conn any) []string {
	var out []string
	collect(reflect.ValueOf(conn), &out)
	return out
}

func collect(v reflect.Value, out *[]string) {
	switch v.Kind() { //nolint:exhaustive
	case reflect.Pointer, reflect.Interface:
		if !v.IsNil() {
			collect(v.Elem(), out)
		}
	case reflect.Slice, reflect.Array:
		for i := range v.Len() {
			collect(v.Index(i), out)
		}
	case reflect.Map:
		for _, k := range v.MapKeys() {
			collect(v.MapIndex(k), out)
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
				s := fv.String()
				// Inline secret value.
				if s != "" && field.Tag.Get("sensitive") == "true" {
					*out = append(*out, s)
				}
				// Path whose file CONTENTS are the secret (service_account_file,
				// private_key_path): read it, skipping implausibly large files.
				if s != "" && field.Tag.Get("sensitive_file") == "true" {
					if fi, err := os.Stat(s); err == nil && fi.Size() > 0 && fi.Size() <= maxSecretFileSize {
						if b, err := os.ReadFile(s); err == nil && len(b) > 0 {
							*out = append(*out, string(b))
						}
					}
				}
				// Strings never hold nested structs — nothing to recurse into.
				continue
			}
			collect(fv, out)
		}
	}
}

// Masker masks a fixed set of secret forms in arbitrary text.
type Masker struct {
	ordered []string // all secret forms, longest-first
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
	return &Masker{ordered: ordered}
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

// Writer wraps w in a line-buffering masking writer. Call Flush once writing is
// done to emit any buffered partial line.
func (r *Masker) Writer(w io.Writer) *LineWriter {
	return &LineWriter{r: r, w: w}
}

// maxLineBuffer bounds the buffer so output that never includes a line
// terminator cannot grow memory without bound.
const maxLineBuffer = 1 << 20

// LineWriter masks output written through it, flushing everything up to the last
// newline at once (so multi-line secrets are caught) and buffering the partial tail.
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
	// Emit through the last line terminator as one block, so a multi-line secret
	// (PEM key, service-account JSON) is masked as a whole, not leaked line by line.
	if i := bytes.LastIndexAny(lw.buf, "\r\n"); i >= 0 {
		if err := lw.emit(lw.buf[:i+1]); err != nil {
			return 0, err
		}
		lw.buf = append(lw.buf[:0], lw.buf[i+1:]...)
	}
	if len(lw.buf) > maxLineBuffer {
		if err := lw.emit(lw.buf); err != nil {
			return 0, err
		}
		lw.buf = lw.buf[:0]
	}
	return len(p), nil
}

// Flush masks and writes any buffered partial line.
func (lw *LineWriter) Flush() error {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	if len(lw.buf) == 0 {
		return nil
	}
	err := lw.emit(lw.buf)
	lw.buf = lw.buf[:0]
	return err
}

func (lw *LineWriter) emit(line []byte) error {
	_, err := io.WriteString(lw.w, lw.r.Mask(string(line)))
	return err
}
