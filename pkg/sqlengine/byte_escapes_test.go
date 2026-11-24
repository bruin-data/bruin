package sqlengine

import "testing"

// TestRawByteEscapes checks that a byte escape that is not part of a UTF-8 character is kept as
// that byte and written back in an escape the target dialect reads as the same byte: \xHH, or
// \OOO for Redshift, which reads only octal escapes.
func TestRawByteEscapes(t *testing.T) {
	t.Parallel()
	cases := []struct{ read, write, sql, want, value string }{
		{"clickhouse", "clickhouse", `SELECT '\xFF'`, `SELECT '\xff'`, "\xff"},
		{"clickhouse", "redshift", `SELECT '\xFF'`, `SELECT '\377'`, "\xff"},
		{"redshift", "redshift", `SELECT '\377'`, `SELECT '\377'`, "\xff"},
		{"redshift", "redshift", `SELECT 'a\3777'`, `SELECT 'a\3777'`, "a\xff7"},
		{"redshift", "redshift", `SELECT '\303\251\200'`, `SELECT 'é\200'`, "é\x80"},
		{"redshift", "clickhouse", `SELECT '\377'`, `SELECT '\xff'`, "\xff"},
		{"postgres", "postgres", `SELECT E'\xFF'`, `SELECT e'\xff'`, "\xff"},
		{"postgres", "postgres", `SELECT E'\377'`, `SELECT e'\xff'`, "\xff"},
	}
	for _, c := range cases {
		e, err := MustDialect(c.read).ParseOne(c.sql, nil)
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		if got := e.Selects()[0].ThisS(); got != c.value {
			t.Errorf("%s: decoded %q, want %q", c.sql, got, c.value)
		}
		out, err := MustDialect(c.write).Generate(e, nil)
		if err != nil || out != c.want {
			t.Errorf("%s (%s -> %s):\n got %s (%v)\nwant %s", c.sql, c.read, c.write, out, err, c.want)
			continue
		}
		again, err := MustDialect(c.write).ParseOne(out, nil)
		if err != nil {
			t.Errorf("%s: re-parse: %v", out, err)
		} else if got := again.Selects()[0].ThisS(); got != c.value {
			t.Errorf("%s: re-parsed %q, want %q", out, got, c.value)
		}
	}
}
