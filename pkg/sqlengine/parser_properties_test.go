package sqlengine

import "testing"

// TestPropertyModifierRejected checks that a property keyword given a modifier it does not take
// (NO LOCK, DEFAULT ENGINE, ...) is a parse error naming the property.
func TestPropertyModifierRejected(t *testing.T) {
	t.Parallel()
	cases := []struct{ dialect, sql, want string }{
		{"", "CREATE TABLE t, NO LOCK (a INT)", "Cannot parse property 'LOCK'. Line 1, Col: 25."},
		{"mysql", "CREATE TABLE t, NO LOCK (a INT)", "Cannot parse property 'LOCK'. Line 1, Col: 25."},
		{"", "CREATE TABLE t, NO ENGINE=x (a INT)", "Cannot parse property 'ENGINE'. Line 1, Col: 26."},
		{"", "CREATE TABLE t (a INT) DEFAULT ENGINE=x", "Cannot parse property 'ENGINE'. Line 1, Col: 38."},
		{"mysql", "CREATE TABLE t (a INT) DEFAULT LOCK", "Cannot parse property 'LOCK'. Line 1, Col: 35."},
		{"doris", "CREATE TABLE t (a INT) DEFAULT PROPERTIES ('k'='v')", "Cannot parse property 'PROPERTIES'. Line 1, Col: 43."},
	}
	for _, c := range cases {
		_, err := MustDialect(c.dialect).ParseOne(c.sql, nil)
		if _, ok := err.(*ParseError); !ok {
			t.Errorf("[%s] %s: got %T %v, want a ParseError", c.dialect, c.sql, err, err)
			continue
		}
		if got := firstLine(err.Error()); got != c.want {
			t.Errorf("[%s] %s:\n got %s\nwant %s", c.dialect, c.sql, got, c.want)
		}
	}

	// Modifiers a property does take still parse.
	for sql, want := range map[string]string{
		"CREATE TABLE t, NO FALLBACK (a INT)":                 "CREATE TABLE t (a INT)",
		"CREATE TABLE t (a INT) DEFAULT CHARACTER SET = utf8": "CREATE TABLE t (a INT) DEFAULT CHARACTER SET=utf8",
	} {
		d := MustDialect("")
		e, err := d.ParseOne(sql, nil)
		if err != nil {
			t.Errorf("%s: %v", sql, err)
			continue
		}
		if got, err := d.Generate(e, nil); err != nil || got != want {
			t.Errorf("%s: got %q (%v), want %q", sql, got, err, want)
		}
	}
}
