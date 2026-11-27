package sqlengine

import "testing"

// TestClickHouseStringEscapes checks how ClickHouse string literals are decoded and written back:
// \xHH escapes are bytes, so runs that form UTF-8 decode as one character and any other byte above
// 0x7F stays that byte, written back as \xHH; \e, \N, \/, \", \=, \` and a backslash before a control character
// are decoded too, while unknown escapes and short \x escapes keep their backslash. Control
// characters are written as \xHH unless they have a named escape.
func TestClickHouseStringEscapes(t *testing.T) {
	t.Parallel()
	cases := []struct{ sql, want, value string }{
		{`SELECT * FROM t WHERE name = '\x41\x42'`, `SELECT * FROM t WHERE name = 'AB'`, "AB"},
		{`SELECT 'abc\x41'`, `SELECT 'abcA'`, "abcA"},
		{`SELECT '\x414'`, `SELECT 'A4'`, "A4"},
		{`SELECT '\xC3\xA9'`, `SELECT 'é'`, "é"},
		{`SELECT '\xE2\x82\xAC'`, `SELECT '€'`, "€"},
		{`SELECT '\xF0\x9F\x98\x80'`, `SELECT '😀'`, "😀"},
		{`SELECT '\xC3'`, `SELECT '\xc3'`, "\xc3"},
		{`SELECT '\xE2\x82'`, `SELECT '\xe2\x82'`, "\xe2\x82"},
		{`SELECT '\xc3\x28'`, `SELECT '\xc3('`, "\xc3("},
		{`SELECT '\xFF'`, `SELECT '\xff'`, "\xff"},
		{`SELECT '\x80'`, `SELECT '\x80'`, "\x80"},
		{`SELECT 'a\xC3\xA9\xFFb'`, `SELECT 'aé\xffb'`, "aé\xffb"},
		{`SELECT '\xFF\xC3\xA9'`, `SELECT '\xffé'`, "\xffé"},
		{`SELECT '\x27'`, `SELECT ''''`, "'"},
		{`SELECT '\x5C'`, `SELECT '\\'`, `\`},
		{`SELECT '\x00'`, `SELECT '\0'`, "\x00"},
		{`SELECT '\x01'`, `SELECT '\x01'`, "\x01"},
		{`SELECT '\x0a'`, `SELECT '\n'`, "\n"},
		{`SELECT '\x1b'`, `SELECT '\e'`, "\x1b"},
		{`SELECT '\x7f'`, `SELECT '\x7f'`, "\x7f"},
		{"SELECT 'a\x01b'", `SELECT 'a\x01b'`, "a\x01b"},
		{`SELECT '\x4'`, `SELECT '\\x4'`, `\x4`},
		{`SELECT '\xZZ'`, `SELECT '\\xZZ'`, `\xZZ`},
		{`SELECT '\X41'`, `SELECT '\\X41'`, `\X41`},
		{`SELECT '\\x41'`, `SELECT '\\x41'`, `\x41`},
		{`SELECT '\e'`, `SELECT '\e'`, "\x1b"},
		{`SELECT '\N'`, `SELECT ''`, ""},
		{`SELECT 'a\Nb'`, `SELECT 'ab'`, "ab"},
		{`SELECT '\\N'`, `SELECT '\\N'`, `\N`},
		{`SELECT '\/'`, `SELECT '/'`, "/"},
		{`SELECT 'q\"q'`, `SELECT 'q"q'`, `q"q`},
		{`SELECT '\='`, `SELECT '='`, "="},
		{"SELECT '\\`'", "SELECT '`'", "`"},
		{"SELECT 'a\\\tb'", `SELECT 'a\tb'`, "a\tb"},
		{"SELECT 'a\\\x01b'", `SELECT 'a\x01b'`, "a\x01b"},
		{`SELECT '\0'`, `SELECT '\0'`, "\x00"},
		{`SELECT '\z'`, `SELECT '\\z'`, `\z`},
		{`SELECT 'it\'s'`, `SELECT 'it''s'`, "it's"},
		{`SELECT 'a' LIKE '\x25'`, `SELECT 'a' LIKE '%'`, "a"},
	}
	d := MustDialect("clickhouse")
	for _, c := range cases {
		e, err := d.ParseOne(c.sql, nil)
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		lit := e.Find(KLiteral)
		if lit == nil || !lit.IsString() {
			t.Errorf("%s: no string literal", c.sql)
		} else if got := lit.ThisS(); got != c.value {
			t.Errorf("%s: decoded %q, want %q", c.sql, got, c.value)
		}
		if got, err := d.Generate(e, nil); err != nil || got != c.want {
			t.Errorf("%s:\n got %s (%v)\nwant %s", c.sql, got, err, c.want)
		}
	}

	// Identifiers decode the same escapes; heredoc strings are raw, so theirs are not decoded.
	for sql, want := range map[string]string{
		`SELECT $$\x41$$`:         `SELECT '\\x41'`,
		`SELECT 1 AS "a\x41"`:     `SELECT 1 AS "aA"`,
		"SELECT 1 AS `a\\x41`":    `SELECT 1 AS "aA"`,
		`SELECT 1 AS "a\xC3\xA9"`: `SELECT 1 AS "aé"`,
		`SELECT 1 AS "a\xFF"`:     `SELECT 1 AS "a\xff"`,
		`SELECT 1 AS "a\Nb"`:      `SELECT 1 AS "ab"`,
	} {
		if got := transpileOne(t, "clickhouse", "clickhouse", sql); got != want {
			t.Errorf("%s:\n got %s\nwant %s", sql, got, want)
		}
	}
}
