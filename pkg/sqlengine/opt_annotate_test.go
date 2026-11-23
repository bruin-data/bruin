package sqlengine

import "testing"

func TestISODateHelpers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		text             string
		isDate, datetime bool
	}{
		{"2020-01-01", true, true},
		{"20200101", true, true},
		{"2020-01-01 10:00:00", false, true},
		{"2020-01-01T10:00:00.123", false, true},
		{"2020-01-01 10:00:00+05:30", false, true},
		{"2020-01-01T10:00", false, true},
		{"2020-01-01T10", false, true},
		{"2020-W01", true, true},
		{"2020W01", true, true},
		{"2020-W01-1", true, true},
		{"2020W011", true, true},
		{"2020-W53-1", true, true},
		{"2021-W53-1", false, false},
		{"2020-13-01", false, false},
		{"2020-02-30", false, false},
		{"2020-02-29", true, true},
		{"2021-02-29", false, false},
		{"abc", false, false},
		{"", false, false},
		{"2020", false, false},
		{"2020-1-1", false, false},
		{"2020-01-1", false, false},
		{"20200101ab", true, false},
		{"2020W011xx", true, false},
		{"2020-01-01x", false, false},
		{"2020-01-01 24:00:00", false, false},
		{"2020-01-01 23:59:60", false, false},
		{"2020-01-01 10:00:00Z", false, true},
		{"2020-01-01 10:00:00z", false, false},
		{"2020-01-01 10:00:00.", false, false},
		{"2020-01-01 10:00:00,5", false, true},
		{"2020-01-01 10:00:00.1234567", false, true},
		{"2020-01-01 10:00:00.12x", false, false},
		{"2020-01-01 1000", false, true},
		{"2020-01-01 100000", false, true},
		{"2020-01-01 10:0000", false, false},
		{"2020-01-01 10:00:00+0530", false, true},
		{"2020-01-01 10:00:00+05", false, true},
		{"2020-01-01 10:00:00+5", false, false},
		{"2020-01-01 10:00:00-24:00", false, false},
		{"2020-01-01 10:00:00+23:59:59.999999", false, true},
		{"2020-01-01T10:00:00+00:00", false, true},
		{"0000-01-01", false, false},
		{"9999-12-31", true, true},
		{"2020-01-01  10:00", false, false},
		{"2020-01-01\u00e910:00", false, true},
		{"\u0662\u0660\u0662\u0660-01-01", false, false},
		{"2020-01-01 10.5", false, true},
		{"2020-01-01 10:00.5", false, true},
		{"2020-01-01 10:00:00 ", false, false},
		{"2020-01-01T", false, false},
		{"2020-01-01T1", false, false},
		{"2020-01-0110:00", false, false},
		{"2020-W011", false, false},
		{"2020-W01-", false, false},
		{"2020-W01-1T10:00", false, true},
		{"2020W0110:00", false, false},
		{"2020W01T10", false, true},
		{"2020-001", false, false},
		{"2020001", false, false},
		{"1999-12-31 23:59:59.999999", false, true},
		{"2020-01-01 10:00:00+", false, false},
		{"2020-01-01 10:00:00+05:", false, false},
		{"2020-01-01 10:00:00+05:3", false, false},
		{"2020-01-01 10:00:00Z+", false, false},
		{"2020-01-01 10:-1", false, false},
		{"2020-01-01 1a", false, false},
		{"2020-01-01 10:00:00.000000000000001", false, true},
		{"2020-01-01 10:00:00.000000x", false, false},
	}
	for _, c := range cases {
		if got := annIsISODate(c.text); got != c.isDate {
			t.Errorf("annIsISODate(%q) = %v, want %v", c.text, got, c.isDate)
		}
		if got := annIsISODatetime(c.text); got != c.datetime {
			t.Errorf("annIsISODatetime(%q) = %v, want %v", c.text, got, c.datetime)
		}
	}
}
