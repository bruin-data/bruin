package ingestr

import "testing"

func TestNormaliseColumnName(t *testing.T) {
	t.Parallel()
	testCases := map[string]string{
		"CamelCase":        "camel_case",
		"With Space":       "with_space",
		"With  Two  Space": "with_two_space",
		"1 twoThree":       "_1_two_three",
		"danger⛔":          "danger",
	}

	for in, out := range testCases {
		result := normalizeColumnName(in)
		if result != out {
			t.Errorf("expected normaliseColumnName(%q) = %q, was %q", in, out, result)
		}
	}
}
