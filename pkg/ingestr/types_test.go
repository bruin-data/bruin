package ingestr

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/python"
)

func TestNormaliseColumnName(t *testing.T) {
	t.Parallel()
	testCases := map[string]string{
		"CamelCase":        "camel_case",
		"With Space":       "with_space",
		"With  Two  Space": "with_two_space",
		"1 twoThree":       "_1_two_three",
		"danger⛔":          "danger",
		"DateOfBirth":      "date_of_birth",
	}

	for in, out := range testCases {
		result := python.NormalizeColumnName(in)
		if result != out {
			t.Errorf("expected NormalizeColumnName(%q) = %q, was %q", in, out, result)
		}
	}
}
