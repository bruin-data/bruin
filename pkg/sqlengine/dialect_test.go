package sqlengine

import "testing"

// TestUnknownDialectSuggestion checks that an unknown dialect name suggests only registered
// dialects.
func TestUnknownDialectSuggestion(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"snowflak":      "Unknown dialect 'snowflak'. Did you mean snowflake?",
		"postgress":     "Unknown dialect 'postgress'. Did you mean postgres?",
		"teradat":       "Unknown dialect 'teradat'.",
		"not-a-dialect": "Unknown dialect 'not-a-dialect'.",
	}
	for name, want := range cases {
		_, err := GetDialect(name)
		if err == nil || err.Error() != want {
			t.Errorf("GetDialect(%q) error = %v, want %q", name, err, want)
		}
	}
}
