package cmd

import (
	"testing"
)

func TestLevenshteinDistance(t *testing.T) {
	tests := []struct {
		a        string
		b        string
		expected int
	}{
		{"", "", 0},
		{"", "abc", 3},
		{"abc", "", 3},
		{"abc", "abc", 0},
		{"kitten", "sitting", 3},
		{"run", "rnu", 2},
		{"render", "rendor", 1},
		{"format", "fromat", 2},
		{"help", "hlep", 2},
	}

	for _, test := range tests {
		result := levenshteinDistance(test.a, test.b)
		if result != test.expected {
			t.Errorf("levenshteinDistance(%q, %q) = %d; expected %d", test.a, test.b, result, test.expected)
		}
	}
}

func TestSuggestCommand(t *testing.T) {
	validCommands := []string{
		"validate", "run", "render", "lineage", "clean", "format",
		"docs", "init", "environments", "query", "patch", "data-diff",
		"diff", "import", "version", "help", "h",
	}

	tests := []struct {
		input     string
		threshold int
		expected  string
	}{
		{"rnu", 2, "run"},
		{"rendor", 2, "render"},
		{"fromat", 2, "format"},
		{"hlep", 2, "help"},
		{"inicialize", 2, ""}, // too far from "init"
		{"xyz", 2, ""},        // no close match
		{"hel", 2, "help"},
		{"runn", 2, "run"},
		{"cleann", 2, "clean"},
		{"patc", 2, "patch"},
		{"environmen", 2, "environments"}, // distance 2, within threshold
		{"environmet", 2, "environments"}, // distance 2, within threshold  
		{"environ", 2, ""},    // distance 4, exceeds threshold
		{"ver", 2, ""},        // distance 4 from "version", exceeds threshold
		{"versio", 2, "version"},
	}

	for _, test := range tests {
		result := suggestCommand(test.input, validCommands, test.threshold)
		if result != test.expected {
			t.Errorf("suggestCommand(%q, validCommands, %d) = %q; expected %q", 
				test.input, test.threshold, result, test.expected)
		}
	}
}

func TestSuggestCommandWithDifferentThresholds(t *testing.T) {
	validCommands := []string{"run", "render", "format"}

	tests := []struct {
		input     string
		threshold int
		expected  string
	}{
		{"rnu", 1, ""},     // distance 2, exceeds threshold 1
		{"rnu", 2, "run"},  // distance 2, within threshold 2
		{"rnu", 3, "run"},  // distance 2, within threshold 3
		{"xyz", 3, "run"},  // distance 3, finds closest match
	}

	for _, test := range tests {
		result := suggestCommand(test.input, validCommands, test.threshold)
		if result != test.expected {
			t.Errorf("suggestCommand(%q, validCommands, %d) = %q; expected %q", 
				test.input, test.threshold, result, test.expected)
		}
	}
}