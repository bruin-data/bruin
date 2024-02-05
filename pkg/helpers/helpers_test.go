package helpers

import (
	"testing"
)

func TestTrimSuffix(t *testing.T) {
	t.Parallel()
	tests := []struct {
		s      string
		suffix string
		want   string
	}{
		{"hello world", " world", "hello"},
		{"teststring", "ing", "teststr"},
		{"teststring", "none", "teststring"},
		{"", "suffix", ""},
		{"onlysuffix", "onlysuffix", ""},
		{"repeatedsuffixsuffix", "suffix", "repeatedsuffix"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.s, func(t *testing.T) {
			t.Parallel()
			got := TrimSuffix(tt.s, tt.suffix)
			if got != tt.want {
				t.Errorf("TrimSuffix(%q, %q) = %q, want %q", tt.s, tt.suffix, got, tt.want)
			}
		})
	}
}
