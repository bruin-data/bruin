package cmd

import (
	"testing"
)

func TestCalculatePercentageDiff(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		val1      float64
		val2      float64
		tolerance float64
		expected  string
	}{
		{
			name:      "zero difference",
			val1:      100.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "-",
		},
		{
			name:      "positive difference",
			val1:      150.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "50.0%",
		},
		{
			name:      "negative difference",
			val1:      75.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "-25.0%",
		},
		{
			name:      "divide by zero with non-zero numerator",
			val1:      100.0,
			val2:      0.0,
			tolerance: 0.001,
			expected:  "∞%",
		},
		{
			name:      "both values zero",
			val1:      0.0,
			val2:      0.0,
			tolerance: 0.001,
			expected:  "-",
		},
		{
			name:      "small decimal values",
			val1:      1.1,
			val2:      1.0,
			tolerance: 0.001,
			expected:  "10.0%",
		},
		{
			name:      "large values",
			val1:      1000000.0,
			val2:      900000.0,
			tolerance: 0.001,
			expected:  "11.1%",
		},
		{
			name:      "negative values",
			val1:      -50.0,
			val2:      -100.0,
			tolerance: 0.001,
			expected:  "-50.0%",
		},
		{
			name:      "mixed sign values",
			val1:      50.0,
			val2:      -100.0,
			tolerance: 0.001,
			expected:  "-150.0%",
		},
		{
			name:      "difference within tolerance",
			val1:      100.0005,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "<0.001%",
		},
		{
			name:      "difference just above tolerance",
			val1:      100.002,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "0.0%",
		},
		{
			name:      "negative difference within tolerance",
			val1:      99.9995,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "<0.001%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := calculatePercentageDiff(tt.val1, tt.val2, tt.tolerance)
			if result != tt.expected {
				t.Errorf("calculatePercentageDiff(%.6f, %.6f, %.3f) = %q, want %q", tt.val1, tt.val2, tt.tolerance, result, tt.expected)
			}
		})
	}
}

func TestCalculatePercentageDiffInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		val1      int64
		val2      int64
		tolerance float64
		expected  string
	}{
		{
			name:      "zero difference integers",
			val1:      100,
			val2:      100,
			tolerance: 0.001,
			expected:  "-",
		},
		{
			name:      "positive difference integers",
			val1:      150,
			val2:      100,
			tolerance: 0.001,
			expected:  "50.0%",
		},
		{
			name:      "negative difference integers",
			val1:      75,
			val2:      100,
			tolerance: 0.001,
			expected:  "-25.0%",
		},
		{
			name:      "divide by zero with integers",
			val1:      100,
			val2:      0,
			tolerance: 0.001,
			expected:  "∞%",
		},
		{
			name:      "both integers zero",
			val1:      0,
			val2:      0,
			tolerance: 0.001,
			expected:  "-",
		},
		{
			name:      "large integer values",
			val1:      1000000,
			val2:      900000,
			tolerance: 0.001,
			expected:  "11.1%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := calculatePercentageDiffInt(tt.val1, tt.val2, tt.tolerance)
			if result != tt.expected {
				t.Errorf("calculatePercentageDiffInt(%d, %d, %.3f) = %q, want %q", tt.val1, tt.val2, tt.tolerance, result, tt.expected)
			}
		})
	}
}

func TestCalculatePercentageDiffEdgeCases(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		val1      float64
		val2      float64
		tolerance float64
		expected  string
	}{
		{
			name:      "very small positive difference",
			val1:      1.0001,
			val2:      1.0,
			tolerance: 0.001,
			expected:  "0.0%", // Should round to 0.0%
		},
		{
			name:      "very small negative difference",
			val1:      0.9999,
			val2:      1.0,
			tolerance: 0.001,
			expected:  "-0.0%", // Should round to -0.0%
		},
		{
			name:      "precise decimal calculation",
			val1:      33.0,
			val2:      30.0,
			tolerance: 0.001,
			expected:  "10.0%",
		},
		{
			name:      "one third increase",
			val1:      4.0,
			val2:      3.0,
			tolerance: 0.001,
			expected:  "33.3%",
		},
		{
			name:      "double the value",
			val1:      200.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "100.0%",
		},
		{
			name:      "half the value",
			val1:      50.0,
			val2:      100.0,
			tolerance: 0.001,
			expected:  "-50.0%",
		},
		{
			name:      "high tolerance test",
			val1:      102.0,
			val2:      100.0,
			tolerance: 5.0,
			expected:  "<5%", // 2% difference should be considered within 5% tolerance
		},
		{
			name:      "very high tolerance test",
			val1:      150.0,
			val2:      100.0,
			tolerance: 100.0,
			expected:  "<100%", // 50% difference should be considered within 100% tolerance
		},
		{
			name:      "custom tolerance test",
			val1:      100.05,
			val2:      100.0,
			tolerance: 0.1,
			expected:  "<0.1%", // 0.05% difference should be considered within 0.1% tolerance
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := calculatePercentageDiff(tt.val1, tt.val2, tt.tolerance)
			if result != tt.expected {
				t.Errorf("calculatePercentageDiff(%.4f, %.4f, %.3f) = %q, want %q", tt.val1, tt.val2, tt.tolerance, result, tt.expected)
			}
		})
	}
}

func TestFormatDiffValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		rawDiff        float64
		percentageDiff string
		expected       string
	}{
		{
			name:           "difference within tolerance - very small",
			rawDiff:        5.274e-16,
			percentageDiff: "<0.001%",
			expected:       "5.274e-16",
		},
		{
			name:           "difference within tolerance - small",
			rawDiff:        0.0005,
			percentageDiff: "<0.001%",
			expected:       "0.0005",
		},
		{
			name:           "difference above tolerance",
			rawDiff:        0.5,
			percentageDiff: "10.0%",
			expected:       "0.5",
		},
		{
			name:           "zero difference",
			rawDiff:        0.0,
			percentageDiff: "-",
			expected:       "-",
		},
		{
			name:           "large difference",
			rawDiff:        1000.0,
			percentageDiff: "50.0%",
			expected:       "1000",
		},
		{
			name:           "very small but significant difference",
			rawDiff:        0.001,
			percentageDiff: "0.1%",
			expected:       "0.001",
		},
		{
			name:           "negative difference within tolerance",
			rawDiff:        -1e-15,
			percentageDiff: "<0.001%",
			expected:       "-1e-15",
		},
		{
			name:           "negative difference above tolerance",
			rawDiff:        -0.25,
			percentageDiff: "-5.0%",
			expected:       "-0.25",
		},
		{
			name:           "difference within tolerance - medium",
			rawDiff:        0.05,
			percentageDiff: "<5%",
			expected:       "0.05",
		},
		{
			name:           "difference within tolerance - micro",
			rawDiff:        1e-7,
			percentageDiff: "<0.001%",
			expected:       "1e-07",
		},
		{
			name:           "difference within tolerance - milli",
			rawDiff:        0.0002,
			percentageDiff: "<0.1%",
			expected:       "0.0002",
		},
		{
			name:           "difference within tolerance - large scale",
			rawDiff:        5.0,
			percentageDiff: "<10%",
			expected:       "5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := formatDiffValue(tt.rawDiff, tt.percentageDiff)
			if result != tt.expected {
				t.Errorf("formatDiffValue(%.6g, %q) = %q, want %q", tt.rawDiff, tt.percentageDiff, result, tt.expected)
			}
		})
	}
}
