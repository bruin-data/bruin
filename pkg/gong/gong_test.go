package gong

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildDownloadURL(t *testing.T) {
	t.Parallel()

	url := BuildDownloadURL()

	// URL should contain the base URL
	assert.Contains(t, url, BaseURL)

	// URL should contain the version
	assert.Contains(t, url, Version)

	// URL should follow the expected format
	expectedOS := getOSName()
	expectedArch := getArchName()
	expectedURL := BaseURL + "/releases/" + Version + "/" + expectedOS + "/gong_" + expectedArch
	assert.Equal(t, expectedURL, url)
}

func TestGetOSName(t *testing.T) {
	t.Parallel()

	osName := getOSName()

	// Should return a valid OS name
	validOSNames := []string{"darwin", "linux", "windows"}
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" || runtime.GOOS == "windows" {
		assert.Contains(t, validOSNames, osName)
	} else {
		// For other OSes, it should return runtime.GOOS
		assert.Equal(t, runtime.GOOS, osName)
	}
}

func TestGetArchName(t *testing.T) {
	t.Parallel()

	archName := getArchName()

	// Should return a valid architecture name
	validArchNames := []string{"amd64", "arm64"}
	if runtime.GOARCH == "amd64" || runtime.GOARCH == "arm64" {
		assert.Contains(t, validArchNames, archName)
	} else {
		// For other architectures, it should return runtime.GOARCH
		assert.Equal(t, runtime.GOARCH, archName)
	}
}

func TestParseVersionOutput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{"standard output", "gong version 0.1.2", "v0.1.2"},
		{"already has v prefix", "gong version v0.1.2", "v0.1.2"},
		{"just version number", "0.1.2", "0.1.2"},
		{"empty string", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expect, parseVersionOutput(tt.input))
		})
	}
}

func TestBuildDownloadURL_Format(t *testing.T) {
	t.Parallel()

	url := BuildDownloadURL()

	// Verify the URL matches the expected pattern
	// Format: {BaseURL}/releases/{Version}/{os}/gong_{arch}
	assert.Regexp(t, `^https://storage\.googleapis\.com/gong-release/releases/v\d+\.\d+\.\d+/(darwin|linux|windows)/gong_(amd64|arm64)$`, url)
}
