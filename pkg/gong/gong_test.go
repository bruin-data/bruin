package gong

import (
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildDownloadURL(t *testing.T) {
	t.Parallel()
	url := buildDownloadURL(strings.TrimSpace(Version))
	// URL should contain the base URL
	assert.Contains(t, url, BaseURL)
	// URL should contain the version
	assert.Contains(t, url, strings.TrimSpace(Version))
	// URL should follow the expected format
	expectedOS := getOSName()
	expectedArch := getArchName()
	expectedURL := BaseURL + "/releases/" + strings.TrimSpace(Version) + "/" + expectedOS + "/gong_" + expectedArch
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
	url := buildDownloadURL(strings.TrimSpace(Version))
	// Verify the URL matches the expected pattern
	// Format: {BaseURL}/releases/{Version}/{os}/gong_{arch}
	assert.Regexp(t, `^https://storage\.googleapis\.com/gong-release/releases/v\d+\.\d+\.\d+/(darwin|linux|windows)/gong_(amd64|arm64)$`, url)
}

func TestBuildDownloadURL_RespectsRequestedVersion(t *testing.T) {
	t.Parallel()
	url := buildDownloadURL("v9.9.9")
	assert.Contains(t, url, "/releases/v9.9.9/")
}

func TestChecker_SlotPerVersion(t *testing.T) {
	t.Parallel()
	c := &Checker{}
	a := c.slotFor("v1.0.0")
	b := c.slotFor("v1.0.0")
	d := c.slotFor("v1.0.5")
	assert.Same(t, a, b, "same version returns the same slot")
	assert.NotSame(t, a, d, "different versions get different slots")
	// dropSlot must remove only the matching slot.
	c.dropSlot("v1.0.0", a)
	a2 := c.slotFor("v1.0.0")
	assert.NotSame(t, a, a2, "slot should be regenerated after drop")
	// dropSlot with a stale pointer is a no-op.
	c.dropSlot("v1.0.0", a)
	a3 := c.slotFor("v1.0.0")
	assert.Same(t, a2, a3, "stale drop must not evict a fresh slot")
}

func TestChecker_SlotForIsConcurrencySafe(t *testing.T) {
	t.Parallel()
	c := &Checker{}
	const goroutines = 32
	results := make([]*installSlot, goroutines)
	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = c.slotFor("v1.0.0")
		}(i)
	}
	wg.Wait()
	require.NotNil(t, results[0])
	for _, slot := range results {
		assert.Same(t, results[0], slot, "all goroutines must observe the same slot for the same version")
	}
}
