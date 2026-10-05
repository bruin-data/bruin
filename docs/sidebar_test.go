package docs_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// unlistedPages are pages that are deliberately left out of the sidebar. Keep
// this list short: a page that belongs here should be a redirect, a superseded
// page kept for old links, or content that is not meant to be browsed.
var unlistedPages = map[string]string{
	"overview":                    "CLI summary embedded in the binary, not part of the site",
	"commands/update":             "redirect to commands/upgrade",
	"getting-started/concepts":    "superseded by core-concepts/overview",
	"getting-started/features":    "superseded by core-concepts/overview",
	"getting-started/credentials": "superseded by core-concepts/project",
	"getting-started/devenv":      "superseded by core-concepts/project",
	"platforms/materialization":   "superseded by assets/materialization",
}

var sidebarLink = regexp.MustCompile(`link:\s*"(/[^"#]*)`)

// sidebarSection returns the `sidebar: [...]` array from the VitePress config,
// so links in `nav` or elsewhere do not count as sidebar entries. It matches
// brackets naively, which holds as long as no sidebar label contains one.
func sidebarSection(t *testing.T, config string) string {
	t.Helper()

	start := strings.Index(config, "sidebar:")
	require.NotEqual(t, -1, start, "no sidebar in config.mjs")
	open := strings.Index(config[start:], "[")
	require.NotEqual(t, -1, open, "sidebar is not an array")
	start += open

	depth := 0
	for i := start; i < len(config); i++ {
		switch config[i] {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				return config[start : i+1]
			}
		}
	}
	require.FailNow(t, "sidebar array is not closed")
	return ""
}

// TestEveryPageIsInSidebar catches pages that ship without a sidebar entry,
// which leaves them reachable only through search or a stray link.
func TestEveryPageIsInSidebar(t *testing.T) {
	t.Parallel()

	config, err := os.ReadFile(filepath.Join(".vitepress", "config.mjs"))
	require.NoError(t, err)

	linked := map[string]bool{}
	for _, match := range sidebarLink.FindAllStringSubmatch(sidebarSection(t, string(config)), -1) {
		page := strings.Trim(match[1], "/")
		if page == "" {
			page = "index"
		}
		linked[page] = true
	}

	err = filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == "node_modules" || strings.HasPrefix(d.Name(), ".")) && path != "." {
			return filepath.SkipDir
		}
		if d.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}

		page := filepath.ToSlash(strings.TrimSuffix(path, ".md"))
		if _, ok := unlistedPages[page]; ok {
			assert.False(t, linked[page],
				"%s is in the sidebar, remove it from unlistedPages", page)
			return nil
		}
		assert.True(t, linked[page],
			"docs/%s.md is not in the sidebar in docs/.vitepress/config.mjs; "+
				"add it there, or to unlistedPages if it is deliberately hidden", page)
		return nil
	})
	require.NoError(t, err)
}
