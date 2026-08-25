package templates_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// syncedTemplates are the templates whose docs page is generated from the
// template README. Keep this in step with SYNCED in
// scripts/sync_template_docs.py.
var syncedTemplates = []string{
	"posthog-bigquery",
	"stripe-bigquery",
}

var imageRef = regexp.MustCompile(`\]\(images/([^)]+)\)`)

func generatedHeader(name string) string {
	return "<!-- Generated from templates/" + name + "/README.md. " +
		"Do not edit directly; run `make sync-template-docs`. -->\n"
}

// firstDifference reports the 1-indexed line where want and got diverge, along
// with both versions of it. Comparing the two pages whole would print several
// hundred lines of identical README on every failure, which buries the one line
// that actually moved.
func firstDifference(want, got string) (line int, wantLine, gotLine string) {
	wantLines, gotLines := strings.Split(want, "\n"), strings.Split(got, "\n")
	for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
		var w, g string
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w != g {
			return i + 1, w, g
		}
	}
	return 0, "", ""
}

// TestTemplateDocsAreInSync guards the generated docs pages against drift: the
// template README is the source of truth, and editing either copy alone should
// fail here rather than ship two versions of the same page.
func TestTemplateDocsAreInSync(t *testing.T) {
	t.Parallel()

	for _, name := range syncedTemplates {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			readme, err := os.ReadFile(filepath.Join(name, "README.md"))
			require.NoError(t, err)

			page, err := os.ReadFile(filepath.Join(
				"..", "docs", "getting-started", "templates-docs", name+"-README.md",
			))
			require.NoError(t, err)

			want := generatedHeader(name) + imageRef.ReplaceAllString(string(readme), "](/$1)")
			if string(page) == want {
				return
			}

			line, wantLine, gotLine := firstDifference(want, string(page))
			t.Errorf(
				"docs page for %q is out of date, run `make sync-template-docs`\n"+
					"first difference at line %d:\n  README: %q\n  docs:   %q",
				name, line, wantLine, gotLine,
			)
		})
	}
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// TestTemplateDocsImagesArePublished checks that every image a synced README
// references was copied into docs/public/, since VitePress serves them from the
// site root rather than from the template folder.
func TestTemplateDocsImagesArePublished(t *testing.T) {
	t.Parallel()

	for _, name := range syncedTemplates {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			readme, err := os.ReadFile(filepath.Join(name, "README.md"))
			require.NoError(t, err)

			for _, match := range imageRef.FindAllStringSubmatch(string(readme), -1) {
				image := match[1]

				inTemplate, err := os.ReadFile(filepath.Join(name, "images", image))
				require.NoError(t, err, "%s references images/%s", name, image)

				published, err := os.ReadFile(filepath.Join("..", "docs", "public", image))
				require.NoError(t, err,
					"docs/public/%s is missing, run `make sync-template-docs`", image)

				// Compare digests rather than the bytes themselves: asserting on
				// []byte would spill the whole PNG into the failure output.
				assert.Equal(t, digest(inTemplate), digest(published),
					"docs/public/%s is stale, run `make sync-template-docs`", image)
			}
		})
	}
}

// TestTemplateDocsSyncIsClean runs the generator in check mode so the test and
// the script cannot disagree about what "in sync" means.
func TestTemplateDocsSyncIsClean(t *testing.T) {
	t.Parallel()

	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}

	out, err := exec.Command(
		python, filepath.Join("..", "scripts", "sync_template_docs.py"), "--check",
	).CombinedOutput()
	assert.NoError(t, err, "sync check failed:\n%s", out)
}
