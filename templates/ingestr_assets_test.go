package templates_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type ingestrAsset struct {
	Type       string `yaml:"type"`
	Parameters struct {
		SourceConnection string `yaml:"source_connection"`
		SourceTable      string `yaml:"source_table"`
	} `yaml:"parameters"`
}

// TestIngestrTemplateAssetsHaveUniqueSourceTables guards against copy-paste
// mistakes where two assets in a template pull the same source table, which
// leaves one of the destination tables loaded with the wrong data.
func TestIngestrTemplateAssetsHaveUniqueSourceTables(t *testing.T) {
	t.Parallel()

	templateDirs, err := os.ReadDir(".")
	require.NoError(t, err)

	for _, dir := range templateDirs {
		if !dir.IsDir() {
			continue
		}

		seen := map[string]string{}
		err := filepath.WalkDir(dir.Name(), func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".asset.yml") {
				return err
			}

			content, err := os.ReadFile(path)
			if err != nil {
				return err
			}

			var asset ingestrAsset
			if err := yaml.Unmarshal(content, &asset); err != nil || asset.Type != "ingestr" {
				return nil //nolint:nilerr // non-asset or templated YAML is out of scope here
			}

			key := asset.Parameters.SourceConnection + "/" + asset.Parameters.SourceTable
			if other, ok := seen[key]; ok {
				assert.Failf(t, "duplicate ingestr source table", "%s and %s both load %s", other, path, key)
			}
			seen[key] = path

			return nil
		})
		require.NoError(t, err)
	}
}
