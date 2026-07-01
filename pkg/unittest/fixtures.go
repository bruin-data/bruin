package unittest

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// ResolveFixtures expands the named fixtures a test references into mock inputs
// and merges them with the test's own inputs. A pipeline-level fixture supplies
// rows for one asset; a test pulls it in by name (UnitTest.Fixtures) so many
// tests can share a baseline set of rows (typically lookup or dimension
// tables) without repeating them.
//
// The test's own inputs win: an explicit inputs entry for an asset fully
// replaces a referenced fixture for that same asset. It is an error to
// reference a fixture the pipeline does not define, or to reference two
// fixtures that both supply rows for the same asset (ambiguous).
func ResolveFixtures(available []pipeline.Fixture, test pipeline.UnitTest) ([]pipeline.UnitTestInput, error) {
	if len(test.Fixtures) == 0 {
		return test.Inputs, nil
	}

	byName := make(map[string]pipeline.Fixture, len(available))
	for _, f := range available {
		byName[f.Name] = f
	}

	// Assets the test mocks explicitly; a referenced fixture for one of these is
	// skipped so the test's own rows take precedence.
	explicit := make(map[string]struct{}, len(test.Inputs))
	for _, in := range test.Inputs {
		explicit[normalizeName(in.Asset)] = struct{}{}
	}

	merged := append([]pipeline.UnitTestInput(nil), test.Inputs...)
	provider := make(map[string]string, len(test.Fixtures)) // asset -> fixture that supplied it
	for _, name := range test.Fixtures {
		f, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("unit test %q references fixture %q, which the pipeline does not define", test.Name, name)
		}
		key := normalizeName(f.Asset)
		if prev, dup := provider[key]; dup {
			return nil, fmt.Errorf("fixtures %q and %q both supply rows for asset %q; reference only one", prev, name, f.Asset)
		}
		provider[key] = name
		if _, overridden := explicit[key]; overridden {
			continue
		}
		merged = append(merged, pipeline.UnitTestInput{Asset: f.Asset, Rows: f.Rows})
	}
	return merged, nil
}
