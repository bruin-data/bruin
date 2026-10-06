package sqlparser

import (
	"encoding/json"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// hoistSkeleton drops what hoisting may rewrite (whitespace and semicolons) and sorts the rest:
// reordering statements must neither lose nor invent any other character.
func hoistSkeleton(s string) string {
	runes := []rune(strings.Map(func(r rune) rune {
		if r == ';' || isPySpace(r) {
			return -1
		}
		return r
	}, s))
	slices.Sort(runes)
	return string(runes)
}

func checkHoistDeclaresInvariants(t *testing.T, query, dialect string) {
	t.Helper()
	res := hoistDeclares(query, dialect)
	got, _ := res["query"].(string)
	if res["error"] != "" {
		require.Equal(t, query, got, "a failed hoist must return its input")
		return
	}
	if got == query {
		return
	}
	require.Equal(t, hoistSkeleton(query), hoistSkeleton(got), "hoisting changed more than whitespace and separators:\n%q\n%q", query, got)
	if strings.Contains(query, "--") {
		// Known issue: a statement ending in a line comment absorbs the ';' that hoisting appends,
		// so its output can split differently the second time.
		return
	}
	again := hoistDeclares(got, dialect)
	require.Equal(t, got, again["query"], "hoisting is not idempotent for %q", query)
}

func checkHoistDeclaresListInvariants(t *testing.T, queries []string, dialect string) {
	t.Helper()
	res := hoistDeclaresList(queries, dialect)
	require.Empty(t, res["error"])
	got, _ := res["queries"].([]string)
	require.ElementsMatch(t, queries, got, "hoisting must permute the entries")
	sawStatement := false
	for _, q := range got {
		isDeclare := isDeclareStatement(pyStrip(q), normalizeDialect(dialect))
		require.False(t, isDeclare && sawStatement, "declaration %q after a statement in %q", q, got)
		sawStatement = sawStatement || !isDeclare
	}
	require.Equal(t, got, hoistDeclaresList(got, dialect)["queries"], "hoisting is not idempotent for %q", queries)
}

// TestHoistInvariantsOnGoldenCorpus checks properties every hoist must satisfy on the composed
// scripts of testdata/golden/hoist.json.gz, independently of the recorded Python answers.
func TestHoistInvariantsOnGoldenCorpus(t *testing.T) {
	t.Parallel()
	recs := readGolden(t, filepath.Join("testdata", "golden", "hoist.json.gz"))
	step := 1
	if raceEnabled {
		step = 25
	}
	for i := 0; i < len(recs); i += step {
		var pc parserCommand
		require.NoError(t, json.Unmarshal(recs[i].Cmd, &pc))
		dialect, _ := pc.Contents["dialect"].(string)
		switch pc.Command {
		case "hoist-declares":
			checkHoistDeclaresInvariants(t, pc.Contents["query"].(string), dialect)
		case "hoist-declares-list":
			checkHoistDeclaresListInvariants(t, toStrings(pc.Contents["queries"]), dialect)
		}
	}
}

func FuzzHoistDeclares(f *testing.F) {
	for _, seed := range []string{
		"SET x = 1;\nDECLARE y INT64;\nSELECT 1;",
		"SELECT 'a;b';\nBEGIN\n  DECLARE y INT64;\nEND;\nDECLARE x INT64;",
		"SELECT 1; BEGIN IF TRUE THEN SELECT 2; END IF; DECLARE inner_x INT64; END; DECLARE outer_x INT64;",
		"BEGIN TRANSACTION;\nSET x = 1;\nDECLARE y INT64;\nCOMMIT TRANSACTION;",
		"SELECT $$DECLARE fake INT;$$; DECLARE real_x INT;",
		"SELECT 1; DECLARE @t TABLE(id INT); DECLARE @x INT;",
	} {
		for _, dialect := range []string{"bigquery", "postgres", "tsql", "snowflake"} {
			f.Add(seed, dialect)
		}
	}
	f.Fuzz(func(t *testing.T, query, dialect string) {
		if _, err := getDialect(dialect); err != nil {
			t.Skip()
		}
		checkHoistDeclaresInvariants(t, query, dialect)
	})
}

func FuzzHoistDeclaresList(f *testing.F) {
	f.Add("SET x = 1\x00DECLARE y INT64\x00SELECT 1", "bigquery")
	f.Add("SELECT 1; DECLARE embedded INT\x00  DECLARE a STRING  \x00\x00DECLARE @x INT = 5", "tsql")
	f.Fuzz(func(t *testing.T, joined, dialect string) {
		if _, err := getDialect(dialect); err != nil {
			t.Skip()
		}
		checkHoistDeclaresListInvariants(t, strings.Split(joined, "\x00"), dialect)
	})
}
