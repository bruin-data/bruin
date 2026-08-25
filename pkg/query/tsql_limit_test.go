package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTSQLLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		limit    int64
		expected string
	}{
		{
			name:     "plain select uses derived-table wrapper",
			query:    "SELECT * FROM users",
			limit:    10,
			expected: "SELECT TOP 10 * FROM (\nSELECT * FROM users\n) as t",
		},
		{
			name:     "trailing semicolon and whitespace are trimmed",
			query:    "SELECT * FROM users; \n\t",
			limit:    5,
			expected: "SELECT TOP 5 * FROM (\nSELECT * FROM users\n) as t",
		},
		{
			name:     "case-sensitive aliases are preserved verbatim",
			query:    "SELECT src.Account FROM (SELECT x AS Account FROM t) src",
			limit:    100,
			expected: "SELECT TOP 100 * FROM (\nSELECT src.Account FROM (SELECT x AS Account FROM t) src\n) as t",
		},
		{
			name:     "single CTE is limited via appended CTE",
			query:    "WITH a AS (SELECT 1 AS id) SELECT * FROM a",
			limit:    10,
			expected: "WITH a AS (SELECT 1 AS id),\n__bruin_limited AS (\nSELECT * FROM a\n)\nSELECT TOP 10 * FROM __bruin_limited",
		},
		{
			name:     "multiple CTEs keep all definitions",
			query:    "WITH a AS (SELECT 1 AS id), b AS (SELECT id FROM a) SELECT * FROM b",
			limit:    25,
			expected: "WITH a AS (SELECT 1 AS id), b AS (SELECT id FROM a),\n__bruin_limited AS (\nSELECT * FROM b\n)\nSELECT TOP 25 * FROM __bruin_limited",
		},
		{
			name:     "CTE with column list",
			query:    "WITH a (id, n) AS (SELECT 1, 2) SELECT * FROM a",
			limit:    3,
			expected: "WITH a (id, n) AS (SELECT 1, 2),\n__bruin_limited AS (\nSELECT * FROM a\n)\nSELECT TOP 3 * FROM __bruin_limited",
		},
		{
			name:     "lowercase with keyword",
			query:    "with a as (select 1 as id) select * from a",
			limit:    7,
			expected: "with a as (select 1 as id),\n__bruin_limited AS (\nselect * from a\n)\nSELECT TOP 7 * FROM __bruin_limited",
		},
		{
			name:     "nested parentheses inside a CTE body",
			query:    "WITH a AS (SELECT * FROM (SELECT id FROM t) x) SELECT * FROM a",
			limit:    9,
			expected: "WITH a AS (SELECT * FROM (SELECT id FROM t) x),\n__bruin_limited AS (\nSELECT * FROM a\n)\nSELECT TOP 9 * FROM __bruin_limited",
		},
		{
			name:     "comma inside a string literal does not start a new CTE",
			query:    "WITH a AS (SELECT ',' AS sep) SELECT * FROM a",
			limit:    4,
			expected: "WITH a AS (SELECT ',' AS sep),\n__bruin_limited AS (\nSELECT * FROM a\n)\nSELECT TOP 4 * FROM __bruin_limited",
		},
		{
			name:     "AS inside a bracketed identifier is not the CTE AS",
			query:    "WITH [my AS cte] AS (SELECT 1 AS id) SELECT * FROM [my AS cte]",
			limit:    6,
			expected: "WITH [my AS cte] AS (SELECT 1 AS id),\n__bruin_limited AS (\nSELECT * FROM [my AS cte]\n)\nSELECT TOP 6 * FROM __bruin_limited",
		},
		{
			name:     "leading line comment before WITH",
			query:    "-- lead comment\nWITH a AS (SELECT 1 AS id) SELECT * FROM a",
			limit:    8,
			expected: "-- lead comment\nWITH a AS (SELECT 1 AS id),\n__bruin_limited AS (\nSELECT * FROM a\n)\nSELECT TOP 8 * FROM __bruin_limited",
		},
		{
			name:     "final select carries a mid-query comment",
			query:    "WITH a AS (SELECT 1 AS id)\n-- pick everything\nSELECT * FROM a",
			limit:    2,
			expected: "WITH a AS (SELECT 1 AS id),\n__bruin_limited AS (\n-- pick everything\nSELECT * FROM a\n)\nSELECT TOP 2 * FROM __bruin_limited",
		},
		{
			name:     "final query is a UNION over CTEs",
			query:    "WITH a AS (SELECT 1 AS id) SELECT * FROM a UNION ALL SELECT 2",
			limit:    50,
			expected: "WITH a AS (SELECT 1 AS id),\n__bruin_limited AS (\nSELECT * FROM a UNION ALL SELECT 2\n)\nSELECT TOP 50 * FROM __bruin_limited",
		},
		{
			name:     "word starting with with is not a CTE",
			query:    "SELECT withheld FROM t",
			limit:    10,
			expected: "SELECT TOP 10 * FROM (\nSELECT withheld FROM t\n) as t",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.expected, TSQLLimit(tt.query, tt.limit))
		})
	}
}

func TestSplitLeadingWith(t *testing.T) {
	t.Parallel()

	t.Run("no leading with", func(t *testing.T) {
		t.Parallel()
		_, _, ok := splitLeadingWith("SELECT * FROM t")
		assert.False(t, ok)
	})

	t.Run("malformed with falls back", func(t *testing.T) {
		t.Parallel()
		// WITH (NOLOCK)-style input has no CTE AS clause; must not be treated as a CTE.
		_, _, ok := splitLeadingWith("WITH (NOLOCK) SELECT 1")
		assert.False(t, ok)
	})

	t.Run("splits prefix and body", func(t *testing.T) {
		t.Parallel()
		prefix, body, ok := splitLeadingWith("WITH a AS (SELECT 1 AS id) SELECT * FROM a")
		assert.True(t, ok)
		assert.Equal(t, "WITH a AS (SELECT 1 AS id)", prefix)
		assert.Equal(t, "SELECT * FROM a", body)
	})
}
