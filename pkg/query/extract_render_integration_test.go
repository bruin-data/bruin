package query

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractAndRenderCouldWorkTogether(t *testing.T) {
	t.Parallel()

	query := `
-- @bruin.name: some.task.name
-- @bruin.type: sf.sql
-- @bruin.depends: dependency-1

set variable1 = '{{ ds }}'::date;
set variable2 =  'some other date';
set variable3 = 21;


SELECT
    $variable1,
    $variable2,
    $variable3
;


set variable4 = dateadd(days, -($variable2 - 1), $variable1);
CREATE OR REPLACE TABLE my-awesome-table as
with dummy_dates as (
        SELECT
            dateadd(days, -(ROW_NUMBER() OVER (ORDER BY seq4()) - 1), $variable1) as event_date,
            concat(value1, '--', value2) as commentSyntaxAsString,
        FROM TABLE(GENERATOR(ROWCOUNT => $variable2 + 1))
    ),
    joinedTable as (
        SELECT
            field1,
			field2,
    	FROM dummy_dates
    ),
    secondTable as (SELECT 1),
    /*
    SELECT
		name, surname
    FROM my-multiline-comment-table
    GROUP BY 1,2
    ORDER BY 1,2;
     */

    SELECT
        a,
		b,
		c,

    FROM my-awesome-table
    GROUP BY 1,2,3
    ORDER BY 1, 2, 3
;
`

	expectedQueries := []*Query{
		{
			VariableDefinitions: []string{
				"set variable1 = '2022-01-01'::date",
				"set variable2 =  'some other date'",
				"set variable3 = 21",
			},
			Query: `SELECT
    $variable1,
    $variable2,
    $variable3`,
		},
		{
			VariableDefinitions: []string{
				"set variable1 = '2022-01-01'::date",
				"set variable2 =  'some other date'",
				"set variable3 = 21",
				"set variable4 = dateadd(days, -($variable2 - 1), $variable1)",
			},
			Query: `CREATE OR REPLACE TABLE my-awesome-table as
with dummy_dates as (
        SELECT
            dateadd(days, -(ROW_NUMBER() OVER (ORDER BY seq4()) - 1), $variable1) as event_date,
            concat(value1, '--', value2) as commentSyntaxAsString,
        FROM TABLE(GENERATOR(ROWCOUNT => $variable2 + 1))
    ),
    joinedTable as (
        SELECT
            field1,
			field2,
    	FROM dummy_dates
    ),
    secondTable as (SELECT 1),
    SELECT
        a,
		b,
		c,
    FROM my-awesome-table
    GROUP BY 1,2,3
    ORDER BY 1, 2, 3`,
		},
	}

	fs := afero.NewMemMapFs()
	err := afero.WriteFile(fs, "somefile.sql", []byte(query), 0o644)
	require.NoError(t, err)

	extractor := FileQuerySplitterExtractor{
		Fs: fs,
		Renderer: jinja.NewRenderer(jinja.Context{
			"ds": "2022-01-01",
		}),
	}
	res, err := extractor.ExtractQueriesFromFile("somefile.sql")
	require.NoError(t, err)
	assert.Equal(t, expectedQueries, res)
}
