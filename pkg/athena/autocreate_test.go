package athena

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*DB)(nil)

func TestDB_RelationExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		database  string
		table     string
		query     string
		result    *sqlmock.Rows
		queryErr  error
		want      bool
		wantError string
	}{
		{
			name:      "quoted name part",
			database:  "analytics",
			table:     `"my-db".events`,
			wantError: "quoted names are not supported",
		},
		{
			name:     "one-part name uses the connection's database",
			database: "analytics",
			table:    "events",
			query:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('analytics', 'analytics') AND table_name IN ('events', 'events')",
			result:   sqlmock.NewRows([]string{"_col0"}).AddRow(int64(1)),
			want:     true,
		},
		{
			name:   "one-part name without a database uses the driver's default database",
			table:  "events",
			query:  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('default', 'default') AND table_name IN ('events', 'events')",
			result: sqlmock.NewRows([]string{"_col0"}).AddRow(int64(1)),
			want:   true,
		},
		{
			name:     "database.table",
			database: "analytics",
			table:    "staging.events",
			query:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('staging', 'staging') AND table_name IN ('events', 'events')",
			result:   sqlmock.NewRows([]string{"_col0"}).AddRow(int64(1)),
			want:     true,
		},
		{
			name:     "catalog.database.table",
			database: "analytics",
			table:    "awsdatacatalog.staging.events",
			query:    `SELECT COUNT(*) FROM "awsdatacatalog".information_schema.tables WHERE table_schema IN ('staging', 'staging') AND table_name IN ('events', 'events')`,
			result:   sqlmock.NewRows([]string{"_col0"}).AddRow(int64(1)),
			want:     true,
		},
		{
			name:     "mixed case is also looked up in lower case",
			database: "analytics",
			table:    "Staging.Events",
			query:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('Staging', 'staging') AND table_name IN ('Events', 'events')",
			result:   sqlmock.NewRows([]string{"_col0"}).AddRow(int64(1)),
			want:     true,
		},
		{
			name:     "string literals are escaped",
			database: "analytics",
			table:    "o'brien.order's",
			query:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('o''brien', 'o''brien') AND table_name IN ('order''s', 'order''s')",
			result:   sqlmock.NewRows([]string{"_col0"}).AddRow(int64(0)),
			want:     false,
		},
		{
			name:     "missing database or table finds nothing",
			database: "analytics",
			table:    "nowhere.events",
			query:    "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('nowhere', 'nowhere') AND table_name IN ('events', 'events')",
			result:   sqlmock.NewRows([]string{"_col0"}).AddRow("0"),
			want:     false,
		},
		{
			name:      "an empty result is an error, not a missing table",
			database:  "analytics",
			table:     "staging.events",
			query:     "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('staging', 'staging') AND table_name IN ('events', 'events')",
			result:    sqlmock.NewRows([]string{"_col0"}),
			wantError: "failed to check whether table 'staging.events' exists",
		},
		{
			name:      "query errors are returned",
			database:  "analytics",
			table:     "staging.events",
			query:     "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema IN ('staging', 'staging') AND table_name IN ('events', 'events')",
			queryErr:  errors.New("AccessDeniedException"),
			wantError: "AccessDeniedException",
		},
		{
			name:      "empty component",
			table:     "staging..events",
			wantError: "empty component",
		},
		{
			name:      "too many components",
			table:     "a.b.c.d",
			wantError: "must be in format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mockDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer mockDB.Close()

			if tt.query != "" {
				expectation := mock.ExpectQuery(tt.query)
				if tt.queryErr != nil {
					expectation.WillReturnError(tt.queryErr)
				} else {
					expectation.WillReturnRows(tt.result)
				}
			}

			db := &DB{conn: sqlx.NewDb(mockDB, "sqlmock"), config: &Config{Database: tt.database}}
			got, err := db.RelationExists(context.Background(), tt.table)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestMaterializer_FirstRunCreatesTable checks that the first run of every
// incremental strategy builds the table without assuming that it exists.
func TestMaterializer_FirstRunCreatesTable(t *testing.T) {
	t.Parallel()

	columns := []pipeline.Column{
		{Name: "id", Type: "INTEGER", PrimaryKey: true},
		{Name: "updated_at", Type: "TIMESTAMP"},
	}
	strategies := []pipeline.Materialization{
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyMerge},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDeleteInsert, IncrementalKey: "id"},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTruncateInsert},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTimeInterval, IncrementalKey: "updated_at", TimeGranularity: pipeline.MaterializationTimeGranularityTimestamp},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategySCD2ByColumn},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategySCD2ByTime, IncrementalKey: "updated_at"},
	}

	for _, mat := range strategies {
		t.Run(string(mat.Strategy), func(t *testing.T) {
			t.Parallel()

			asset := &pipeline.Asset{Name: "analytics.events", Materialization: mat, Columns: columns}
			queries, err := NewMaterializer(false).Render(asset.FirstRun(), "SELECT 1 AS id, current_timestamp AS updated_at", "s3://bucket/results")
			require.NoError(t, err)

			// A new table is written next to the target and renamed into place,
			// which only drops the target if it exists.
			require.Len(t, queries, 3)
			assert.Contains(t, queries[0], "CREATE TABLE __bruin_tmp_")
			assert.NotContains(t, queries[0], "analytics.events")
			assert.Equal(t, "DROP TABLE IF EXISTS analytics.events", trimSpaceAndSemicolon(queries[1]))
			assert.Regexp(t, `^ALTER TABLE __bruin_tmp_\w+ RENAME TO analytics\.events$`, trimSpaceAndSemicolon(queries[2]))
		})
	}
}

func trimSpaceAndSemicolon(query string) string {
	return strings.TrimSuffix(strings.TrimSpace(query), ";")
}
