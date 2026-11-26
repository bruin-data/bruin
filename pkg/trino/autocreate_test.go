package trino

import (
	"context"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*Client)(nil)

func TestClient_RelationExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    Config
		table     string
		query     string
		args      []driver.Value
		count     int64
		queryErr  error
		want      bool
		wantError string
	}{
		{
			name:      "quoted name part",
			config:    Config{Catalog: "lake", Schema: "analytics"},
			table:     `"my-schema".events`,
			wantError: "quoted names are not supported",
		},
		{
			name:   "one-part name uses the connection's catalog and schema",
			config: Config{Catalog: "lake", Schema: "analytics"},
			table:  "events",
			query:  `SELECT count(*) FROM "lake".information_schema.tables WHERE table_schema IN (?, ?) AND table_name IN (?, ?)`,
			args:   []driver.Value{"analytics", "analytics", "events", "events"},
			count:  1,
			want:   true,
		},
		{
			name:   "two-part name uses the connection's catalog",
			config: Config{Catalog: "lake", Schema: "analytics"},
			table:  "staging.events",
			query:  `SELECT count(*) FROM "lake".information_schema.tables WHERE table_schema IN (?, ?) AND table_name IN (?, ?)`,
			args:   []driver.Value{"staging", "staging", "events", "events"},
			count:  1,
			want:   true,
		},
		{
			name:   "three-part name sets the catalog",
			config: Config{Catalog: "lake", Schema: "analytics"},
			table:  "other.staging.events",
			query:  `SELECT count(*) FROM "other".information_schema.tables WHERE table_schema IN (?, ?) AND table_name IN (?, ?)`,
			args:   []driver.Value{"staging", "staging", "events", "events"},
			want:   false,
		},
		{
			name:   "mixed case is also looked up in lower case",
			config: Config{Catalog: "lake"},
			table:  "Staging.Events",
			query:  `SELECT count(*) FROM "lake".information_schema.tables WHERE table_schema IN (?, ?) AND table_name IN (?, ?)`,
			args:   []driver.Value{"Staging", "staging", "Events", "events"},
			count:  1,
			want:   true,
		},
		{
			name:      "quotes in the catalog are rejected",
			config:    Config{},
			table:     `my"cat.staging.events`,
			wantError: "quoted names are not supported",
		},
		{
			name:   "missing schema or table finds nothing",
			config: Config{Catalog: "lake"},
			table:  "nowhere.events",
			query:  `SELECT count(*) FROM "lake".information_schema.tables WHERE table_schema IN (?, ?) AND table_name IN (?, ?)`,
			args:   []driver.Value{"nowhere", "nowhere", "events", "events"},
			want:   false,
		},
		{
			name:      "query errors are returned",
			config:    Config{Catalog: "lake"},
			table:     "staging.events",
			query:     `SELECT count(*) FROM "lake".information_schema.tables WHERE table_schema IN (?, ?) AND table_name IN (?, ?)`,
			args:      []driver.Value{"staging", "staging", "events", "events"},
			queryErr:  errors.New("Access Denied"),
			wantError: "Access Denied",
		},
		{
			name:      "one-part name without a connection schema",
			config:    Config{Catalog: "lake"},
			table:     "events",
			wantError: "the connection has no schema",
		},
		{
			name:      "two-part name without a connection catalog",
			config:    Config{Schema: "analytics"},
			table:     "staging.events",
			wantError: "the connection has no catalog",
		},
		{
			name:      "empty component",
			config:    Config{Catalog: "lake"},
			table:     "staging..events",
			wantError: "empty component",
		},
		{
			name:      "too many components",
			config:    Config{Catalog: "lake"},
			table:     "a.b.c.d",
			wantError: "table name must be in format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
			require.NoError(t, err)
			defer db.Close()

			if tt.query != "" {
				expectation := mock.ExpectQuery(tt.query).WithArgs(tt.args...)
				if tt.queryErr != nil {
					expectation.WillReturnError(tt.queryErr)
				} else {
					expectation.WillReturnRows(sqlmock.NewRows([]string{"_col0"}).AddRow(tt.count))
				}
			}

			client := &Client{connection: db, config: tt.config}
			got, err := client.RelationExists(context.Background(), tt.table)
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
// incremental strategy Trino supports builds the table without assuming that it
// exists.
func TestMaterializer_FirstRunCreatesTable(t *testing.T) {
	t.Parallel()

	strategies := []pipeline.Materialization{
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDeleteInsert, IncrementalKey: "id"},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTruncateInsert},
		{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTimeInterval, IncrementalKey: "dt", TimeGranularity: pipeline.MaterializationTimeGranularityDate},
	}

	for _, mat := range strategies {
		t.Run(string(mat.Strategy), func(t *testing.T) {
			t.Parallel()

			asset := &pipeline.Asset{Name: "analytics.events", Materialization: mat}
			got, err := NewMaterializer(false).Render(asset.FirstRun(), "SELECT 1 AS id")
			require.NoError(t, err)
			assert.Equal(t, "DROP TABLE IF EXISTS \"analytics\".\"events\";\nCREATE TABLE \"analytics\".\"events\" WITH (format = 'PARQUET') AS\nSELECT 1 AS id;", strings.TrimSpace(got))
		})
	}
}
