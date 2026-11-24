package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/bruin-data/bruin/pkg/autocreate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var _ autocreate.RelationChecker = (*Client)(nil)

type recordedQuery struct {
	sql  string
	args []any
}

// fakeRelationConn answers the queries RelationExists runs: the current
// database and the number of matching relations.
type fakeRelationConn struct {
	currentDatabase string
	count           uint64
	err             error
	queries         []recordedQuery
}

func (c *fakeRelationConn) Query(_ context.Context, sql string, args ...any) (driver.Rows, error) {
	c.queries = append(c.queries, recordedQuery{sql: sql, args: args})
	if c.err != nil {
		return nil, c.err
	}
	if sql == "SELECT currentDatabase()" {
		return &singleValueRows{value: c.currentDatabase}, nil
	}
	return &singleValueRows{value: c.count}, nil
}

func (c *fakeRelationConn) Exec(context.Context, string, ...any) error {
	return errors.New("unexpected exec")
}

type singleValueRows struct {
	value any
	read  bool
}

func (r *singleValueRows) Next() bool {
	if r.read {
		return false
	}
	r.read = true
	return true
}

func (r *singleValueRows) Scan(dest ...any) error {
	switch d := dest[0].(type) {
	case *uint64:
		*d = r.value.(uint64)
	case *string:
		*d = r.value.(string)
	default:
		return errors.New("unexpected scan destination")
	}
	return nil
}

func (r *singleValueRows) ScanStruct(any) error             { return nil }
func (r *singleValueRows) ColumnTypes() []driver.ColumnType { return nil }
func (r *singleValueRows) Totals(...any) error              { return nil }
func (r *singleValueRows) Columns() []string                { return nil }
func (r *singleValueRows) Close() error                     { return nil }
func (r *singleValueRows) Err() error                       { return nil }

func TestClient_RelationExists(t *testing.T) {
	t.Parallel()

	const lookup = "SELECT count() FROM system.tables WHERE database = ? AND name = ?"

	tests := []struct {
		name        string
		config      *Config
		table       string
		count       uint64
		want        bool
		wantQueries []recordedQuery
	}{
		{
			name:  "database.table is looked up as written",
			table: "Analytics.Events",
			count: 1,
			want:  true,
			wantQueries: []recordedQuery{
				{sql: lookup, args: []any{"Analytics", "Events"}},
			},
		},
		{
			name:   "a one-part name resolves in the current database",
			config: &Config{Database: "configured"},
			table:  "events",
			count:  1,
			want:   true,
			wantQueries: []recordedQuery{
				{sql: "SELECT currentDatabase()"},
				{sql: lookup, args: []any{"current", "events"}},
			},
		},
		{
			name:  "missing table or database",
			table: "analytics.events",
			want:  false,
			wantQueries: []recordedQuery{
				{sql: lookup, args: []any{"analytics", "events"}},
			},
		},
		{
			name:   "a cluster checks every replica",
			config: &Config{Cluster: "my_cluster"},
			table:  "analytics.events",
			want:   false,
			wantQueries: []recordedQuery{
				{
					sql:  "SELECT count() FROM clusterAllReplicas(?, system.tables) WHERE database = ? AND name = ?",
					args: []any{"my_cluster", "analytics", "events"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := tt.config
			if config == nil {
				config = &Config{}
			}
			conn := &fakeRelationConn{currentDatabase: "current", count: tt.count}
			client := &Client{connection: conn, config: config}

			got, err := client.RelationExists(context.Background(), tt.table)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			assert.Equal(t, tt.wantQueries, conn.queries)
		})
	}
}

func TestClient_RelationExistsErrors(t *testing.T) {
	t.Parallel()

	for _, table := range []string{"", "a.b.c", "analytics.", "`analytics`.events", `"events"`, "analytics. events", "my-db.events"} {
		t.Run(table, func(t *testing.T) {
			t.Parallel()

			conn := &fakeRelationConn{}
			client := &Client{connection: conn, config: &Config{}}

			_, err := client.RelationExists(context.Background(), table)
			require.Error(t, err)
			assert.Empty(t, conn.queries)
		})
	}

	t.Run("query error", func(t *testing.T) {
		t.Parallel()

		client := &Client{connection: &fakeRelationConn{err: errors.New("connection refused")}, config: &Config{}}

		_, err := client.RelationExists(context.Background(), "analytics.events")
		require.ErrorContains(t, err, "connection refused")

		_, err = client.RelationExists(context.Background(), "events")
		require.ErrorContains(t, err, "connection refused")
	})

	t.Run("empty current database", func(t *testing.T) {
		t.Parallel()

		client := &Client{connection: &fakeRelationConn{}, config: &Config{}}

		_, err := client.RelationExists(context.Background(), "events")
		require.Error(t, err)
	})
}
