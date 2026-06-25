package mongo

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildDatabaseSummary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		summaryName     string
		collectionsByDB map[string][]string
		want            *ansisql.DBDatabase
	}{
		{
			name:            "empty server",
			summaryName:     "mongo",
			collectionsByDB: map[string][]string{},
			want: &ansisql.DBDatabase{
				Name:    "mongo",
				Schemas: []*ansisql.DBSchema{},
			},
		},
		{
			name:        "maps databases to schemas and collections to tables, sorted",
			summaryName: "mongo",
			collectionsByDB: map[string][]string{
				"shop":      {"orders", "users"},
				"analytics": {"events"},
			},
			want: &ansisql.DBDatabase{
				Name: "mongo",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "analytics",
						Tables: []*ansisql.DBTable{
							{Name: "events", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
						},
					},
					{
						Name: "shop",
						Tables: []*ansisql.DBTable{
							{Name: "orders", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
							{Name: "users", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
						},
					},
				},
			},
		},
		{
			name:        "skips system databases and system collections",
			summaryName: "mongo",
			collectionsByDB: map[string][]string{
				"admin":  {"system.users"},
				"local":  {"oplog.rs"},
				"config": {"shards"},
				"shop":   {"users", "system.views", "system.profile"},
			},
			want: &ansisql.DBDatabase{
				Name: "mongo",
				Schemas: []*ansisql.DBSchema{
					{
						Name: "shop",
						Tables: []*ansisql.DBTable{
							{Name: "users", Type: ansisql.DBTableTypeTable, Columns: []*ansisql.DBColumn{}},
						},
					},
				},
			},
		},
		{
			name:        "database with only system collections yields an empty schema",
			summaryName: "mongo",
			collectionsByDB: map[string][]string{
				"shop": {"system.views"},
			},
			want: &ansisql.DBDatabase{
				Name: "mongo",
				Schemas: []*ansisql.DBSchema{
					{Name: "shop", Tables: []*ansisql.DBTable{}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := buildDatabaseSummary(tt.summaryName, tt.collectionsByDB)
			require.NotNil(t, got)
			assert.Equal(t, tt.want, got)

			// Imported MongoDB collections must never carry columns.
			for _, schema := range got.Schemas {
				for _, table := range schema.Tables {
					assert.Empty(t, table.Columns)
				}
			}
		})
	}
}
