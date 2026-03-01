package vertica

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		identifier string
		want       string
	}{
		{
			name:       "simple identifier",
			identifier: "table_name",
			want:       `"table_name"`,
		},
		{
			name:       "schema.table identifier",
			identifier: "schema.table_name",
			want:       `"schema"."table_name"`,
		},
		{
			name:       "identifier with double quotes",
			identifier: `table"name`,
			want:       `"table""name"`,
		},
		{
			name:       "three part identifier",
			identifier: "catalog.schema.table",
			want:       `"catalog"."schema"."table"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := QuoteIdentifier(tt.identifier)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLimit(t *testing.T) {
	t.Parallel()

	db := &DB{config: &Config{Database: "testdb"}}

	tests := []struct {
		name  string
		query string
		limit int64
		want  string
	}{
		{
			name:  "basic query",
			query: "SELECT * FROM users",
			limit: 10,
			want:  "SELECT * FROM (\nSELECT * FROM users\n) t LIMIT 10",
		},
		{
			name:  "query with trailing semicolon",
			query: "SELECT * FROM users;",
			limit: 5,
			want:  "SELECT * FROM (\nSELECT * FROM users\n) t LIMIT 5",
		},
		{
			name:  "query with trailing whitespace and semicolons",
			query: "SELECT * FROM users; \n\t",
			limit: 100,
			want:  "SELECT * FROM (\nSELECT * FROM users\n) t LIMIT 100",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := db.Limit(tt.query, tt.limit)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildTableExistsQuery(t *testing.T) {
	t.Parallel()

	db := &DB{config: &Config{Database: "testdb"}}

	tests := []struct {
		name      string
		tableName string
		want      string
		wantErr   bool
	}{
		{
			name:      "simple table name defaults to public schema",
			tableName: "users",
			want:      "SELECT COUNT(*) FROM v_catalog.tables WHERE table_schema = 'public' AND table_name = 'users'",
		},
		{
			name:      "schema.table format",
			tableName: "analytics.events",
			want:      "SELECT COUNT(*) FROM v_catalog.tables WHERE table_schema = 'analytics' AND table_name = 'events'",
		},
		{
			name:      "three part name is invalid",
			tableName: "a.b.c",
			wantErr:   true,
		},
		{
			name:      "empty component is invalid",
			tableName: ".table",
			wantErr:   true,
		},
		{
			name:      "trailing dot is invalid",
			tableName: "schema.",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := db.BuildTableExistsQuery(tt.tableName)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
