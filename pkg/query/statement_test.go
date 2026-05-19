package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsLikelyResultQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		sql  string
		want bool
	}{
		{
			name: "select with leading comments returns result",
			sql:  "-- comment\n/* another comment */\nSELECT * FROM users",
			want: true,
		},
		{
			name: "empty select with cte returns result",
			sql:  "WITH users AS (SELECT 1) SELECT * FROM users",
			want: true,
		},
		{
			name: "insert returning returns result",
			sql:  "INSERT INTO users (id) VALUES (1) RETURNING id",
			want: true,
		},
		{
			name: "returning inside string literal is ignored",
			sql:  "UPDATE users SET name = 'returning'",
			want: false,
		},
		{
			name: "update without returning does not return result",
			sql:  "UPDATE users SET name = 'Ada'",
			want: false,
		},
		{
			name: "create table does not return result",
			sql:  "CREATE TABLE users (id INTEGER)",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, IsLikelyResultQuery(tt.sql))
		})
	}
}

func TestStatementTypeFromCommandTag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		commandTag string
		want       string
	}{
		{commandTag: "UPDATE 3", want: "UPDATE"},
		{commandTag: "INSERT 0 2", want: "INSERT"},
		{commandTag: "CREATE TABLE", want: "CREATE TABLE"},
		{commandTag: "DELETE 0", want: "DELETE"},
	}

	for _, tt := range tests {
		t.Run(tt.commandTag, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, StatementTypeFromCommandTag(tt.commandTag))
		})
	}
}

func TestSQLStatementType(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "CREATE TABLE", SQLStatementType("CREATE TABLE users (id INTEGER)"))
	assert.Equal(t, "DROP VIEW", SQLStatementType("DROP VIEW users"))
	assert.Equal(t, "UPDATE", SQLStatementType("UPDATE users SET name = 'Ada'"))
}
