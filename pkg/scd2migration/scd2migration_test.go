package scd2migration

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeQuerier struct {
	columns [][]interface{}
	ran     []string
}

func (f *fakeQuerier) Select(_ context.Context, _ *query.Query) ([][]interface{}, error) {
	return f.columns, nil
}

func (f *fakeQuerier) RunQueryWithoutResult(_ context.Context, q *query.Query) error {
	f.ran = append(f.ran, q.Query)
	return nil
}

func naiveCols(from, until string) [][]interface{} {
	return [][]interface{}{{"_valid_from", from}, {"_valid_until", until}}
}

func TestPostgres(t *testing.T) {
	t.Parallel()

	db := &fakeQuerier{columns: naiveCols("timestamp without time zone", "timestamp without time zone")}
	require.NoError(t, Postgres(context.Background(), db, "public.products"))
	assert.Equal(t, []string{
		`ALTER TABLE "public"."products" ADD COLUMN "_valid_from__bruin_tz" TIMESTAMPTZ`,
		`UPDATE "public"."products" SET "_valid_from__bruin_tz" = CAST("_valid_from" AS TIMESTAMPTZ)`,
		`ALTER TABLE "public"."products" DROP COLUMN "_valid_from"`,
		`ALTER TABLE "public"."products" RENAME COLUMN "_valid_from__bruin_tz" TO "_valid_from"`,
		`ALTER TABLE "public"."products" ADD COLUMN "_valid_until__bruin_tz" TIMESTAMPTZ`,
		`UPDATE "public"."products" SET "_valid_until__bruin_tz" = CAST("_valid_until" AS TIMESTAMPTZ)`,
		`ALTER TABLE "public"."products" DROP COLUMN "_valid_until"`,
		`ALTER TABLE "public"."products" RENAME COLUMN "_valid_until__bruin_tz" TO "_valid_until"`,
	}, db.ran)
}

func TestPostgresDropsLeftoverTempColumn(t *testing.T) {
	t.Parallel()

	db := &fakeQuerier{columns: [][]interface{}{
		{"_valid_from", "timestamp without time zone"},
		{"_valid_until", "timestamp without time zone"},
		{"_valid_from__bruin_tz", "timestamp with time zone"},
	}}
	require.NoError(t, Postgres(context.Background(), db, "products"))
	assert.Equal(t, []string{
		`ALTER TABLE "products" DROP COLUMN "_valid_from__bruin_tz"`,
		`ALTER TABLE "products" ADD COLUMN "_valid_from__bruin_tz" TIMESTAMPTZ`,
		`UPDATE "products" SET "_valid_from__bruin_tz" = CAST("_valid_from" AS TIMESTAMPTZ)`,
		`ALTER TABLE "products" DROP COLUMN "_valid_from"`,
		`ALTER TABLE "products" RENAME COLUMN "_valid_from__bruin_tz" TO "_valid_from"`,
		`ALTER TABLE "products" ADD COLUMN "_valid_until__bruin_tz" TIMESTAMPTZ`,
		`UPDATE "products" SET "_valid_until__bruin_tz" = CAST("_valid_until" AS TIMESTAMPTZ)`,
		`ALTER TABLE "products" DROP COLUMN "_valid_until"`,
		`ALTER TABLE "products" RENAME COLUMN "_valid_until__bruin_tz" TO "_valid_until"`,
	}, db.ran)
}

func TestPostgresRecoversInterruptedSwap(t *testing.T) {
	t.Parallel()

	// A prior run dropped _valid_from but crashed before renaming the temp back.
	// The next run must finish the rename rather than leave the table broken.
	db := &fakeQuerier{columns: [][]interface{}{
		{"_valid_from__bruin_tz", "timestamp with time zone"},
		{"_valid_until", "timestamp with time zone"},
	}}
	require.NoError(t, Postgres(context.Background(), db, "products"))
	assert.Equal(t, []string{
		`ALTER TABLE "products" RENAME COLUMN "_valid_from__bruin_tz" TO "_valid_from"`,
	}, db.ran)
}

func TestPostgresAlreadyTimezoneAware(t *testing.T) {
	t.Parallel()

	db := &fakeQuerier{columns: naiveCols("timestamp with time zone", "timestamp with time zone")}
	require.NoError(t, Postgres(context.Background(), db, "products"))
	assert.Empty(t, db.ran)
}

func TestSnowflake(t *testing.T) {
	t.Parallel()

	db := &fakeQuerier{columns: naiveCols("timestamp_ntz", "timestamp_ntz")}
	require.NoError(t, Snowflake(context.Background(), db, "DB.SCHEMA.PRODUCTS"))
	assert.Equal(t, []string{
		"ALTER TABLE DB.SCHEMA.PRODUCTS ADD COLUMN _valid_from__bruin_tz TIMESTAMP_TZ",
		"UPDATE DB.SCHEMA.PRODUCTS SET _valid_from__bruin_tz = CONVERT_TIMEZONE('UTC', CAST(_valid_from AS TIMESTAMP_TZ))",
		"ALTER TABLE DB.SCHEMA.PRODUCTS DROP COLUMN _valid_from",
		"ALTER TABLE DB.SCHEMA.PRODUCTS RENAME COLUMN _valid_from__bruin_tz TO _valid_from",
		"ALTER TABLE DB.SCHEMA.PRODUCTS ADD COLUMN _valid_until__bruin_tz TIMESTAMP_TZ",
		"UPDATE DB.SCHEMA.PRODUCTS SET _valid_until__bruin_tz = CONVERT_TIMEZONE('UTC', CAST(_valid_until AS TIMESTAMP_TZ))",
		"ALTER TABLE DB.SCHEMA.PRODUCTS DROP COLUMN _valid_until",
		"ALTER TABLE DB.SCHEMA.PRODUCTS RENAME COLUMN _valid_until__bruin_tz TO _valid_until",
	}, db.ran)
}

func TestSnowflakeLTZIsMigratedButTZIsSkipped(t *testing.T) {
	t.Parallel()

	db := &fakeQuerier{columns: naiveCols("timestamp_ltz", "timestamp_tz")}
	require.NoError(t, Snowflake(context.Background(), db, "schema.products"))
	assert.Equal(t, []string{
		"ALTER TABLE schema.products ADD COLUMN _valid_from__bruin_tz TIMESTAMP_TZ",
		"UPDATE schema.products SET _valid_from__bruin_tz = CONVERT_TIMEZONE('UTC', CAST(_valid_from AS TIMESTAMP_TZ))",
		"ALTER TABLE schema.products DROP COLUMN _valid_from",
		"ALTER TABLE schema.products RENAME COLUMN _valid_from__bruin_tz TO _valid_from",
	}, db.ran)
}

func TestMySQL(t *testing.T) {
	t.Parallel()

	// The old string sentinel left _valid_until as VARCHAR; _valid_from is fine.
	db := &fakeQuerier{columns: naiveCols("datetime", "varchar")}
	require.NoError(t, MySQL(context.Background(), db, "scd2db.products"))
	assert.Equal(t, []string{
		"ALTER TABLE scd2db.products ADD COLUMN _valid_until__bruin_tz DATETIME",
		"UPDATE scd2db.products SET _valid_until__bruin_tz = CAST(_valid_until AS DATETIME)",
		"ALTER TABLE scd2db.products DROP COLUMN _valid_until",
		"ALTER TABLE scd2db.products RENAME COLUMN _valid_until__bruin_tz TO _valid_until",
	}, db.ran)
}

func TestMySQLBytesFromDriver(t *testing.T) {
	t.Parallel()

	// MySQL's driver returns []byte for text columns; ensure it is handled.
	db := &fakeQuerier{columns: [][]interface{}{
		{[]byte("_valid_from"), []byte("datetime")},
		{[]byte("_valid_until"), []byte("datetime")},
	}}
	require.NoError(t, MySQL(context.Background(), db, "products"))
	assert.Empty(t, db.ran)
}
