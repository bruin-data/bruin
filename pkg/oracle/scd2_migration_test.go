package oracle

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildSCD2MigrationStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		tableName   string
		columnTypes map[string]string
		want        []string
	}{
		{
			name:      "both columns naive are migrated",
			tableName: "scd2.oracle_products",
			columnTypes: map[string]string{
				"bruin_valid_from":  "timestamp(6)",
				"bruin_valid_until": "timestamp(6)",
			},
			want: []string{
				"ALTER TABLE scd2.oracle_products ADD (bruin_valid_from__bruin_tz TIMESTAMP(6) WITH TIME ZONE)",
				"UPDATE scd2.oracle_products SET bruin_valid_from__bruin_tz = FROM_TZ(CAST(bruin_valid_from AS TIMESTAMP(6)), 'UTC')",
				"ALTER TABLE scd2.oracle_products DROP COLUMN bruin_valid_from",
				"ALTER TABLE scd2.oracle_products RENAME COLUMN bruin_valid_from__bruin_tz TO bruin_valid_from",
				"ALTER TABLE scd2.oracle_products ADD (bruin_valid_until__bruin_tz TIMESTAMP(6) WITH TIME ZONE)",
				"UPDATE scd2.oracle_products SET bruin_valid_until__bruin_tz = FROM_TZ(CAST(bruin_valid_until AS TIMESTAMP(6)), 'UTC')",
				"ALTER TABLE scd2.oracle_products DROP COLUMN bruin_valid_until",
				"ALTER TABLE scd2.oracle_products RENAME COLUMN bruin_valid_until__bruin_tz TO bruin_valid_until",
			},
		},
		{
			name:      "already timezone-aware is a no-op",
			tableName: "oracle_products",
			columnTypes: map[string]string{
				"bruin_valid_from":  "timestamp(6) with time zone",
				"bruin_valid_until": "timestamp(6) with time zone",
			},
			want: []string{},
		},
		{
			name:      "only one column naive migrates just that one",
			tableName: "oracle_products",
			columnTypes: map[string]string{
				"bruin_valid_from":  "timestamp(6) with time zone",
				"bruin_valid_until": "timestamp(6)",
			},
			want: []string{
				"ALTER TABLE oracle_products ADD (bruin_valid_until__bruin_tz TIMESTAMP(6) WITH TIME ZONE)",
				"UPDATE oracle_products SET bruin_valid_until__bruin_tz = FROM_TZ(CAST(bruin_valid_until AS TIMESTAMP(6)), 'UTC')",
				"ALTER TABLE oracle_products DROP COLUMN bruin_valid_until",
				"ALTER TABLE oracle_products RENAME COLUMN bruin_valid_until__bruin_tz TO bruin_valid_until",
			},
		},
		{
			name:        "no scd2 columns present is a no-op",
			tableName:   "oracle_products",
			columnTypes: map[string]string{"product_id": "number"},
			want:        []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, buildSCD2MigrationStatements(tt.tableName, tt.columnTypes))
		})
	}
}
