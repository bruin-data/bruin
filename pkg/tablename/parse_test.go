package tablename

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCapability_Parse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		platform string
		raw      string
		defaults Defaults
		want     TableName
		wantErr  bool
	}{
		{
			name:     "snowflake one-part fills both defaults",
			platform: "snowflake",
			raw:      "events",
			defaults: Defaults{Catalog: "PROD", Schema: "PUBLIC"},
			want:     TableName{Catalog: "PROD", Schema: "PUBLIC", Table: "events"},
		},
		{
			name:     "snowflake two-part fills catalog default",
			platform: "snowflake",
			raw:      "analytics.events",
			defaults: Defaults{Catalog: "PROD", Schema: "PUBLIC"},
			want:     TableName{Catalog: "PROD", Schema: "analytics", Table: "events"},
		},
		{
			name:     "snowflake three-part keeps all components",
			platform: "snowflake",
			raw:      "raw.analytics.events",
			defaults: Defaults{Catalog: "PROD", Schema: "PUBLIC"},
			want:     TableName{Catalog: "raw", Schema: "analytics", Table: "events"},
		},
		{
			name:     "bigquery three-part is project.dataset.table",
			platform: "google_cloud_platform",
			raw:      "my-project.finance.revenue",
			defaults: Defaults{Catalog: "default-project"},
			want:     TableName{Catalog: "my-project", Schema: "finance", Table: "revenue"},
		},
		{
			name:     "bigquery two-part fills project default",
			platform: "google_cloud_platform",
			raw:      "finance.revenue",
			defaults: Defaults{Catalog: "default-project"},
			want:     TableName{Catalog: "default-project", Schema: "finance", Table: "revenue"},
		},
		{
			name:     "bigquery one-part is rejected (no default dataset)",
			platform: "google_cloud_platform",
			raw:      "revenue",
			wantErr:  true,
		},
		{
			name:     "databricks three-part is catalog.schema.table",
			platform: "databricks",
			raw:      "main.silver.orders",
			want:     TableName{Catalog: "main", Schema: "silver", Table: "orders"},
		},
		{
			name:     "postgres rejects three-part",
			platform: "postgres",
			raw:      "db.public.users",
			wantErr:  true,
		},
		{
			name:     "postgres two-part is schema.table",
			platform: "postgres",
			raw:      "public.users",
			want:     TableName{Schema: "public", Table: "users"},
		},
		{
			name:     "doris two-part is database.table",
			platform: "doris",
			raw:      "analytics.orders",
			want:     TableName{Schema: "analytics", Table: "orders"},
		},
		{
			name:     "doris rejects three-part",
			platform: "doris",
			raw:      "catalog.analytics.orders",
			wantErr:  true,
		},
		{
			name:     "clickhouse rejects three-part",
			platform: "clickhouse",
			raw:      "cat.db.tbl",
			wantErr:  true,
		},
		{
			name:     "fabric three-part is database schema table",
			platform: "fabric",
			raw:      "warehouse.dbo.orders",
			want:     TableName{Catalog: "warehouse", Schema: "dbo", Table: "orders"},
		},
		{
			name:     "empty component is rejected",
			platform: "snowflake",
			raw:      "raw..events",
			wantErr:  true,
		},
		{
			name:     "trailing dot (empty component) is rejected",
			platform: "snowflake",
			raw:      "raw.events.",
			wantErr:  true,
		},
		{
			name:     "too many components rejected even for three-level platform",
			platform: "snowflake",
			raw:      "a.b.c.d",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cb, ok := For(tt.platform)
			require.True(t, ok, "expected capability for platform %q", tt.platform)

			got, err := cb.Parse(tt.raw, tt.defaults)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestCapability_Unbounded(t *testing.T) {
	t.Parallel()

	cb, ok := For("dremio")
	require.True(t, ok)
	assert.True(t, cb.Unbounded)

	// Arbitrary depth is accepted; only empty components are rejected.
	require.NoError(t, cb.CheckName("source.folder.subfolder.table"))
	require.NoError(t, cb.CheckName("table"))
	require.Error(t, cb.CheckName("source..table"))
}

func TestTableName_Helpers(t *testing.T) {
	t.Parallel()

	full := TableName{Catalog: "raw", Schema: "analytics", Table: "events"}
	assert.Equal(t, TableName{Catalog: "RAW", Schema: "ANALYTICS", Table: "EVENTS"}, full.Upper())
	assert.Equal(t, "raw.analytics", full.QualifiedSchema("."))
	assert.Equal(t, "raw.analytics.events", full.String("."))

	noCatalog := TableName{Schema: "public", Table: "users"}
	assert.Equal(t, "public", noCatalog.QualifiedSchema("."))
	assert.Equal(t, "public.users", noCatalog.String("."))

	bare := TableName{Table: "events"}
	assert.Empty(t, bare.QualifiedSchema("."))
	assert.Equal(t, "events", bare.String("."))
}

func TestFor_UnknownPlatform(t *testing.T) {
	t.Parallel()
	_, ok := For("not_a_real_platform")
	assert.False(t, ok)
}

func TestSchemaToCreate(t *testing.T) {
	t.Parallel()

	upper := strings.ToUpper
	lower := strings.ToLower

	tests := []struct {
		name      string
		raw       string
		transform func(string) string
		want      string
		wantOK    bool
	}{
		{name: "two-part upper", raw: "analytics.events", transform: upper, want: "ANALYTICS", wantOK: true},
		{name: "three-part qualifies with catalog", raw: "raw.analytics.events", transform: upper, want: "RAW.ANALYTICS", wantOK: true},
		{name: "three-part lower (duckdb)", raw: "cat.main.t", transform: lower, want: "cat.main", wantOK: true},
		{name: "single component has no schema", raw: "events", transform: upper, wantOK: false},
		{name: "four components skipped", raw: "a.b.c.d", transform: upper, wantOK: false},
		{name: "empty component rejected", raw: "raw..events", transform: upper, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := SchemaToCreate(tt.raw, tt.transform)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestContainerToCreate(t *testing.T) {
	t.Parallel()

	upper := strings.ToUpper
	lower := strings.ToLower

	tests := []struct {
		name      string
		raw       string
		transform func(string) string
		want      string
		wantOK    bool
	}{
		{name: "three-part returns the catalog/database (upper)", raw: "raw.analytics.events", transform: upper, want: "RAW", wantOK: true},
		{name: "three-part lower (databricks-ish)", raw: "Main.Silver.Orders", transform: lower, want: "main", wantOK: true},
		{name: "two-part has no parent container", raw: "analytics.events", transform: upper, wantOK: false},
		{name: "single component has no parent container", raw: "events", transform: upper, wantOK: false},
		{name: "four components skipped", raw: "a.b.c.d", transform: upper, wantOK: false},
		{name: "empty component rejected", raw: "raw..events", transform: upper, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ContainerToCreate(tt.raw, tt.transform)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}
