package semantic

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- shared helpers ---

func expectContains(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Fatalf("expected SQL to contain %q\nSQL: %s", want, got)
	}
}

func expectNotContains(t *testing.T, got, unwanted string) {
	t.Helper()
	if strings.Contains(got, unwanted) {
		t.Fatalf("expected SQL NOT to contain %q\nSQL: %s", unwanted, got)
	}
}

func minimalEngine(t *testing.T, m *Model) *Engine {
	t.Helper()
	e, err := NewEngine(m)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	return e
}

// richTestModel exercises every dimension kind, metric kind, segment, and
// window flavor the engine supports. Tests share this model to avoid drift.
func richTestModel() *Model {
	return &Model{
		Name:   "orders",
		Source: Source{Table: "analytics.fct_orders"},
		Dimensions: []Dimension{
			{Name: "order_id", Type: "string", Hidden: true},
			{Name: "status", Type: "string"},
			{Name: "country", Type: "string"},
			{Name: "category", Type: "string"},
			{
				Name: "order_date",
				Type: "time",
				Granularities: map[string]string{
					"day":   "date_trunc('day', order_date)",
					"month": "date_trunc('month', order_date)",
					"year":  "date_trunc('year', order_date)",
				},
			},
			{Name: "is_first_order", Type: "boolean", Expression: "customer_order_number = 1"},
			{Name: "order_size", Type: "string", Expression: "case when amount >= 100 then 'large' else 'small' end"},
		},
		Metrics: []Metric{
			{Name: "revenue", Expression: "sum(amount)"},
			{Name: "cost", Expression: "sum(cogs)"},
			{Name: "order_count", Expression: "count(distinct order_id)"},
			{Name: "row_count", Expression: "count(*)"},
			{Name: "completed_revenue", Expression: "sum(amount)", Filter: "status = 'completed'"},
			{Name: "completed_count", Expression: "count(*)", Filter: "status = 'completed'"},
			{Name: "completed_distinct_orders", Expression: "count(distinct order_id)", Filter: "status = 'completed'"},
			{Name: "profit", Expression: "{revenue} - {cost}"},
			{Name: "avg_order_value", Expression: "{revenue} / {order_count}"},
			{Name: "profit_margin", Expression: "{profit} / {revenue}"},
			{Name: "completion_rate", Expression: "{completed_revenue} / {revenue}"},
			// mixed-expression metric (allowed outside window chains)
			{Name: "raw_aov", Expression: "sum(amount) / {order_count}"},
			{
				Name:       "running_revenue",
				Expression: "{revenue}",
				Window:     &Window{Type: "running_total", OrderBy: "order_date", PartitionBy: []string{"category"}},
			},
			{
				Name:       "prev_revenue",
				Expression: "{revenue}",
				Window:     &Window{Type: "lag", OrderBy: "order_date", PartitionBy: []string{"category"}, Offset: 1},
			},
			{
				Name:       "next_revenue",
				Expression: "{revenue}",
				Window:     &Window{Type: "lead", OrderBy: "order_date", Offset: 2},
			},
			{
				Name:       "revenue_rank",
				Expression: "{revenue}",
				Window:     &Window{Type: "rank", OrderBy: "order_date"},
			},
			{
				Name:       "revenue_share",
				Expression: "{revenue}",
				Window:     &Window{Type: "percent_of_total"},
			},
			{
				Name:       "revenue_growth",
				Expression: "({revenue} - {prev_revenue}) / {prev_revenue}",
			},
		},
		Segments: []Segment{
			{Name: "completed", Filter: "status = 'completed'"},
			{Name: "high_value", Filter: "amount > 100"},
			{Name: "us", Filter: "country = 'US'"},
		},
	}
}

// --- fixture / loader tests ---

func TestLoadDir_FixtureModels(t *testing.T) {
	t.Parallel()

	models, err := LoadDir("testdata/project/semantic")
	if err != nil {
		t.Fatal(err)
	}
	model, ok := models["sales"]
	if !ok {
		t.Fatalf("expected sales model, got %v", Names(models))
	}
	if model.Source.Table != "analytics.orders" {
		t.Fatalf("unexpected source table: %s", model.Source.Table)
	}
}

func TestLoadDir_LoadsNestedModelFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	nested := filepath.Join(dir, "commerce")
	if err := os.MkdirAll(nested, 0o755); err != nil {
		t.Fatalf("create fixture dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(nested, "orders.yml"), []byte("name: orders\nsource:\n  table: analytics.orders\nmetrics:\n  - name: revenue\n    expression: sum(amount)\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	models, err := LoadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := models["orders"]; !ok {
		t.Fatalf("expected nested orders model, got %v", Names(models))
	}
}

func TestLoadDirPartial_ReportsParseErrorsByFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "broken.yml"), []byte("name: broken\nsource:\n  table: ["), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	models, invalid, err := LoadDirPartial(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 0 {
		t.Fatalf("expected no valid models, got %v", Names(models))
	}
	got := invalid["broken.yml"]
	if got == nil {
		t.Fatalf("expected broken.yml in invalid map, got %v", invalid)
	}
	if !strings.Contains(got.Error(), "loading broken.yml") {
		t.Fatalf("expected file path in error, got %q", got.Error())
	}
}

func TestLoadFile_RejectsInvalidModel(t *testing.T) {
	t.Parallel()

	if _, err := LoadFile("testdata/project/semantic/does_not_exist.yml"); err == nil {
		t.Fatal("expected error loading nonexistent file")
	}
}

func TestLoadFile_DefaultsModelSchemaToV1(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "sales.yml")
	if err := os.WriteFile(path, []byte("name: sales\nsource:\n  table: sales\n"), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	model, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load model without schema: %v", err)
	}
	if model.Schema != "v1" {
		t.Fatalf("expected default schema v1, got %q", model.Schema)
	}
}

func TestLoadFile_AcceptsLegacyDacSchemaID(t *testing.T) {
	t.Parallel()

	// dac models written before the engine was shared declared this URL as
	// their schema id; the engine must keep accepting it so those files do
	// not need to be migrated.
	path := filepath.Join(t.TempDir(), "sales.yml")
	body := "schema: https://getbruin.com/schemas/dac/semantic-model/v1\nname: sales\nsource:\n  table: sales\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	model, err := LoadFile(path)
	if err != nil {
		t.Fatalf("load model with legacy schema id: %v", err)
	}
	if model.Schema != "https://getbruin.com/schemas/dac/semantic-model/v1" {
		t.Fatalf("expected legacy schema id preserved, got %q", model.Schema)
	}
}

func TestLoadDir_EmptyDirIsOk(t *testing.T) {
	t.Parallel()

	models, err := LoadDir("")
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 0 {
		t.Fatalf("expected empty map, got %v", models)
	}
}

// --- table-driven SQL generation ---

type sqlCase struct {
	name    string
	query   Query
	must    []string
	mustNot []string
}

func TestGenerateSQL_Cases(t *testing.T) {
	t.Parallel()

	engine := minimalEngine(t, richTestModel())

	cases := []sqlCase{
		// --- dimensions ---
		{
			name:  "single base metric",
			query: Query{Metrics: []string{"revenue"}},
			must:  []string{"SELECT sum(amount) AS revenue", "FROM analytics.fct_orders"},
			mustNot: []string{
				"GROUP BY",
				"WHERE",
				"ORDER BY",
				"LIMIT",
			},
		},
		{
			name: "metric with column dimension",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
			},
			must: []string{
				"SELECT country AS country, sum(amount) AS revenue",
				"GROUP BY 1",
			},
		},
		{
			name: "multiple base metrics",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue", "cost", "row_count"},
			},
			must: []string{
				"sum(amount) AS revenue",
				"sum(cogs) AS cost",
				"count(*) AS row_count",
				"GROUP BY 1",
			},
		},
		{
			name: "time dimension with month granularity",
			query: Query{
				Dimensions: []DimensionRef{{Name: "order_date", Granularity: "month"}},
				Metrics:    []string{"revenue"},
			},
			must: []string{"date_trunc('month', order_date) AS order_date", "GROUP BY 1"},
		},
		{
			name: "time dimension defaults to bare column without granularity",
			query: Query{
				Dimensions: []DimensionRef{{Name: "order_date"}},
				Metrics:    []string{"revenue"},
			},
			must:    []string{"order_date AS order_date"},
			mustNot: []string{"date_trunc"},
		},
		{
			name: "calculated dimension uses expression",
			query: Query{
				Dimensions: []DimensionRef{{Name: "is_first_order"}},
				Metrics:    []string{"revenue"},
			},
			must: []string{"customer_order_number = 1 AS is_first_order"},
		},
		{
			name: "calculated dimension with case expression",
			query: Query{
				Dimensions: []DimensionRef{{Name: "order_size"}},
				Metrics:    []string{"revenue"},
			},
			must: []string{"case when amount >= 100 then 'large' else 'small' end AS order_size"},
		},
		{
			name: "multiple dimensions",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}, {Name: "category"}},
				Metrics:    []string{"revenue"},
			},
			must: []string{
				"country AS country",
				"category AS category",
				"GROUP BY 1, 2",
			},
		},
		{
			name:  "hidden dimension still queryable",
			query: Query{Dimensions: []DimensionRef{{Name: "order_id"}}, Metrics: []string{"row_count"}},
			must:  []string{"order_id AS order_id"},
		},

		// --- filtered base metrics ---
		{
			name:  "sum with filter wraps in CASE WHEN",
			query: Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"completed_revenue"}},
			must:  []string{"sum(CASE WHEN status = 'completed' THEN amount ELSE NULL END) AS completed_revenue"},
		},
		{
			name:  "count star with filter rewrites to CASE WHEN 1",
			query: Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"completed_count"}},
			must:  []string{"count(CASE WHEN status = 'completed' THEN 1 ELSE NULL END) AS completed_count"},
		},
		{
			name:  "count distinct with filter preserves DISTINCT",
			query: Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"completed_distinct_orders"}},
			must:  []string{"count(DISTINCT CASE WHEN status = 'completed' THEN order_id ELSE NULL END) AS completed_distinct_orders"},
		},

		// --- derived metrics ---
		{
			name: "simple derived metric one level",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"avg_order_value"},
			},
			must: []string{"sum(amount) / NULLIF(count(distinct order_id), 0) AS avg_order_value"},
		},
		{
			name: "nested derived metric two levels",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"profit_margin"},
			},
			// profit = revenue - cost; profit_margin = profit / revenue.
			// Inner expansion gets parens because it has top-level `-`.
			must: []string{"(sum(amount) - sum(cogs)) / NULLIF(sum(amount), 0) AS profit_margin"},
		},
		{
			name: "derived subtraction no NULLIF",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"profit"},
			},
			must:    []string{"sum(amount) - sum(cogs) AS profit"},
			mustNot: []string{"NULLIF"},
		},
		{
			name: "derived referencing filtered metric",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"completion_rate"},
			},
			must: []string{
				"sum(CASE WHEN status = 'completed' THEN amount ELSE NULL END) / NULLIF(sum(amount), 0) AS completion_rate",
			},
		},
		{
			name: "mixed expression derived metric",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"raw_aov"},
			},
			must: []string{"sum(amount) / NULLIF(count(distinct order_id), 0) AS raw_aov"},
		},

		// --- segments ---
		{
			name: "single segment lands in WHERE",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Segments:   []string{"completed"},
			},
			must: []string{"WHERE status = 'completed'"},
		},
		{
			name: "multiple segments AND together",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Segments:   []string{"completed", "high_value"},
			},
			must: []string{"WHERE status = 'completed' AND amount > 100"},
		},

		// --- structured filters ---
		{
			name: "filter equals string",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "country", Operator: "equals", Value: "US"}},
			},
			must: []string{"WHERE country = 'US'"},
		},
		{
			name: "filter not_equals",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "status", Operator: "not_equals", Value: "cancelled"}},
			},
			must: []string{"WHERE status != 'cancelled'"},
		},
		{
			name: "filter gte",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "order_date", Operator: "gte", Value: "2024-01-01"}},
			},
			must: []string{"WHERE order_date >= '2024-01-01'"},
		},
		{
			name: "filter in list",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "status", Operator: "in", Value: []string{"shipped", "delivered"}}},
			},
			must: []string{"WHERE status IN ('shipped', 'delivered')"},
		},
		{
			name: "filter not_in list",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "status", Operator: "not_in", Value: []string{"cancelled"}}},
			},
			must: []string{"WHERE status NOT IN ('cancelled')"},
		},
		{
			name: "filter between number range",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "country", Operator: "between", Value: []interface{}{100, 500}}},
			},
			must: []string{"WHERE country BETWEEN 100 AND 500"},
		},
		{
			name: "filter is_null",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "country", Operator: "is_null"}},
			},
			must: []string{"WHERE country IS NULL"},
		},
		{
			name: "filter is_not_null",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "country", Operator: "is_not_null"}},
			},
			must: []string{"WHERE country IS NOT NULL"},
		},
		{
			name: "raw expression filter passes through",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Expression: "country IN ('US', 'CA') OR region = 'EU'"}},
			},
			must: []string{"WHERE country IN ('US', 'CA') OR region = 'EU'"},
		},
		{
			name: "filter expression with dimension ref",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Expression: "{is_first_order} = TRUE"}},
			},
			must: []string{"WHERE customer_order_number = 1 = TRUE"},
		},
		{
			name: "filter expression with metric ref routes to HAVING",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Filters:    []Filter{{Expression: "{revenue} > 1000"}},
			},
			must:    []string{"HAVING sum(amount) > 1000"},
			mustNot: []string{"WHERE sum(amount)"},
		},
		{
			name: "filter expression with derived metric ref routes to HAVING",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"profit"},
				Filters:    []Filter{{Expression: "{profit_margin} > 0.2"}},
			},
			must: []string{"HAVING"},
		},
		{
			name: "mixed WHERE and HAVING from filters",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Filters: []Filter{
					{Dimension: "country", Operator: "equals", Value: "US"},
					{Expression: "{revenue} > 1000"},
				},
			},
			must: []string{"WHERE country = 'US'", "HAVING sum(amount) > 1000"},
		},
		{
			name: "filter combined with segment",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Filters:    []Filter{{Dimension: "country", Operator: "equals", Value: "US"}},
				Segments:   []string{"completed"},
			},
			must: []string{"WHERE country = 'US' AND status = 'completed'"},
		},
		{
			name: "raw aggregate expression filter routes to HAVING",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Filters:    []Filter{{Expression: "sum(amount) > 1000"}},
			},
			must:    []string{"HAVING sum(amount) > 1000"},
			mustNot: []string{"WHERE sum(amount)"},
		},

		// --- sort and limit ---
		{
			name: "sort desc",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Sort:       []SortSpec{{Name: "revenue", Direction: "desc"}},
			},
			must: []string{"ORDER BY revenue DESC"},
		},
		{
			name: "sort default direction is asc",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Sort:       []SortSpec{{Name: "country"}},
			},
			must: []string{"ORDER BY country ASC"},
		},
		{
			name: "multiple sorts",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}, {Name: "category"}},
				Metrics:    []string{"revenue"},
				Sort: []SortSpec{
					{Name: "country", Direction: "asc"},
					{Name: "revenue", Direction: "desc"},
				},
			},
			must: []string{"ORDER BY country ASC, revenue DESC"},
		},
		{
			name: "sort with limit",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country"}},
				Metrics:    []string{"revenue"},
				Sort:       []SortSpec{{Name: "revenue", Direction: "desc"}},
				Limit:      10,
			},
			must: []string{"ORDER BY revenue DESC", "LIMIT 10"},
		},

		// --- window metrics (wrapped queries) ---
		{
			name: "window running_total wraps query",
			query: Query{
				Dimensions: []DimensionRef{{Name: "category"}, {Name: "order_date", Granularity: "month"}},
				Metrics:    []string{"running_revenue"},
			},
			must: []string{
				"FROM (SELECT",
				"sum(amount) AS revenue",
				") base",
				"SUM(base.revenue) OVER (PARTITION BY base.category ORDER BY base.order_date ROWS UNBOUNDED PRECEDING) AS running_revenue",
			},
		},
		{
			name: "window lag uses configured offset",
			query: Query{
				Dimensions: []DimensionRef{{Name: "category"}, {Name: "order_date", Granularity: "month"}},
				Metrics:    []string{"prev_revenue"},
			},
			must: []string{"LAG(base.revenue, 1) OVER (PARTITION BY base.category ORDER BY base.order_date) AS prev_revenue"},
		},
		{
			name: "window lead uses configured offset and skips empty partition_by",
			query: Query{
				Dimensions: []DimensionRef{{Name: "order_date", Granularity: "month"}},
				Metrics:    []string{"next_revenue"},
			},
			must:    []string{"LEAD(base.revenue, 2) OVER (ORDER BY base.order_date) AS next_revenue"},
			mustNot: []string{"PARTITION BY"},
		},
		{
			name: "window rank",
			query: Query{
				Dimensions: []DimensionRef{{Name: "order_date", Granularity: "month"}},
				Metrics:    []string{"revenue_rank"},
			},
			must: []string{"RANK() OVER (ORDER BY base.order_date) AS revenue_rank"},
		},
		{
			name: "window percent_of_total has no order_by or partition",
			query: Query{
				Dimensions: []DimensionRef{{Name: "category"}},
				Metrics:    []string{"revenue_share"},
			},
			must:    []string{"base.revenue / NULLIF(SUM(base.revenue) OVER (), 0) AS revenue_share"},
			mustNot: []string{"PARTITION BY", "ORDER BY base"},
		},
		{
			name: "derived metric over window metric",
			query: Query{
				Dimensions: []DimensionRef{{Name: "category"}, {Name: "order_date", Granularity: "month"}},
				Metrics:    []string{"revenue_growth"},
			},
			must: []string{
				"sum(amount) AS revenue",
				"LAG(base.revenue, 1) OVER",
				"AS revenue_growth",
			},
		},
		{
			name: "window query carries WHERE and segment into inner subquery",
			query: Query{
				Dimensions: []DimensionRef{{Name: "category"}, {Name: "order_date", Granularity: "month"}},
				Metrics:    []string{"running_revenue"},
				Filters:    []Filter{{Dimension: "country", Operator: "equals", Value: "US"}},
				Segments:   []string{"completed"},
			},
			must: []string{
				"WHERE country = 'US' AND status = 'completed'",
				"FROM (SELECT",
				"SUM(base.revenue) OVER",
			},
		},
		{
			name: "window query supports outer ORDER BY and LIMIT",
			query: Query{
				Dimensions: []DimensionRef{{Name: "category"}, {Name: "order_date", Granularity: "month"}},
				Metrics:    []string{"running_revenue"},
				Sort:       []SortSpec{{Name: "order_date", Direction: "asc"}},
				Limit:      5,
			},
			must: []string{") base ORDER BY base.order_date ASC LIMIT 5"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			sql, err := engine.GenerateSQL(&tc.query)
			if err != nil {
				t.Fatalf("GenerateSQL: %v\nquery: %+v", err, tc.query)
			}
			for _, want := range tc.must {
				expectContains(t, sql, want)
			}
			for _, unwanted := range tc.mustNot {
				expectNotContains(t, sql, unwanted)
			}
		})
	}
}

// --- query validation errors ---

func TestGenerateSQL_QueryErrors(t *testing.T) {
	t.Parallel()

	engine := minimalEngine(t, richTestModel())

	cases := []struct {
		name  string
		query Query
		want  string
	}{
		{
			name:  "empty query",
			query: Query{},
			want:  "at least one dimension or metric",
		},
		{
			name:  "unknown metric",
			query: Query{Metrics: []string{"nonexistent"}},
			want:  "metric not found: nonexistent",
		},
		{
			name:  "unknown dimension",
			query: Query{Dimensions: []DimensionRef{{Name: "ghost"}}, Metrics: []string{"revenue"}},
			want:  "dimension not found: ghost",
		},
		{
			name: "unknown segment",
			query: Query{
				Metrics:  []string{"revenue"},
				Segments: []string{"never_defined"},
			},
			want: "segment not found: never_defined",
		},
		{
			name: "invalid granularity",
			query: Query{
				Dimensions: []DimensionRef{{Name: "order_date", Granularity: "fortnight"}},
				Metrics:    []string{"revenue"},
			},
			want: `invalid granularity "fortnight"`,
		},
		{
			name: "granularity on non-time dimension",
			query: Query{
				Dimensions: []DimensionRef{{Name: "country", Granularity: "month"}},
				Metrics:    []string{"revenue"},
			},
			want: "granularity on non-time dimension",
		},
		{
			name: "filter with unknown dimension",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "ghost", Operator: "equals", Value: "x"}},
			},
			want: "filter dimension not found",
		},
		{
			name: "filter with no dimension and no expression",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Operator: "equals", Value: "x"}},
			},
			want: "filter dimension is required",
		},
		{
			name: "sort references unknown field",
			query: Query{
				Metrics: []string{"revenue"},
				Sort:    []SortSpec{{Name: "ghost"}},
			},
			want: "sort field not found: ghost",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := engine.GenerateSQL(&tc.query)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %q", tc.want, err.Error())
			}
		})
	}
}

// --- model-level validation errors ---

func TestNewEngine_ValidationErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		model Model
		want  string
	}{
		{
			name:  "empty model name",
			model: Model{Source: Source{Table: "t"}},
			want:  "model name is required",
		},
		{
			name:  "missing source table",
			model: Model{Name: "m"},
			want:  "source.table is required",
		},
		{
			name: "duplicate name across dim and metric",
			model: Model{
				Name:   "m",
				Source: Source{Table: "t"},
				Dimensions: []Dimension{
					{Name: "shared", Type: "string"},
				},
				Metrics: []Metric{
					{Name: "shared", Expression: "sum(x)"},
				},
			},
			want: "duplicate name: shared",
		},
		{
			name: "metric without expression",
			model: Model{
				Name:    "m",
				Source:  Source{Table: "t"},
				Metrics: []Metric{{Name: "bad"}},
			},
			want: "expression is required",
		},
		{
			name: "derived metric references unknown",
			model: Model{
				Name:    "m",
				Source:  Source{Table: "t"},
				Metrics: []Metric{{Name: "bad", Expression: "{ghost} + 1"}},
			},
			want: "references unknown metric {ghost}",
		},
		{
			name: "circular dependency",
			model: Model{
				Name:   "m",
				Source: Source{Table: "t"},
				Metrics: []Metric{
					{Name: "a", Expression: "{b}"},
					{Name: "b", Expression: "{a}"},
				},
			},
			want: "circular dependency",
		},
		{
			name: "segment without filter",
			model: Model{
				Name:     "m",
				Source:   Source{Table: "t"},
				Metrics:  []Metric{{Name: "n", Expression: "count(*)"}},
				Segments: []Segment{{Name: "s"}},
			},
			want: "filter is required",
		},
		{
			name: "window expression with extra arithmetic",
			model: Model{
				Name:   "m",
				Source: Source{Table: "t"},
				Dimensions: []Dimension{
					{Name: "order_date", Type: "time"},
				},
				Metrics: []Metric{
					{Name: "rev", Expression: "sum(amount)"},
					{
						Name:       "running",
						Expression: "{rev} * 2",
						Window:     &Window{Type: "running_total", OrderBy: "order_date"},
					},
				},
			},
			want: "expression must be exactly a single {ref}",
		},
		{
			name: "window unknown order_by",
			model: Model{
				Name:   "m",
				Source: Source{Table: "t"},
				Dimensions: []Dimension{
					{Name: "order_date", Type: "time"},
				},
				Metrics: []Metric{
					{Name: "rev", Expression: "sum(amount)"},
					{
						Name:       "running",
						Expression: "{rev}",
						Window:     &Window{Type: "running_total", OrderBy: "ghost"},
					},
				},
			},
			want: "order_by references unknown dimension",
		},
		{
			name: "window unknown partition_by",
			model: Model{
				Name:   "m",
				Source: Source{Table: "t"},
				Dimensions: []Dimension{
					{Name: "order_date", Type: "time"},
				},
				Metrics: []Metric{
					{Name: "rev", Expression: "sum(amount)"},
					{
						Name:       "running",
						Expression: "{rev}",
						Window:     &Window{Type: "running_total", OrderBy: "order_date", PartitionBy: []string{"ghost"}},
					},
				},
			},
			want: "partition_by references unknown dimension",
		},
		{
			name: "window unknown type",
			model: Model{
				Name:   "m",
				Source: Source{Table: "t"},
				Dimensions: []Dimension{
					{Name: "order_date", Type: "time"},
				},
				Metrics: []Metric{
					{Name: "rev", Expression: "sum(amount)"},
					{
						Name:       "running",
						Expression: "{rev}",
						Window:     &Window{Type: "fancy", OrderBy: "order_date"},
					},
				},
			},
			want: "unknown window.type",
		},
		{
			name: "window over mixed-aggregation chain",
			model: Model{
				Name:   "m",
				Source: Source{Table: "t"},
				Dimensions: []Dimension{
					{Name: "order_date", Type: "time"},
				},
				Metrics: []Metric{
					{Name: "order_count", Expression: "count(distinct order_id)"},
					{Name: "raw_aov", Expression: "sum(amount) / {order_count}"},
					{
						Name:       "running_aov",
						Expression: "{raw_aov}",
						Window:     &Window{Type: "running_total", OrderBy: "order_date"},
					},
				},
			},
			want: "mixes {refs} with raw aggregation",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewEngine(&tc.model)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %q", tc.want, err.Error())
			}
		})
	}
}

// --- focused regression tests ---

func TestFormatValue_EscapesSingleQuotes(t *testing.T) {
	t.Parallel()

	m := &Model{
		Name:       "t",
		Source:     Source{Table: "t"},
		Dimensions: []Dimension{{Name: "name", Type: "string"}},
		Metrics:    []Metric{{Name: "n", Expression: "count(*)"}},
	}
	engine := minimalEngine(t, m)
	sql, err := engine.GenerateSQL(&Query{
		Metrics: []string{"n"},
		Filters: []Filter{
			{Dimension: "name", Operator: "equals", Value: "O'Brien"},
			{Dimension: "name", Operator: "in", Value: []string{"a'b", "c"}},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "name = 'O''Brien'")
	expectContains(t, sql, "name IN ('a''b', 'c')")
}

func TestFormatValue_BoolRendersAsKeyword(t *testing.T) {
	t.Parallel()

	m := &Model{
		Name:       "t",
		Source:     Source{Table: "t"},
		Dimensions: []Dimension{{Name: "active", Type: "boolean"}},
		Metrics:    []Metric{{Name: "n", Expression: "count(*)"}},
	}
	engine := minimalEngine(t, m)
	sql, err := engine.GenerateSQL(&Query{
		Metrics: []string{"n"},
		Filters: []Filter{{Dimension: "active", Operator: "equals", Value: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "active = TRUE")
}

func TestFilterOnCalculatedDimension(t *testing.T) {
	t.Parallel()

	m := &Model{
		Name:       "t",
		Source:     Source{Table: "orders"},
		Dimensions: []Dimension{{Name: "is_first_order", Type: "boolean", Expression: "customer_order_number = 1"}},
		Metrics:    []Metric{{Name: "n", Expression: "count(*)"}},
	}
	engine := minimalEngine(t, m)
	sql, err := engine.GenerateSQL(&Query{
		Metrics: []string{"n"},
		Filters: []Filter{{Dimension: "is_first_order", Operator: "equals", Value: true}},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "WHERE customer_order_number = 1 = TRUE")
}

func TestJinjaTemplateDelimitersPassThrough(t *testing.T) {
	t.Parallel()

	// The mask/unmask trick must not eat Jinja {{ ... }} placeholders that
	// dashboards splice into raw filter expressions before SQL generation.
	m := &Model{
		Name:       "t",
		Source:     Source{Table: "orders"},
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
		Metrics:    []Metric{{Name: "n", Expression: "count(*)"}},
	}
	engine := minimalEngine(t, m)
	sql, err := engine.GenerateSQL(&Query{
		Metrics: []string{"n"},
		Filters: []Filter{{Expression: "country = '{{ filters.country }}'"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "WHERE country = '{{ filters.country }}'")
}

func TestFilterMetricFilterCannotReferenceAggregatesInWrappedQuery(t *testing.T) {
	t.Parallel()

	// A base metric whose own filter contains an aggregate must be rejected
	// in the wrapped (window) path, since the inner subquery cannot evaluate
	// aggregates inside a per-row CASE WHEN.
	m := &Model{
		Name:   "m",
		Source: Source{Table: "t"},
		Dimensions: []Dimension{
			{Name: "order_date", Type: "time"},
		},
		Metrics: []Metric{
			{Name: "rev", Expression: "sum(amount)"},
			// pathological: filter references another metric (aggregate)
			{Name: "weird", Expression: "sum(amount)", Filter: "{rev} > 100"},
			{
				Name:       "running_weird",
				Expression: "{weird}",
				Window:     &Window{Type: "running_total", OrderBy: "order_date"},
			},
		},
	}
	engine, err := NewEngine(m)
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	_, err = engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "order_date"}},
		Metrics:    []string{"running_weird"},
	})
	if err == nil {
		t.Fatal("expected error: metric filter referencing aggregate in wrapped query")
	}
	if !strings.Contains(err.Error(), "cannot reference aggregates") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWindowMetricAddsRequiredDimensionsToInnerQuery(t *testing.T) {
	t.Parallel()

	engine := minimalEngine(t, richTestModel())
	sql, err := engine.GenerateSQL(&Query{Metrics: []string{"running_revenue"}})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "category AS category")
	expectContains(t, sql, "order_date AS order_date")
	expectContains(t, sql, "GROUP BY 1, 2")
	expectContains(t, sql, "SUM(base.revenue) OVER (PARTITION BY base.category ORDER BY base.order_date ROWS UNBOUNDED PRECEDING)")
}

func TestWindowMetricCanReferenceDerivedMetric(t *testing.T) {
	t.Parallel()

	m := &Model{
		Name:   "m",
		Source: Source{Table: "orders"},
		Dimensions: []Dimension{
			{Name: "order_date", Type: "time"},
		},
		Metrics: []Metric{
			{Name: "revenue", Expression: "sum(amount)"},
			{Name: "cost", Expression: "sum(cogs)"},
			{Name: "profit", Expression: "{revenue} - {cost}"},
			{
				Name:       "running_profit",
				Expression: "{profit}",
				Window:     &Window{Type: "running_total", OrderBy: "order_date"},
			},
		},
	}
	engine := minimalEngine(t, m)
	sql, err := engine.GenerateSQL(&Query{Metrics: []string{"running_profit"}})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "sum(amount) - sum(cogs) AS profit")
	expectContains(t, sql, "SUM(base.profit) OVER (ORDER BY base.order_date ROWS UNBOUNDED PRECEDING) AS running_profit")
}

func TestDerivedMetricOverWindowMetricCollectsWindowInputs(t *testing.T) {
	t.Parallel()

	m := &Model{
		Name:   "m",
		Source: Source{Table: "orders"},
		Dimensions: []Dimension{
			{Name: "order_date", Type: "time"},
		},
		Metrics: []Metric{
			{Name: "revenue", Expression: "sum(amount)"},
			{
				Name:       "running_revenue",
				Expression: "{revenue}",
				Window:     &Window{Type: "running_total", OrderBy: "order_date"},
			},
			{Name: "running_revenue_ratio", Expression: "{running_revenue} / 10"},
		},
	}
	engine := minimalEngine(t, m)
	sql, err := engine.GenerateSQL(&Query{Metrics: []string{"running_revenue_ratio"}})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "sum(amount) AS revenue")
	expectContains(t, sql, "SUM(base.revenue) OVER (ORDER BY base.order_date ROWS UNBOUNDED PRECEDING) / 10 AS running_revenue_ratio")
}

func TestPercentOfTotalUsesPartitionBy(t *testing.T) {
	t.Parallel()

	m := richTestModel()
	for i := range m.Metrics {
		if m.Metrics[i].Name == "revenue_share" {
			m.Metrics[i].Window.PartitionBy = []string{"category"}
			break
		}
	}
	engine := minimalEngine(t, m)
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "category"}},
		Metrics:    []string{"revenue_share"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "base.revenue / NULLIF(SUM(base.revenue) OVER (PARTITION BY base.category), 0) AS revenue_share")
}

func TestStructuredFilterValidationRejectsInvalidOperatorAndValue(t *testing.T) {
	t.Parallel()

	engine := minimalEngine(t, richTestModel())
	cases := []struct {
		name  string
		query Query
		want  string
	}{
		{
			name: "unknown operator",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "country", Operator: "equal", Value: "US"}},
			},
			want: `invalid filter operator "equal"`,
		},
		{
			name: "malformed between",
			query: Query{
				Metrics: []string{"revenue"},
				Filters: []Filter{{Dimension: "order_date", Operator: "between", Value: []interface{}{"2025-01-01"}}},
			},
			want: "invalid between filter value",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := engine.GenerateSQL(&tc.query)
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %q", tc.want, err.Error())
			}
		})
	}
}

func TestJoinGraphJoinsReachableDimension(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "customers", Relationship: "many_to_one", ForeignKey: "customer_id"},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	customers := &Model{
		Name:       "customers",
		Source:     Source{Table: "customers"},
		PrimaryKey: "customer_id",
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{
		"orders":    orders,
		"customers": customers,
	})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "customers.country"}},
		Metrics:    []string{"revenue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "SELECT customers.country AS customers_country, sum(base.order_total) AS revenue")
	expectContains(t, sql, "FROM (SELECT * FROM orders) base")
	expectContains(t, sql, "LEFT JOIN (SELECT * FROM customers) customers ON base.customer_id = customers.customer_id")
	expectContains(t, sql, "GROUP BY 1")
}

func TestJoinGraphRejectsMissingTargetKeyAndPrimaryKey(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "customers", Relationship: "many_to_one", ForeignKey: "customer_id"},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	customers := &Model{
		Name:       "customers",
		Source:     Source{Table: "customers"},
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
	}

	_, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "customers": customers})
	if err == nil {
		t.Fatal("expected missing join target key to fail")
	}
	want := `model "orders": join "customers" requires target_key or primary_key on target model "customers"`
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error containing %q, got %q", want, err.Error())
	}
}

func TestJoinGraphAllowsUnqualifiedDimensionWhenUnambiguous(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "customers", Relationship: "many_to_one", ForeignKey: "customer_id"},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	customers := &Model{
		Name:       "customers",
		Source:     Source{Table: "customers"},
		PrimaryKey: "customer_id",
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "customers": customers})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "country"}},
		Metrics:    []string{"revenue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "customers.country AS country")
	expectContains(t, sql, "SELECT customers.country AS country, sum(base.order_total) AS revenue")
}

func TestJoinGraphRejectsFanoutPath(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "order_items", Relationship: "one_to_many", ForeignKey: "order_id"},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	orderItems := &Model{
		Name:       "order_items",
		Source:     Source{Table: "order_items"},
		Dimensions: []Dimension{{Name: "product_id", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "order_items": orderItems})
	if err != nil {
		t.Fatal(err)
	}
	_, err = engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "order_items.product_id"}},
		Metrics:    []string{"revenue"},
	})
	if err == nil {
		t.Fatal("expected unsafe fanout path to fail")
	}
	if !strings.Contains(err.Error(), "join not found or unsafe: order_items") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestJoinGraphSupportsRemoteStructuredFilter(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "customers", Relationship: "many_to_one", ForeignKey: "customer_id"},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	customers := &Model{
		Name:       "customers",
		Source:     Source{Table: "customers"},
		PrimaryKey: "customer_id",
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "customers": customers})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := engine.GenerateSQL(&Query{
		Metrics: []string{"revenue"},
		Filters: []Filter{{
			Dimension: "customers.country",
			Operator:  "equals",
			Value:     "US",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "LEFT JOIN (SELECT * FROM customers) customers ON base.customer_id = customers.customer_id")
	expectContains(t, sql, "WHERE customers.country = 'US'")
}

func TestJoinGraphSupportsMultiHopSafePath(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "customers", Relationship: "many_to_one", ForeignKey: "customer_id"},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	customers := &Model{
		Name:       "customers",
		Source:     Source{Table: "customers"},
		PrimaryKey: "customer_id",
		Joins: []Join{
			{Name: "countries", Relationship: "many_to_one", ForeignKey: "country_id"},
		},
	}
	countries := &Model{
		Name:       "countries",
		Source:     Source{Table: "countries"},
		PrimaryKey: "country_id",
		Dimensions: []Dimension{{Name: "region", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{
		"orders":    orders,
		"customers": customers,
		"countries": countries,
	})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "countries.region"}},
		Metrics:    []string{"revenue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "LEFT JOIN (SELECT * FROM customers) customers ON base.customer_id = customers.customer_id")
	expectContains(t, sql, "LEFT JOIN (SELECT * FROM countries) countries ON customers.country_id = countries.country_id")
	expectContains(t, sql, "countries.region AS countries_region")
}

func TestJoinGraphUsesTargetModelPrimaryKey(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "customers", Relationship: "many_to_one", ForeignKey: "buyer_id"},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	customers := &Model{
		Name:       "customers",
		Source:     Source{Table: "customers"},
		PrimaryKey: "id",
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "customers": customers})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "customers.country"}},
		Metrics:    []string{"revenue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "ON base.buyer_id = customers.id")
}

func TestJoinGraphSupportsTargetKeyOverride(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "customers", Relationship: "many_to_one", ForeignKey: "buyer_email", TargetKey: "email"},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	customers := &Model{
		Name:       "customers",
		Source:     Source{Table: "customers"},
		PrimaryKey: "id",
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "customers": customers})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "customers.country"}},
		Metrics:    []string{"revenue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "ON base.buyer_email = customers.email")
}

func TestJoinGraphSupportsSQLOverride(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{
				Name:         "customer_tiers",
				Relationship: "many_to_one",
				SQL:          "{orders}.customer_id = {customer_tiers}.customer_id AND {orders}.order_date BETWEEN {customer_tiers}.valid_from AND {customer_tiers}.valid_to",
			},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	customerTiers := &Model{
		Name:       "customer_tiers",
		Source:     Source{Table: "customer_tiers"},
		Dimensions: []Dimension{{Name: "tier", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "customer_tiers": customerTiers})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "customer_tiers.tier"}},
		Metrics:    []string{"revenue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "ON base.customer_id = customer_tiers.customer_id AND base.order_date BETWEEN customer_tiers.valid_from AND customer_tiers.valid_to")
}

func TestJoinGraphUsesRequestedAliasWhenMultipleJoinsTargetSameModel(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "billing_country", Model: "countries", Relationship: "many_to_one", ForeignKey: "billing_country_id"},
			{Name: "shipping_country", Model: "countries", Relationship: "many_to_one", ForeignKey: "shipping_country_id"},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
	}
	countries := &Model{
		Name:       "countries",
		Source:     Source{Table: "countries"},
		PrimaryKey: "id",
		Dimensions: []Dimension{{Name: "region", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "countries": countries})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "shipping_country.region"}},
		Metrics:    []string{"revenue"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "shipping_country.region AS shipping_country_region")
	expectContains(t, sql, "LEFT JOIN (SELECT * FROM countries) shipping_country ON base.shipping_country_id = shipping_country.id")
	expectNotContains(t, sql, "billing_country")
}

func TestJoinGraphQualifiesRootFieldsWhenJoined(t *testing.T) {
	t.Parallel()

	orders := &Model{
		Name:   "orders",
		Source: Source{Table: "orders"},
		Joins: []Join{
			{Name: "customers", Relationship: "many_to_one", ForeignKey: "customer_id"},
		},
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
		Metrics:    []Metric{{Name: "revenue", Expression: "sum(order_total)"}},
		Segments:   []Segment{{Name: "completed", Filter: "status = 'completed'"}},
	}
	customers := &Model{
		Name:       "customers",
		Source:     Source{Table: "customers"},
		PrimaryKey: "customer_id",
		Dimensions: []Dimension{{Name: "segment", Type: "string"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "customers": customers})
	if err != nil {
		t.Fatal(err)
	}
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "country"}, {Name: "customers.segment"}},
		Metrics:    []string{"revenue"},
		Filters:    []Filter{{Dimension: "country", Operator: "equals", Value: "US"}},
		Segments:   []string{"completed"},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "SELECT base.country AS country, customers.segment AS customers_segment, sum(base.order_total) AS revenue")
	expectContains(t, sql, "WHERE base.country = 'US' AND base.status = 'completed'")
	expectContains(t, sql, "GROUP BY 1, 2")
}

func TestWindowSortOnlyDimensionIsAvailableInOuterOrder(t *testing.T) {
	t.Parallel()

	engine := minimalEngine(t, richTestModel())
	sql, err := engine.GenerateSQL(&Query{
		Metrics: []string{"running_revenue"},
		Sort:    []SortSpec{{Name: "country", Direction: "asc"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	expectContains(t, sql, "country AS country")
	expectContains(t, sql, "SUM(base.revenue) OVER")
	expectContains(t, sql, "ORDER BY base.country ASC")
}

func TestRichFixtureSmoke(t *testing.T) {
	t.Parallel()

	// End-to-end smoke against the YAML fixture, mirroring the earlier
	// hand-rolled assertions.
	model, err := LoadFile("testdata/project/semantic/sales.yml")
	if err != nil {
		t.Fatal(err)
	}
	engine := minimalEngine(t, model)
	sql, err := engine.GenerateSQL(&Query{
		Dimensions: []DimensionRef{{Name: "order_date", Granularity: "month"}},
		Metrics:    []string{"avg_order_value"},
		Filters:    []Filter{{Dimension: "country", Operator: "equals", Value: "US"}},
		Segments:   []string{"completed"},
		Sort:       []SortSpec{{Name: "order_date", Direction: "asc"}},
		Limit:      12,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"date_trunc('month', order_date) AS order_date",
		"sum(amount) / NULLIF(count(distinct order_id), 0) AS avg_order_value",
		"FROM analytics.orders",
		"WHERE country = 'US' AND status = 'completed'",
		"GROUP BY 1",
		"ORDER BY order_date ASC",
		"LIMIT 12",
	} {
		expectContains(t, sql, want)
	}
}

func TestGenerateSQLWithColumns_JoinedDimensionAlias(t *testing.T) {
	t.Parallel()

	customers := &Model{
		Name:       "customers",
		Source:     Source{Table: "public.customers"},
		PrimaryKey: "customer_id",
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
	}
	orders := &Model{
		Name:       "orders",
		Source:     Source{Table: "public.orders"},
		PrimaryKey: "order_id",
		Joins:      []Join{{Name: "customers", Relationship: "many_to_one", ForeignKey: "customer_id"}},
		Metrics:    []Metric{{Name: "revenue", Expression: "sum(amount)"}},
	}

	engine, err := NewEngineWithModels(orders, map[string]*Model{"orders": orders, "customers": customers})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}

	sql, cols, err := engine.GenerateSQLWithColumns(&Query{
		Dimensions: []DimensionRef{{Name: "customers.country"}},
		Metrics:    []string{"revenue"},
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !strings.Contains(sql, "AS customers_country") {
		t.Fatalf("expected sanitized alias in SQL, got: %s", sql)
	}

	want := []QueryColumn{
		{Name: "customers_country", Field: "customers.country"},
		{Name: "revenue", Field: "revenue"},
	}
	if len(cols) != len(want) {
		t.Fatalf("expected %d columns, got %d: %+v", len(want), len(cols), cols)
	}
	for i, c := range want {
		if cols[i] != c {
			t.Fatalf("column %d = %+v, want %+v", i, cols[i], c)
		}
	}
}
