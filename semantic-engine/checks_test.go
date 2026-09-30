package semantic

import (
	"database/sql/driver"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/spf13/afero"
)

func checksTestModel() *Model {
	return &Model{
		Name:       "orders",
		Source:     Source{Table: "analytics.orders", Connection: "warehouse"},
		PrimaryKey: "order_id",
		Joins:      []Join{{Name: "customers", Relationship: "many_to_one", ForeignKey: "customer_id"}},
		Dimensions: []Dimension{
			{Name: "order_id", Type: "number", Checks: []Check{{Name: "not_null"}, {Name: "unique"}}},
			{Name: "country", Type: "string", Checks: []Check{{Name: "accepted_values", Value: []interface{}{"US", "DE"}}}},
			{Name: "status", Type: "string", Checks: []Check{{Name: "pattern", Value: "^[a-z]+$"}}},
			{Name: "amount", Type: "number", Checks: []Check{{Name: "non_negative"}, {Name: "min", Value: 0}, {Name: "max", Value: 1000}}},
			{Name: "order_size", Type: "string", Expression: "case when amount >= 100 then 'large' else 'small' end", Checks: []Check{{Name: "not_null"}}},
		},
		Metrics: []Metric{
			{Name: "revenue", Expression: "sum(amount)", Checks: []Check{{Name: "positive"}, {Name: "max", Value: 100000}}},
			{Name: "order_count", Expression: "count(distinct order_id)", Checks: []Check{{Name: "not_null"}, {Name: "equals", Value: 7}}},
			{Name: "avg_order_value", Expression: "{revenue} / {order_count}", Checks: []Check{{Name: "min", Value: 1.5}}},
		},
		Segments: []Segment{{Name: "completed", Filter: "status = 'completed'"}},
		Checks: []ModelCheck{
			{Name: "completed_revenue", Query: &Query{Metrics: []string{"revenue"}, Segments: []string{"completed"}}, Value: 730},
			{Name: "country_count", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}}, Count: int64Ptr(2)},
			{Name: "no_negative_amounts", Query: &Query{Dimensions: []DimensionRef{{Name: "order_id"}}, Filters: []Filter{{Dimension: "amount", Operator: "lt", Value: 0}}}, Count: int64Ptr(0)},
			{Name: "customer_countries", Query: &Query{Dimensions: []DimensionRef{{Name: "customers.country"}}}, Count: int64Ptr(2)},
		},
	}
}

func customersTestModel() *Model {
	return &Model{
		Name:       "customers",
		Source:     Source{Table: "analytics.customers"},
		PrimaryKey: "customer_id",
		Dimensions: []Dimension{{Name: "country", Type: "string"}},
	}
}

func int64Ptr(v int64) *int64 { return &v }

func compiledByID(t *testing.T, checks []CompiledCheck, id string) *CompiledCheck {
	t.Helper()
	for i := range checks {
		if checks[i].ID() == id {
			return &checks[i]
		}
	}
	t.Fatalf("check %q not found", id)
	return nil
}

func TestCountChecks(t *testing.T) {
	t.Parallel()
	if got := CountChecks(checksTestModel()); got != 17 {
		t.Fatalf("CountChecks = %d, want 17", got)
	}
	if got := CountChecks(nil); got != 0 {
		t.Fatalf("CountChecks(nil) = %d, want 0", got)
	}
}

func TestCompileChecksSQL(t *testing.T) {
	t.Parallel()
	models := map[string]*Model{"orders": checksTestModel(), "customers": customersTestModel()}
	checks, err := CompileChecks(models, CompileChecksOptions{Dialect: "duckdb"})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	if len(checks) != 17 {
		t.Fatalf("expected 17 compiled checks, got %d", len(checks))
	}

	cases := map[string]string{
		"orders.dimension.order_id.not_null":       "SELECT count(*) AS bruin_check_value FROM analytics.orders WHERE order_id IS NULL",
		"orders.dimension.order_id.unique":         "SELECT count(order_id) - count(DISTINCT order_id) AS bruin_check_value FROM analytics.orders",
		"orders.dimension.country.accepted_values": "SELECT count(*) AS bruin_check_value FROM analytics.orders WHERE country NOT IN ('US', 'DE')",
		"orders.dimension.status.pattern":          "SELECT count(*) AS bruin_check_value FROM analytics.orders WHERE status !~ '^[a-z]+$'",
		"orders.dimension.amount.non_negative":     "SELECT count(*) AS bruin_check_value FROM analytics.orders WHERE amount < 0",
		"orders.dimension.amount.min":              "SELECT count(*) AS bruin_check_value FROM analytics.orders WHERE amount < 0",
		"orders.dimension.amount.max":              "SELECT count(*) AS bruin_check_value FROM analytics.orders WHERE amount > 1000",
		"orders.dimension.order_size.not_null":     "SELECT count(*) AS bruin_check_value FROM analytics.orders WHERE (case when amount >= 100 then 'large' else 'small' end) IS NULL",
		"orders.metric.revenue.positive":           "SELECT sum(amount) AS revenue FROM analytics.orders",
		"orders.metric.avg_order_value.min":        "SELECT sum(amount) / NULLIF(count(distinct order_id), 0) AS avg_order_value FROM analytics.orders",
		"orders.check.completed_revenue":           "SELECT sum(amount) AS revenue FROM analytics.orders WHERE status = 'completed'",
		"orders.check.country_count":               "SELECT count(*) AS bruin_check_value FROM (SELECT country AS country, sum(amount) AS revenue FROM analytics.orders GROUP BY 1) bruin_check",
		"orders.check.no_negative_amounts":         "SELECT count(*) AS bruin_check_value FROM (SELECT order_id AS order_id FROM analytics.orders WHERE amount < 0 GROUP BY 1) bruin_check",
	}
	for id, want := range cases {
		got := compiledByID(t, checks, id).SQL
		if got != want {
			t.Errorf("%s SQL mismatch\n got: %s\nwant: %s", id, got, want)
		}
	}

	joined := compiledByID(t, checks, "orders.check.customer_countries")
	expectContains(t, joined.SQL, "LEFT JOIN (SELECT * FROM analytics.customers) customers")
	expectContains(t, joined.SQL, ") bruin_check")

	metric := compiledByID(t, checks, "orders.metric.revenue.positive")
	if metric.Scope != CheckScopeMetric || metric.Target != "revenue" || metric.Model != "orders" {
		t.Fatalf("unexpected metric check metadata: %+v", metric)
	}
}

func TestCompileChecksOrderAndModelOrdering(t *testing.T) {
	t.Parallel()
	a := &Model{Name: "b_model", Source: Source{Table: "b"}, Metrics: []Metric{{Name: "n", Expression: "count(*)"}}, Checks: []ModelCheck{{Name: "x", Query: &Query{Metrics: []string{"n"}}}}}
	b := &Model{Name: "a_model", Source: Source{Table: "a"}, Metrics: []Metric{{Name: "n", Expression: "count(*)"}}, Checks: []ModelCheck{{Name: "y", Query: &Query{Metrics: []string{"n"}}}}}
	checks, err := CompileChecks(map[string]*Model{"b_model": a, "a_model": b}, CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	if checks[0].Model != "a_model" || checks[1].Model != "b_model" {
		t.Fatalf("expected checks ordered by model name, got %s then %s", checks[0].Model, checks[1].Model)
	}
}

func TestPatternMismatchSQLByDialect(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"bigquery":   "NOT REGEXP_CONTAINS(status, r'^a''b$')",
		"duckdb":     "status !~ '^a''b$'",
		"postgres":   "status !~ '^a''b$'",
		"snowflake":  "status NOT REGEXP '^a''b$'",
		"mysql":      "NOT (status REGEXP '^a''b$')",
		"clickhouse": "NOT match(status, '^a''b$')",
		"databricks": "NOT (status RLIKE '^a''b$')",
		"tsql":       "status NOT LIKE '^a''b$'",
		"trino":      "NOT REGEXP_LIKE(status, '^a''b$')",
		"":           "NOT REGEXP_LIKE(status, '^a''b$')",
	}
	for dialect, want := range cases {
		if got := patternMismatchSQL("status", "^a'b$", dialect); got != want {
			t.Errorf("dialect %q: got %s, want %s", dialect, got, want)
		}
	}
}

func TestEvaluateDimensionChecks(t *testing.T) {
	t.Parallel()
	models := map[string]*Model{"orders": checksTestModel(), "customers": customersTestModel()}
	checks, err := CompileChecks(models, CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}

	notNull := compiledByID(t, checks, "orders.dimension.order_id.not_null")
	if err := notNull.Evaluate([][]interface{}{{int64(0)}}); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
	err = notNull.Evaluate([][]interface{}{{int64(3)}})
	if err == nil || err.Error() != "dimension 'order_id' has 3 null values" {
		t.Fatalf("unexpected failure message: %v", err)
	}
	if err := notNull.Evaluate([][]interface{}{{1}, {2}}); err == nil || !strings.Contains(err.Error(), "exactly one row") {
		t.Fatalf("expected shape error, got %v", err)
	}
	if err := notNull.Evaluate([][]interface{}{{"abc"}}); err == nil || !strings.Contains(err.Error(), "not a count") {
		t.Fatalf("expected count parse error, got %v", err)
	}

	maxCheck := compiledByID(t, checks, "orders.dimension.amount.max")
	err = maxCheck.Evaluate([][]interface{}{{float64(2)}})
	if err == nil || err.Error() != "dimension 'amount' has 2 values above maximum 1000" {
		t.Fatalf("unexpected failure message: %v", err)
	}
}

func TestEvaluateMetricChecks(t *testing.T) {
	t.Parallel()
	models := map[string]*Model{"orders": checksTestModel(), "customers": customersTestModel()}
	checks, err := CompileChecks(models, CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}

	positive := compiledByID(t, checks, "orders.metric.revenue.positive")
	if err := positive.Evaluate([][]interface{}{{12.5}}); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
	if err := positive.Evaluate([][]interface{}{{int64(0)}}); err == nil || err.Error() != "metric 'revenue' is 0, expected a positive value" {
		t.Fatalf("unexpected failure: %v", err)
	}
	if err := positive.Evaluate([][]interface{}{{nil}}); err == nil || !strings.Contains(err.Error(), "is NULL") {
		t.Fatalf("expected NULL failure, got %v", err)
	}

	notNull := compiledByID(t, checks, "orders.metric.order_count.not_null")
	if err := notNull.Evaluate([][]interface{}{{nil}}); err == nil || err.Error() != "metric 'order_count' is NULL" {
		t.Fatalf("unexpected failure: %v", err)
	}

	equals := compiledByID(t, checks, "orders.metric.order_count.equals")
	if err := equals.Evaluate([][]interface{}{{int32(7)}}); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
	if err := equals.Evaluate([][]interface{}{{"7"}}); err != nil {
		t.Fatalf("expected numeric string to pass, got %v", err)
	}
	if err := equals.Evaluate([][]interface{}{{int64(8)}}); err == nil || err.Error() != "metric 'order_count' is 8, expected 7" {
		t.Fatalf("unexpected failure: %v", err)
	}

	maxCheck := compiledByID(t, checks, "orders.metric.revenue.max")
	if err := maxCheck.Evaluate([][]interface{}{{100000.0}}); err != nil {
		t.Fatalf("expected boundary pass, got %v", err)
	}
	if err := maxCheck.Evaluate([][]interface{}{{100000.5}}); err == nil || err.Error() != "metric 'revenue' is 100000.5, above maximum 100000" {
		t.Fatalf("unexpected failure: %v", err)
	}

	minCheck := compiledByID(t, checks, "orders.metric.avg_order_value.min")
	if err := minCheck.Evaluate([][]interface{}{{1.4}}); err == nil || err.Error() != "metric 'avg_order_value' is 1.4, below minimum 1.5" {
		t.Fatalf("unexpected failure: %v", err)
	}
	if err := minCheck.Evaluate([][]interface{}{{nil}}); err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Fatalf("expected NULL failure, got %v", err)
	}
}

func TestEvaluateModelChecks(t *testing.T) {
	t.Parallel()
	models := map[string]*Model{"orders": checksTestModel(), "customers": customersTestModel()}
	checks, err := CompileChecks(models, CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}

	value := compiledByID(t, checks, "orders.check.completed_revenue")
	if err := value.Evaluate([][]interface{}{{730.0}}); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
	if err := value.Evaluate([][]interface{}{{int64(731)}}); err == nil || err.Error() != "check returned 731, expected 730" {
		t.Fatalf("unexpected failure: %v", err)
	}

	count := compiledByID(t, checks, "orders.check.country_count")
	if err := count.Evaluate([][]interface{}{{int64(2)}}); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
	if err := count.Evaluate([][]interface{}{{int64(3)}}); err == nil || err.Error() != "check returned 3 rows, expected 2" {
		t.Fatalf("unexpected failure: %v", err)
	}

	zeroRows := compiledByID(t, checks, "orders.check.no_negative_amounts")
	if err := zeroRows.Evaluate([][]interface{}{{int64(0)}}); err != nil {
		t.Fatalf("expected zero-row expectation to pass, got %v", err)
	}
	if err := zeroRows.Evaluate([][]interface{}{{int64(1)}}); err == nil || err.Error() != "check returned 1 rows, expected 0" {
		t.Fatalf("unexpected failure: %v", err)
	}
}

func TestModelCheckDefaultsToZeroValue(t *testing.T) {
	t.Parallel()
	model := &Model{
		Name: "m", Source: Source{Table: "t"},
		Metrics: []Metric{{Name: "negatives", Expression: "count(*)", Filter: "amount < 0"}},
		Checks:  []ModelCheck{{Name: "no_negatives", Query: &Query{Metrics: []string{"negatives"}}}},
	}
	checks, err := minimalEngine(t, model).CompileChecks(CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	if err := checks[0].Evaluate([][]interface{}{{int64(0)}}); err != nil {
		t.Fatalf("expected default zero expectation to pass, got %v", err)
	}
	if err := checks[0].Evaluate([][]interface{}{{int64(1)}}); err == nil || err.Error() != "check returned 1, expected 0" {
		t.Fatalf("unexpected failure: %v", err)
	}
}

func TestValuesEqual(t *testing.T) {
	t.Parallel()
	ts := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	cases := []struct {
		expected, actual interface{}
		want             bool
	}{
		{7, int64(7), true},
		{7, 7.0, true},
		{7.5, "7.5", true},
		{0.1, 0.1 + 1e-12, true},
		{7, 8, false},
		{1000000000, int64(1000000001), false},
		{int64(9007199254740992), int64(9007199254740993), false},
		{12.3, big.NewRat(123, 10), true},
		{0, math.Inf(1), false},
		{0, math.NaN(), false},
		{1e9, 1e9 + 1, false},
		{12345678.9, 12345678.900000002, true},
		{1, 0.9999999999999999, true},
		{1000000000, 1e9 + 1, false},
		{1, float32(1.0000001), true},
		{1, float32(1.001), false},
		{"US", "US", true},
		{"US", []byte("US"), true},
		{"US", "DE", false},
		{"2025-01-15", ts, true},
		{"2025-01-15", "2025-01-15T00:00:00Z", true},
		{"2025-01-15T00:00:00Z", "2025-01-15T01:00:00+01:00", true},
		{"2025-01-15T00:00:00.123Z", ts, false},
		// BigQuery DATETIME values print without a time zone.
		{"2025-01-15", "2025-01-15T00:00:00", true},
		{"2025-01-15 00:00:00", "2025-01-15T00:00:00.000000", true},
		{true, 1, true},
		{true, "false", false},
		{false, int64(0), true},
		{nil, nil, true},
		{0, nil, false},
	}
	for _, tc := range cases {
		if got := valuesEqual(tc.expected, tc.actual); got != tc.want {
			t.Errorf("valuesEqual(%v, %v) = %v, want %v", tc.expected, tc.actual, got, tc.want)
		}
	}
}

func TestCompareOrderedDateBounds(t *testing.T) {
	t.Parallel()
	date := time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)
	for _, actual := range []interface{}{date, date.Format(time.RFC3339), []byte("2025-01-15")} {
		for _, tc := range []struct {
			expected string
			want     int
		}{{"2025-01-15", 0}, {"2025-01-14", 1}, {"2025-01-16", -1}} {
			cmp, err := compareOrdered(actual, tc.expected)
			if err != nil || cmp != tc.want {
				t.Fatalf("compareOrdered(%v, %q) = %d, %v; want %d", actual, tc.expected, cmp, err, tc.want)
			}
		}
	}
}

func TestCompareOrderedFloatTolerance(t *testing.T) {
	t.Parallel()
	cases := []struct {
		actual, expected interface{}
		want             int
	}{
		{0.1 + 0.2, 0.3, 0},
		{0.9999999999999999, 1, 0},
		{float32(1.0000001), 1, 0},
		{0.31, 0.3, 1},
		{0.29, 0.3, -1},
		{int64(1000000001), 1000000000, 1},
	}
	for _, tc := range cases {
		cmp, err := compareOrdered(tc.actual, tc.expected)
		if err != nil || cmp != tc.want {
			t.Errorf("compareOrdered(%v, %v) = %d, %v; want %d", tc.actual, tc.expected, cmp, err, tc.want)
		}
	}
}

func TestCheckExactNumericBoundsAndCounts(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"min", "max"} {
		expected := int64(9007199254740992)
		actual := expected + 1
		if name == "min" {
			expected, actual = actual, expected
		}
		model := &Model{Name: "m", Source: Source{Table: "t"}, Metrics: []Metric{{
			Name: "total", Expression: "count(*)", Checks: []Check{{Name: name, Value: expected}},
		}}}
		checks, err := minimalEngine(t, model).CompileChecks(CompileChecksOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := checks[0].Evaluate([][]interface{}{{actual}}); err == nil {
			t.Fatalf("%s should reject %d against %d", name, actual, expected)
		}
		if err := checks[0].Evaluate([][]interface{}{{math.NaN()}}); err == nil {
			t.Fatalf("%s should reject NaN", name)
		}
	}
	model := &Model{Name: "m", Source: Source{Table: "t"}, Dimensions: []Dimension{{Name: "id"}}, Checks: []ModelCheck{{
		Name: "rows", Query: &Query{Dimensions: []DimensionRef{{Name: "id"}}}, Count: int64Ptr(9007199254740992),
	}}}
	checks, err := minimalEngine(t, model).CompileChecks(CompileChecksOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := checks[0].Evaluate([][]interface{}{{int64(9007199254740993)}}); err == nil {
		t.Fatal("unequal row counts should fail")
	}
}

func TestValidationQueriesDeduplicateDiamondJoinDimensions(t *testing.T) {
	t.Parallel()
	regionJoin := Join{Name: "regions", Model: "regions", Relationship: "many_to_one", ForeignKey: "region_id", TargetKey: "id"}
	models := map[string]*Model{
		"orders": {
			Name: "orders", Source: Source{Table: "orders"},
			Joins: []Join{
				{Name: "customers", Model: "customers", Relationship: "many_to_one", ForeignKey: "customer_id", TargetKey: "id"},
				{Name: "stores", Model: "stores", Relationship: "many_to_one", ForeignKey: "store_id", TargetKey: "id"},
			},
			Dimensions: []Dimension{{Name: "status", Type: "string"}},
			Segments:   []Segment{{Name: "s", Filter: "status = 'x'"}},
		},
		"customers": {Name: "customers", Source: Source{Table: "customers"}, Joins: []Join{regionJoin}, Dimensions: []Dimension{{Name: "country", Type: "string"}}},
		"stores":    {Name: "stores", Source: Source{Table: "stores"}, Joins: []Join{regionJoin}, Dimensions: []Dimension{{Name: "city", Type: "string"}}},
		"regions":   {Name: "regions", Source: Source{Table: "regions"}, Dimensions: []Dimension{{Name: "name", Type: "string"}}},
	}
	e, err := NewEngineWithModels(models["orders"], models)
	if err != nil {
		t.Fatalf("NewEngineWithModels: %v", err)
	}
	queries, err := e.ValidationQueries()
	if err != nil {
		t.Fatalf("ValidationQueries: %v", err)
	}
	segment := queries[len(queries)-1]
	if n := strings.Count(segment, "AS regions_name"); n != 1 {
		t.Fatalf("expected regions_name selected once, got %d: %s", n, segment)
	}
}

func TestValidationQueriesRejectUnknownSegmentReference(t *testing.T) {
	t.Parallel()
	model := &Model{Name: "m", Source: Source{Table: "t"}, Segments: []Segment{{Name: "bad", Filter: "{missing} > 0"}}}
	_, err := minimalEngine(t, model).ValidationQueries()
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("expected unknown segment reference error, got %v", err)
	}
}

func TestValidateChecksErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		model *Model
		want  string
	}{
		{
			name: "unknown dimension check",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Dimensions: []Dimension{
				{Name: "d", Checks: []Check{{Name: "relationships"}}},
			}},
			want: `dimension "d": unknown check "relationships"`,
		},
		{
			name: "duplicate dimension check",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Dimensions: []Dimension{
				{Name: "d", Checks: []Check{{Name: "not_null"}, {Name: "not_null"}}},
			}},
			want: `duplicate check "not_null"`,
		},
		{
			name: "value on valueless check",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Dimensions: []Dimension{
				{Name: "d", Checks: []Check{{Name: "not_null", Value: 1}}},
			}},
			want: `check "not_null" does not take a value`,
		},
		{
			name: "min without value",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Dimensions: []Dimension{
				{Name: "d", Checks: []Check{{Name: "min"}}},
			}},
			want: `check "min" requires a value`,
		},
		{
			name: "accepted_values needs list",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Dimensions: []Dimension{
				{Name: "d", Checks: []Check{{Name: "accepted_values", Value: "US"}}},
			}},
			want: `check "accepted_values" requires a non-empty list of values`,
		},
		{
			name: "pattern needs string",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Dimensions: []Dimension{
				{Name: "d", Checks: []Check{{Name: "pattern", Value: 1}}},
			}},
			want: `check "pattern" requires a string pattern value`,
		},
		{
			name: "unique is not a metric check",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Metrics: []Metric{
				{Name: "x", Expression: "count(*)", Checks: []Check{{Name: "unique"}}},
			}},
			want: `metric "x": unknown check "unique"`,
		},
		{
			name: "equals needs value",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Metrics: []Metric{
				{Name: "x", Expression: "count(*)", Checks: []Check{{Name: "equals"}}},
			}},
			want: `check "equals" requires a value`,
		},
		{
			name:  "model check without query",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Checks: []ModelCheck{{Name: "c"}}},
			want:  `model check "c" requires a query`,
		},
		{
			name:  "model check with value and count",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Checks: []ModelCheck{{Name: "c", Query: &Query{}, Value: 1, Count: int64Ptr(1)}}},
			want:  `model check "c" cannot set both value and count`,
		},
		{
			name:  "duplicate model check",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Checks: []ModelCheck{{Name: "c", Query: &Query{}}, {Name: "c", Query: &Query{}}}},
			want:  `duplicate model check "c"`,
		},
		{
			name:  "model check name required",
			model: &Model{Name: "m", Source: Source{Table: "t"}, Checks: []ModelCheck{{Query: &Query{}}}},
			want:  "model check name is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewEngine(tc.model)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestCompileChecksInvalidQuery(t *testing.T) {
	t.Parallel()
	model := &Model{
		Name: "m", Source: Source{Table: "t"}, Metrics: []Metric{{Name: "x", Expression: "count(*)"}},
		Checks: []ModelCheck{{Name: "bad", Query: &Query{Metrics: []string{"missing"}}}},
	}
	e := minimalEngine(t, model)
	_, err := e.CompileChecks(CompileChecksOptions{})
	if err == nil || !strings.Contains(err.Error(), `model "m": check "bad": metric not found: missing`) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidationQueries(t *testing.T) {
	t.Parallel()
	models := map[string]*Model{"orders": checksTestModel(), "customers": customersTestModel()}
	e, err := NewEngineWithModels(models["orders"], models)
	if err != nil {
		t.Fatalf("NewEngineWithModels: %v", err)
	}
	queries, err := e.ValidationQueries()
	if err != nil {
		t.Fatalf("ValidationQueries: %v", err)
	}
	sql := strings.Join(queries, "\n")
	expectContains(t, sql, "order_id AS order_id")
	expectContains(t, sql, "customers.country AS customers_country")
	expectContains(t, sql, "AS avg_order_value")
	expectContains(t, sql, "LEFT JOIN (SELECT * FROM analytics.customers) customers")

	empty := minimalEngine(t, &Model{Name: "empty", Source: Source{Table: "raw.events"}})
	queries, err = empty.ValidationQueries()
	if err != nil {
		t.Fatalf("ValidationQueries: %v", err)
	}
	sql = strings.Join(queries, "\n")
	if sql != "SELECT 1 AS bruin_check_value FROM raw.events" {
		t.Fatalf("unexpected SQL for empty model: %s", sql)
	}
}

func TestLoadModelWithChecksFromYAML(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	content := `schema: v1
name: orders
source:
  table: analytics.orders
  connection: warehouse
dimensions:
  - name: country
    type: string
    checks:
      - name: not_null
      - name: accepted_values
        value: [US, DE]
  - name: order_date
    type: time
    granularities:
      month: date_trunc('month', order_date)
metrics:
  - name: revenue
    expression: sum(amount)
    checks:
      - name: positive
        description: revenue must be positive
  - name: negative_orders
    expression: count(*)
    filter: amount < 0
segments:
  - name: completed
    filter: "status = 'completed'"
checks:
  - name: monthly_rows
    query:
      dimensions: [order_date:month]
      metrics: [revenue]
      segments: [completed]
      filters:
        - dimension: country
          operator: in
          value: [US, DE]
      sort: [revenue:desc]
      limit: 10
    count: 3
  - name: no_negative_amounts
    query:
      metrics: [negative_orders]
    value: 0
  - name: revenue_by_country
    query:
      dimensions: [country]
      metrics: [revenue]
      sort: [revenue:desc]
    value:
      - country: US
        revenue: 600
      - country: DE
        revenue: 210
`
	if err := afero.WriteFile(fs, "/semantic/orders.yml", []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	models, err := LoadDirFS(fs, "/semantic")
	if err != nil {
		t.Fatalf("LoadDirFS: %v", err)
	}
	model := models["orders"]
	if model.Source.Connection != "warehouse" {
		t.Fatalf("expected connection warehouse, got %q", model.Source.Connection)
	}
	if CountChecks(model) != 6 {
		t.Fatalf("expected 6 checks, got %d", CountChecks(model))
	}
	rowsValue, ok := model.Checks[2].Value.([]interface{})
	if !ok || len(rowsValue) != 2 {
		t.Fatalf("expected rows value to parse as a list, got %T", model.Checks[2].Value)
	}
	q := model.Checks[0].Query
	if q.Dimensions[0].Name != "order_date" || q.Dimensions[0].Granularity != "month" {
		t.Fatalf("dimension shorthand not parsed: %+v", q.Dimensions)
	}
	if q.Sort[0].Name != "revenue" || q.Sort[0].Direction != "desc" {
		t.Fatalf("sort shorthand not parsed: %+v", q.Sort)
	}
	if q.Limit != 10 || len(q.Filters) != 1 || q.Filters[0].Operator != "in" {
		t.Fatalf("query not parsed: %+v", q)
	}
	if model.Metrics[0].Checks[0].Description != "revenue must be positive" {
		t.Fatalf("check description not parsed")
	}

	checks, err := CompileChecks(models, CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	monthly := compiledByID(t, checks, "orders.check.monthly_rows")
	expectContains(t, monthly.SQL, "SELECT count(*) AS bruin_check_value FROM (SELECT date_trunc('month', order_date) AS order_date")
	// The count wrapper drops the ORDER BY, which no platform allows inside a
	// derived table and which cannot change a row count.
	if strings.Contains(monthly.SQL, "ORDER BY") {
		t.Fatalf("count wrapper kept the ORDER BY: %s", monthly.SQL)
	}
	expectContains(t, monthly.SQL, "LIMIT 10")
	expectContains(t, monthly.SQL, "country IN ('US', 'DE')")
}

func TestLoadModelKeepsUnquotedDatesAsText(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	content := `schema: v1
name: orders
source:
  table: analytics.orders
dimensions:
  - name: order_date
    type: time
    checks:
      - name: min
        value: 2024-01-01
metrics:
  - name: revenue
    expression: sum(amount)
checks:
  - name: recent_revenue
    query:
      metrics: [revenue]
      filters:
        - dimension: order_date
          operator: between
          value: [2025-02-01, 2025-02-28 23:59:59]
    value:
      - revenue: 10
`
	if err := afero.WriteFile(fs, "/semantic/orders.yml", []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	models, err := LoadDirFS(fs, "/semantic")
	if err != nil {
		t.Fatalf("LoadDirFS: %v", err)
	}
	checks, err := CompileChecks(models, CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	minCheck := compiledByID(t, checks, "orders.dimension.order_date.min")
	expectContains(t, minCheck.SQL, "'2024-01-01'")
	recent := compiledByID(t, checks, "orders.check.recent_revenue")
	expectContains(t, recent.SQL, "BETWEEN '2025-02-01' AND '2025-02-28 23:59:59'")
}

func TestLoadModelCheckExplicitNullValue(t *testing.T) {
	t.Parallel()
	fs := afero.NewMemMapFs()
	content := `schema: v1
name: orders
source:
  table: analytics.orders
metrics:
  - name: revenue
    expression: sum(amount)
checks:
  - name: defaults_to_zero
    query:
      metrics: [revenue]
  - name: expects_null
    query:
      metrics: [revenue]
    value: null
`
	if err := afero.WriteFile(fs, "/semantic/orders.yml", []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	models, err := LoadDirFS(fs, "/semantic")
	if err != nil {
		t.Fatalf("LoadDirFS: %v", err)
	}
	checks, err := CompileChecks(models, CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	zero := compiledByID(t, checks, "orders.check.defaults_to_zero")
	if err := zero.Evaluate([][]interface{}{{int64(0)}}); err != nil {
		t.Fatalf("omitted value should expect 0: %v", err)
	}
	null := compiledByID(t, checks, "orders.check.expects_null")
	if err := null.Evaluate([][]interface{}{{nil}}); err != nil {
		t.Fatalf("value: null should expect NULL: %v", err)
	}
	if err := null.Evaluate([][]interface{}{{int64(0)}}); err == nil {
		t.Fatal("value: null should reject 0")
	}
}

func TestLoadModelRejectsInvalidCheckShapes(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{
			name: "check without name",
			content: `name: orders
source: {table: t}
dimensions:
  - name: d
    checks:
      - value: 1
`,
			want: "schema validation failed",
		},
		{
			name: "unknown key on model check",
			content: `name: orders
source: {table: t}
metrics:
  - name: m
    expression: count(*)
checks:
  - name: c
    query:
      metrics: [m]
    blocking: false
`,
			want: "schema validation failed",
		},
		{
			name: "raw sql is not accepted on model checks",
			content: `name: orders
source: {table: t}
checks:
  - name: c
    sql: select 1
`,
			want: "schema validation failed",
		},
		{
			name: "bad sort direction",
			content: `name: orders
source: {table: t}
metrics:
  - name: m
    expression: count(*)
checks:
  - name: c
    query:
      metrics: [m]
      sort: [m:sideways]
`,
			want: `invalid sort direction "sideways"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fs := afero.NewMemMapFs()
			if err := afero.WriteFile(fs, "/semantic/orders.yml", []byte(tc.content), 0o644); err != nil {
				t.Fatal(err)
			}
			_, err := LoadDirFS(fs, "/semantic")
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestParseDimensionRefAndSortSpec(t *testing.T) {
	t.Parallel()
	ref, err := ParseDimensionRef(" order_date : month ")
	if err != nil || ref.Name != "order_date" || ref.Granularity != "month" {
		t.Fatalf("unexpected result %+v, %v", ref, err)
	}
	if _, err := ParseDimensionRef("order_date:"); err == nil {
		t.Fatal("expected error for empty granularity")
	}
	if _, err := ParseDimensionRef(""); err == nil {
		t.Fatal("expected error for empty ref")
	}

	spec, err := ParseSortSpec("revenue:DESC")
	if err != nil || spec.Name != "revenue" || spec.Direction != "desc" {
		t.Fatalf("unexpected result %+v, %v", spec, err)
	}
	spec, err = ParseSortSpec("revenue")
	if err != nil || spec.Direction != "" {
		t.Fatalf("unexpected result %+v, %v", spec, err)
	}
	if _, err := ParseSortSpec("revenue:sideways"); err == nil {
		t.Fatal("expected error for bad direction")
	}
}

func TestModelCheckExpectedRows(t *testing.T) {
	t.Parallel()
	model := &Model{
		Name:   "orders",
		Source: Source{Table: "analytics.orders"},
		Dimensions: []Dimension{
			{Name: "country", Type: "string"},
			{Name: "order_date", Type: "time", Granularities: map[string]string{"month": "date_trunc('month', order_date)"}},
		},
		Metrics: []Metric{{Name: "revenue", Expression: "sum(amount)"}},
		Checks: []ModelCheck{
			{
				Name: "ordered", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}, Sort: []SortSpec{{Name: "revenue", Direction: "desc"}}},
				Value: []interface{}{
					map[string]interface{}{"country": "US", "revenue": 600},
					map[string]interface{}{"country": "DE", "revenue": 210},
				},
			},
			{
				Name: "unordered", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}},
				Value: []interface{}{
					map[string]interface{}{"country": "DE", "revenue": 210},
					map[string]interface{}{"country": "US", "revenue": 600},
				},
			},
			{
				Name: "partial", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}, Sort: []SortSpec{{Name: "country"}}},
				Value: []interface{}{map[string]interface{}{"country": "DE"}, map[string]interface{}{"country": "US"}},
			},
			{
				Name: "scalars", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Sort: []SortSpec{{Name: "country"}}},
				Value: []interface{}{"DE", "US"},
			},
			{
				Name: "single_row", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}},
				Value: map[string]interface{}{"country": "US", "revenue": 600},
			},
			{
				Name: "with_null", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}},
				Value: []interface{}{map[string]interface{}{"country": nil, "revenue": 5}},
			},
			{
				Name: "granular", Query: &Query{Dimensions: []DimensionRef{{Name: "order_date", Granularity: "month"}}, Metrics: []string{"revenue"}, Sort: []SortSpec{{Name: "order_date"}}},
				Value: []interface{}{map[string]interface{}{"order_date": "2025-01-01", "revenue": 150}},
			},
		},
	}
	checks, err := minimalEngine(t, model).CompileChecks(CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	us := []interface{}{"US", int64(600)}
	de := []interface{}{"DE", 210.0}

	ordered := compiledByID(t, checks, "orders.check.ordered")
	if err := ordered.Evaluate([][]interface{}{us, de}); err != nil {
		t.Fatalf("expected ordered pass, got %v", err)
	}
	if err := ordered.Evaluate([][]interface{}{de, us}); err == nil || err.Error() != "row 1: column 'country' is DE, expected US" {
		t.Fatalf("unexpected ordered failure: %v", err)
	}
	if err := ordered.Evaluate([][]interface{}{us}); err == nil || err.Error() != "check returned 1 rows, expected 2" {
		t.Fatalf("unexpected row count failure: %v", err)
	}
	if err := ordered.Evaluate([][]interface{}{us, {"DE", 211}}); err == nil || err.Error() != "row 2: column 'revenue' is 211, expected 210" {
		t.Fatalf("unexpected cell failure: %v", err)
	}

	unordered := compiledByID(t, checks, "orders.check.unordered")
	if err := unordered.Evaluate([][]interface{}{us, de}); err != nil {
		t.Fatalf("expected unordered pass, got %v", err)
	}
	if err := unordered.Evaluate([][]interface{}{us, {"FR", 210}}); err == nil || err.Error() != "the expected rows cannot all be matched to distinct returned rows; no unused row is left for expected row 1 (country: DE, revenue: 210)" {
		t.Fatalf("unexpected unordered failure: %v", err)
	}

	partial := compiledByID(t, checks, "orders.check.partial")
	if err := partial.Evaluate([][]interface{}{de, us}); err != nil {
		t.Fatalf("expected partial pass, got %v", err)
	}

	scalars := compiledByID(t, checks, "orders.check.scalars")
	if err := scalars.Evaluate([][]interface{}{{"DE"}, {"US"}}); err != nil {
		t.Fatalf("expected scalar rows pass, got %v", err)
	}
	if err := scalars.Evaluate([][]interface{}{{"US"}, {"DE"}}); err == nil {
		t.Fatal("expected ordered scalar rows to fail")
	}

	single := compiledByID(t, checks, "orders.check.single_row")
	if err := single.Evaluate([][]interface{}{us}); err != nil {
		t.Fatalf("expected single row pass, got %v", err)
	}
	if err := single.Evaluate([][]interface{}{us, de}); err == nil || err.Error() != "check returned 2 rows, expected 1" {
		t.Fatalf("unexpected single row failure: %v", err)
	}

	withNull := compiledByID(t, checks, "orders.check.with_null")
	if err := withNull.Evaluate([][]interface{}{{nil, 5}}); err != nil {
		t.Fatalf("expected null match, got %v", err)
	}
	if err := withNull.Evaluate([][]interface{}{{"US", 5}}); err == nil {
		t.Fatal("expected null mismatch to fail")
	}

	granular := compiledByID(t, checks, "orders.check.granular")
	if err := granular.Evaluate([][]interface{}{{time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), 150}}); err != nil {
		t.Fatalf("expected time match, got %v", err)
	}
}

// An unordered check may list overlapping partial rows, where a less specific
// expectation can also match the row a more specific one needs. The match must
// not depend on the order the warehouse returned the rows in.
func TestModelCheckUnorderedRowsWithOverlappingExpectations(t *testing.T) {
	t.Parallel()
	model := &Model{
		Name:       "orders",
		Source:     Source{Table: "analytics.orders"},
		Dimensions: []Dimension{{Name: "country"}, {Name: "status"}},
		Metrics:    []Metric{{Name: "revenue", Expression: "sum(amount)"}},
		Checks: []ModelCheck{{
			Name:  "overlapping",
			Query: &Query{Dimensions: []DimensionRef{{Name: "country"}, {Name: "status"}}, Metrics: []string{"revenue"}},
			Value: []interface{}{
				map[string]interface{}{"country": "US"},
				map[string]interface{}{"country": "US", "status": "completed"},
			},
		}},
	}
	checks, err := minimalEngine(t, model).CompileChecks(CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	check := compiledByID(t, checks, "orders.check.overlapping")

	completed := []interface{}{"US", "completed", 100}
	pending := []interface{}{"US", "pending", 50}
	if err := check.Evaluate([][]interface{}{completed, pending}); err != nil {
		t.Fatalf("expected pass, got %v", err)
	}
	if err := check.Evaluate([][]interface{}{pending, completed}); err != nil {
		t.Fatalf("expected pass in the opposite row order, got %v", err)
	}
	if err := check.Evaluate([][]interface{}{pending, {"US", "refunded", 10}}); err == nil {
		t.Fatal("expected a failure when no assignment exists")
	}
}

func TestValidateChecksRejectsWindowMetricChecks(t *testing.T) {
	t.Parallel()
	model := &Model{
		Name:       "orders",
		Source:     Source{Table: "analytics.orders"},
		Dimensions: []Dimension{{Name: "order_date", Type: "time"}},
		Metrics: []Metric{
			{Name: "revenue", Expression: "sum(amount)"},
			{
				Name:       "running_revenue",
				Expression: "{revenue}",
				Window:     &Window{Type: "running_total", OrderBy: "order_date"},
				Checks:     []Check{{Name: "positive"}},
			},
		},
	}
	_, err := NewEngine(model)
	if err == nil || err.Error() != `metric "running_revenue": checks are not supported on window metrics` {
		t.Fatalf("unexpected error: %v", err)
	}

	// A metric derived from a window metric produces the same multi-row SQL.
	model.Metrics[1].Checks = nil
	model.Metrics = append(model.Metrics, Metric{Name: "double_running", Expression: "{running_revenue} * 2", Checks: []Check{{Name: "positive"}}})
	_, err = NewEngine(model)
	if err == nil || err.Error() != `metric "double_running": checks are not supported on window metrics` {
		t.Fatalf("unexpected error for the derived metric: %v", err)
	}
}

// SQL Server and its relatives reject ORDER BY inside a derived table, and both
// the count wrapper and the validation probe wrap the query in one.
func TestModelCheckSortedQueriesAvoidOrderByInDerivedTables(t *testing.T) {
	t.Parallel()
	model := &Model{
		Name:       "orders",
		Source:     Source{Table: "analytics.orders"},
		Dimensions: []Dimension{{Name: "country"}},
		Metrics:    []Metric{{Name: "revenue", Expression: "sum(amount)"}},
		Checks: []ModelCheck{
			{
				Name:  "counted",
				Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}, Sort: []SortSpec{{Name: "revenue", Direction: "desc"}}},
				Count: int64Ptr(2),
			},
			{
				Name:  "rows",
				Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}, Sort: []SortSpec{{Name: "revenue", Direction: "desc"}}},
				Value: []interface{}{map[string]interface{}{"country": "US"}, map[string]interface{}{"country": "DE"}},
			},
			{
				Name:  "unsorted",
				Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}},
				Value: []interface{}{map[string]interface{}{"country": "US"}, map[string]interface{}{"country": "DE"}},
			},
		},
	}
	checks, err := minimalEngine(t, model).CompileChecks(CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}

	counted := compiledByID(t, checks, "orders.check.counted")
	if strings.Contains(counted.SQL, "ORDER BY") {
		t.Fatalf("count wrapper kept the ORDER BY: %s", counted.SQL)
	}
	if counted.ValidationSQL != "" {
		t.Fatalf("count checks need no separate validation SQL, got %s", counted.ValidationSQL)
	}

	rows := compiledByID(t, checks, "orders.check.rows")
	expectContains(t, rows.SQL, "ORDER BY revenue DESC")
	if strings.Contains(rows.ValidationSQL, "ORDER BY") {
		t.Fatalf("validation SQL kept the ORDER BY: %s", rows.ValidationSQL)
	}

	unsorted := compiledByID(t, checks, "orders.check.unsorted")
	if unsorted.ValidationSQL != "" {
		t.Fatalf("unsorted checks need no separate validation SQL, got %s", unsorted.ValidationSQL)
	}
}

// A window-wrapped query promotes sort dimensions into the inner GROUP BY, so
// there the sort is part of the grouping and must be kept.
func TestModelCheckKeepsSortWhenItChangesGrouping(t *testing.T) {
	t.Parallel()
	model := &Model{
		Name:       "orders",
		Source:     Source{Table: "analytics.orders"},
		Dimensions: []Dimension{{Name: "region"}, {Name: "order_date", Type: "time"}},
		Metrics: []Metric{
			{Name: "revenue", Expression: "sum(amount)"},
			{Name: "running_revenue", Expression: "{revenue}", Window: &Window{Type: "running_total", OrderBy: "order_date"}},
		},
		Checks: []ModelCheck{{
			Name:  "grouped",
			Query: &Query{Metrics: []string{"running_revenue"}, Sort: []SortSpec{{Name: "region"}}},
			Count: int64Ptr(4),
		}},
	}
	checks, err := minimalEngine(t, model).CompileChecks(CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	grouped := compiledByID(t, checks, "orders.check.grouped")
	// Dropping the sort here would drop region from the inner GROUP BY and
	// count a different number of rows, so the sort stays.
	expectContains(t, grouped.SQL, "ORDER BY base.region ASC) bruin_check")
	expectContains(t, grouped.SQL, "SELECT region AS region, order_date AS order_date")
}

type cellValuer struct{ value driver.Value }

func (c cellValuer) Value() (driver.Value, error) { return c.value, nil }

// Some drivers wrap values, such as pgx returning Postgres NUMERIC as a
// pgtype.Numeric. Row comparisons must see the underlying value.
func TestModelCheckExpectedRowsUnwrapsDriverValues(t *testing.T) {
	t.Parallel()
	model := &Model{
		Name:       "orders",
		Source:     Source{Table: "analytics.orders"},
		Dimensions: []Dimension{{Name: "country"}},
		Metrics:    []Metric{{Name: "revenue", Expression: "sum(amount)"}},
		Checks: []ModelCheck{{
			Name:  "rows",
			Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}, Sort: []SortSpec{{Name: "country"}}},
			Value: []interface{}{map[string]interface{}{"country": "US", "revenue": 600}},
		}},
	}
	checks, err := minimalEngine(t, model).CompileChecks(CompileChecksOptions{})
	if err != nil {
		t.Fatalf("CompileChecks: %v", err)
	}
	check := compiledByID(t, checks, "orders.check.rows")
	if err := check.Evaluate([][]interface{}{{"US", cellValuer{"600"}}}); err != nil {
		t.Fatalf("expected the wrapped value to match, got %v", err)
	}
	err = check.Evaluate([][]interface{}{{"US", cellValuer{"601"}}})
	if err == nil || err.Error() != "row 1: column 'revenue' is 601, expected 600" {
		t.Fatalf("unexpected mismatch message: %v", err)
	}
}

func TestModelCheckExpectedRowsErrors(t *testing.T) {
	t.Parallel()
	base := func(check ModelCheck) *Model {
		return &Model{
			Name:       "orders",
			Source:     Source{Table: "t"},
			Dimensions: []Dimension{{Name: "country"}},
			Metrics:    []Metric{{Name: "revenue", Expression: "sum(amount)"}},
			Checks:     []ModelCheck{check},
		}
	}
	cases := []struct {
		name  string
		check ModelCheck
		want  string
	}{
		{
			name:  "unknown column",
			check: ModelCheck{Name: "c", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}}, Value: []interface{}{map[string]interface{}{"region": "EU"}}},
			want:  `value[0] references unknown column "region"; query columns: country`,
		},
		{
			name:  "bare value with several columns",
			check: ModelCheck{Name: "c", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}}, Value: []interface{}{"US"}},
			want:  "value[0] is a bare value but the query returns 2 columns",
		},
		{
			name:  "empty row mapping",
			check: ModelCheck{Name: "c", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}}, Value: []interface{}{map[string]interface{}{}}},
			want:  "value[0]: expected row must list at least one column",
		},
		{
			name:  "nested value",
			check: ModelCheck{Name: "c", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}}, Value: []interface{}{map[string]interface{}{"country": []interface{}{"US"}}}},
			want:  `column "country" must be a number, string, boolean, or null`,
		},
		{
			name:  "list of lists",
			check: ModelCheck{Name: "c", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}}, Value: []interface{}{[]interface{}{"US"}}},
			want:  "value[0] must be a number, string, boolean, null, or a mapping",
		},
		{
			name:  "scalar value with several columns",
			check: ModelCheck{Name: "c", Query: &Query{Dimensions: []DimensionRef{{Name: "country"}}, Metrics: []string{"revenue"}}, Value: 5},
			want:  "a scalar value needs a single-column query, got 2 columns",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			e, err := NewEngine(base(tc.check))
			if err == nil {
				_, err = e.CompileChecks(CompileChecksOptions{})
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}
