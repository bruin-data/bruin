package cmd

import (
	"errors"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func createEmployeeColumns() []pipeline.Column {
	return []pipeline.Column{
		{Name: "id", Type: "str", PrimaryKey: true},
		{Name: "name", Type: "str"},
		{Name: "age", Type: "int64"},
	}
}

func createJoinColumns() []pipeline.Column {
	return []pipeline.Column{
		{Name: "a", Type: "str"},
		{Name: "b", Type: "int64"},
		{Name: "c", Type: "str"},
		{Name: "b2", Type: "int64"},
		{Name: "c2", Type: "str"},
	}
}

const complexJoinQuery = `
	with t1 as (
		select *
		from table1
		join table2
			using(a)
	),
	t2 as (
		select *
		from table2
		left join table1
			using(a)
	)
	select t1.*, t2.b as b2, t2.c as c2
	from t1
	join t2
		using(a)
`

func createComplexJoinPipeline() *pipeline.Pipeline {
	return &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name: "table1",
				Columns: []pipeline.Column{
					{Name: "a", Type: "str"},
					{Name: "b", Type: "int64"},
				},
			},
			{
				Name: "table2",
				Columns: []pipeline.Column{
					{Name: "a", Type: "str"},
					{Name: "c", Type: "str"},
				},
			},
		},
	}
}

func TestInternalParse_Run(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		pipeline     *pipeline.Pipeline
		beforeAssets *pipeline.Asset
		afterAssets  *pipeline.Asset
		wantCount    int
		wantColumns  []pipeline.Column
		want         error
	}{
		{
			name: "simple select all query",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name:    "employees",
						Columns: createEmployeeColumns(),
					},
				},
			},
			beforeAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: "select * from employees",
				},
				Upstreams: []pipeline.Upstream{{Value: "employees"}},
			},
			afterAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: "select * from employees",
				},
				Columns:   createEmployeeColumns(),
				Upstreams: []pipeline.Upstream{{Value: "employees"}},
			},
			wantCount:   3,
			wantColumns: createEmployeeColumns(),
			want:        nil,
		},
		{
			name: "simple select all query wihtout upstream",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name:    "employees",
						Columns: createEmployeeColumns(),
					},
				},
			},
			beforeAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: "select * from employees",
				},
				Upstreams: []pipeline.Upstream{},
			},
			afterAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: "select * from employees",
				},
				Columns:   []pipeline.Column{},
				Upstreams: []pipeline.Upstream{},
			},
			wantCount:   0,
			wantColumns: []pipeline.Column{},
			want:        nil,
		},
		{
			name:     "complex join query",
			pipeline: createComplexJoinPipeline(),
			beforeAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: complexJoinQuery,
				},
				Columns:   []pipeline.Column{},
				Upstreams: []pipeline.Upstream{{Value: "table1"}, {Value: "table2"}},
			},
			afterAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: complexJoinQuery,
				},
				Columns:   createJoinColumns(),
				Upstreams: []pipeline.Upstream{{Value: "table1"}, {Value: "table2"}},
			},
			wantCount:   5,
			wantColumns: createJoinColumns(),
			want:        nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runSingleParseTest(t, tt.pipeline, tt.beforeAssets, tt.afterAssets, tt.wantColumns, tt.wantCount, tt.want)
		})
	}
}

func runSingleParseTest(t *testing.T, p *pipeline.Pipeline, before, after *pipeline.Asset, wantCols []pipeline.Column, wantCount int, want error) {
	t.Helper()

	err := ParseLineage(p, before)
	if !errors.Is(err, want) {
		t.Errorf("ParseLineage() error = %v, want %v", err, want)
	}

	if after != nil {
		assertColumns(t, after.Columns, wantCols, wantCount)
	}
}

func TestParseLineageRecursively(t *testing.T) {
	t.Parallel()

	testCases := map[string]func(*testing.T){
		"basic recursive parsing":   testBasicRecursiveParsing,
		"joins and complex queries": testJoinsAndComplexQueries,
		"advanced SQL features":     testAdvancedSQLFeatures,
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tc(t)
		})
	}
}

func runLineageTests(t *testing.T, tests []struct {
	name     string
	pipeline *pipeline.Pipeline
	after    *pipeline.Pipeline
	want     error
},
) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runSingleLineageTest(t, tt.pipeline, tt.after, tt.want)
		})
	}
}

func runSingleLineageTest(t *testing.T, p, after *pipeline.Pipeline, want error) {
	t.Helper()

	for _, asset := range p.Assets {
		err := parseLineageRecursive(p, asset)
		assertLineageError(t, err, want)
		assertAssetExists(t, after, asset)
	}
}

func assertLineageError(t *testing.T, got, want error) {
	t.Helper()

	if want == nil {
		if got != nil {
			t.Errorf("parseLineageRecursive() error = %v, want nil", got)
		}
		return
	}

	if got == nil || got.Error() != want.Error() {
		t.Errorf("parseLineageRecursive() error = %v, want %v", got, want)
	}
}

func assertAssetExists(t *testing.T, p *pipeline.Pipeline, _ *pipeline.Asset) {
	t.Helper()

	for _, expectedAsset := range p.Assets {
		assetFound := p.GetAssetByName(expectedAsset.Name)
		if assetFound == nil {
			t.Errorf("Asset %s not found in pipeline", expectedAsset.Name)
		}
	}
}

func testBasicRecursiveParsing(t *testing.T) {
	tests := []struct {
		name     string
		pipeline *pipeline.Pipeline
		after    *pipeline.Pipeline
		want     error
	}{
		{
			name: "successful recursive lineage parsing",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "table1",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT * FROM table2",
						},
						Upstreams: []pipeline.Upstream{{Value: "table2"}},
					},
					{
						Name: "table2",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT * FROM table3",
						},
						Upstreams: []pipeline.Upstream{{Value: "table3"}},
					},
					{
						Name: "table3",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "age", Type: "int64"},
						},
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT id,name,age FROM table3",
						},
					},
				},
			},
			after: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "table1",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT * FROM table2",
						},
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "age", Type: "int64"},
						},
						Upstreams: []pipeline.Upstream{{Value: "table2"}},
					},
					{
						Name: "table2",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT * FROM table3",
						},
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "age", Type: "int64"},
						},
						Upstreams: []pipeline.Upstream{{Value: "table3"}},
					},
					{
						Name: "table3",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "age", Type: "int64"},
						},
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT id,name,age FROM table3",
						},
					},
				},
			},
			want: nil,
		},
		{
			name: "successful recursive lineage parsing",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "table1",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT name FROM table2",
						},
						Upstreams: []pipeline.Upstream{{Value: "table2"}},
					},
					{
						Name: "table2",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT id, age FROM table3",
						},
						Upstreams: []pipeline.Upstream{{Value: "table3"}},
					},
					{
						Name: "table3",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "age", Type: "int64"},
						},
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT id,name,age FROM table3",
						},
					},
				},
			},
			after: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "table1",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT name FROM table2",
						},
						Columns: []pipeline.Column{
							{Name: "name", Type: "str"},
						},
						Upstreams: []pipeline.Upstream{{Value: "table2"}},
					},
					{
						Name: "table2",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT id, age FROM table3",
						},
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "age", Type: "int64"},
						},
						Upstreams: []pipeline.Upstream{{Value: "table3"}},
					},
					{
						Name: "table3",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "age", Type: "int64"},
						},
						ExecutableFile: pipeline.ExecutableFile{
							Content: "SELECT id,name,age FROM table3",
						},
					},
				},
			},
			want: nil,
		},
	}
	runLineageTests(t, tests)
}

func testJoinsAndComplexQueries(t *testing.T) {
	tests := []struct {
		name     string
		pipeline *pipeline.Pipeline
		after    *pipeline.Pipeline
		want     error
	}{
		{
			name: "successful recursive lineage parsing with joins",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "analytics",
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
select 
    a.name, 
    b.country 
from people a 
join country b on a.id = b.id;`,
						},
						Upstreams: []pipeline.Upstream{{Value: "country"}, {Value: "people"}},
					},
					{
						Name: "country",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "select id, country from users;",
						},
						Upstreams: []pipeline.Upstream{{Value: "users"}},
					},
					{
						Name: "people",
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
select 
    id, 
    name, 
    last_name,
    now() as current_timestamp 
from users;`,
						},
						Upstreams: []pipeline.Upstream{{Value: "users"}},
					},
					{
						Name: "users",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "last_name", Type: "str"},
							{Name: "country", Type: "str"},
							{Name: "created_at", Type: "timestamp"},
						},
						ExecutableFile: pipeline.ExecutableFile{
							Content: "select *  from user_data;",
						},
					},
				},
			},
			after: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "analytics",
						Columns: []pipeline.Column{
							{Name: "name", Type: "str"},
							{Name: "country", Type: "str"},
						},
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
select 
    a.name, 
    b.country 
from people a 
join country b on a.id = b.id;`,
						},
						Upstreams: []pipeline.Upstream{{Value: "country"}, {Value: "people"}},
					},
					{
						Name: "country",
						ExecutableFile: pipeline.ExecutableFile{
							Content: "select id, country from users;",
						},
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "country", Type: "str"},
						},
						Upstreams: []pipeline.Upstream{{Value: "users"}},
					},
					{
						Name: "people",
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
select 
    id, 
    name, 
    last_name,
    now() as current_timestamp 
from users;`,
						},
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "last_name", Type: "str"},
						},
						Upstreams: []pipeline.Upstream{{Value: "users"}},
					},
					{
						Name: "users",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "last_name", Type: "str"},
							{Name: "country", Type: "str"},
							{Name: "created_at", Type: "timestamp"},
						},
						ExecutableFile: pipeline.ExecutableFile{
							Content: "select *  from user_data;",
						},
					},
				},
			},
			want: nil,
		},
		{
			name: "complex subqueries with aliases and functions",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "user_segments",
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
SELECT 
    user_id,
    CASE 
        WHEN age < 18 THEN 'minor'
        WHEN age BETWEEN 18 AND 65 THEN 'adult'
        ELSE 'senior'
    END as age_group,
    CAST(signup_date AS DATE) as signup_day,
    CAST(ROUND(amount * 100) AS INT) as amount_cents
FROM users
WHERE CAST(signup_date AS DATE) >= '2023-01-01'`,
						},
						Upstreams: []pipeline.Upstream{{Value: "users"}},
					},
					{
						Name: "users",
						Columns: []pipeline.Column{
							{Name: "user_id", Type: "int64"},
							{Name: "age", Type: "int64"},
							{Name: "signup_date", Type: "timestamp"},
							{Name: "amount", Type: "float64"},
						},
					},
				},
			},
			after: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "user_segments",
						Columns: []pipeline.Column{
							{Name: "user_id", Type: "int64"},
							{Name: "age_group", Type: "str"},
							{Name: "signup_day", Type: "date"},
							{Name: "amount_cents", Type: "int64"},
						},
						Upstreams: []pipeline.Upstream{{Value: "users"}},
					},
				},
			},
			want: nil,
		},
		{
			name: "recursive CTEs with window functions",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "employee_hierarchy",
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
WITH RECURSIVE emp_tree AS (
    SELECT 
        id, 
        name,
        manager_id,
        1 as level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    SELECT 
        e.id,
        e.name,
        e.manager_id,
        et.level + 1
    FROM employees e
    JOIN emp_tree et ON e.manager_id = et.id
)
SELECT 
    name,
    level,
    ROW_NUMBER() OVER (PARTITION BY level ORDER BY name) as rank
FROM emp_tree`,
						},
						Upstreams: []pipeline.Upstream{{Value: "employees"}},
					},
					{
						Name: "employees",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "manager_id", Type: "int64"},
						},
					},
				},
			},
			after: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "employee_hierarchy",
						Columns: []pipeline.Column{
							{Name: "name", Type: "str"},
							{Name: "level", Type: "int64"},
							{Name: "rank", Type: "int64"},
						},
						Upstreams: []pipeline.Upstream{{Value: "employees"}},
					},
				},
			},
			want: nil,
		},
		{
			name: "union with different column names",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "combined_data",
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
SELECT 
    user_id as id,
    'customer' as type,
    email as contact
FROM customers
UNION ALL
SELECT 
    vendor_id,
    'vendor',
    phone_number
FROM vendors`,
						},
						Upstreams: []pipeline.Upstream{{Value: "customers"}, {Value: "vendors"}},
					},
					{
						Name: "customers",
						Columns: []pipeline.Column{
							{Name: "user_id", Type: "int64"},
							{Name: "email", Type: "str"},
						},
					},
					{
						Name: "vendors",
						Columns: []pipeline.Column{
							{Name: "vendor_id", Type: "int64"},
							{Name: "phone_number", Type: "str"},
						},
					},
				},
			},
			after: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "combined_data",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "type", Type: "str"},
							{Name: "contact", Type: "str"},
						},
						Upstreams: []pipeline.Upstream{{Value: "customers"}, {Value: "vendors"}},
					},
				},
			},
			want: nil,
		},
		{
			name: "nested subqueries with multiple CTEs",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "sales_report",
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
WITH monthly_sales AS (
    SELECT 
        DATE_TRUNC('month', order_date) as month,
        product_id,
        SUM(quantity) as total_quantity,
        SUM(amount) as total_amount
    FROM orders
    GROUP BY 1, 2
),
product_ranks AS (
    SELECT 
        month,
        product_id,
        total_amount,
        ROW_NUMBER() OVER (PARTITION BY month ORDER BY total_amount DESC) as rank
    FROM monthly_sales
),
top_products AS (
    SELECT DISTINCT product_id
    FROM product_ranks
    WHERE rank <= 5
)
SELECT 
    p.name,
    p.category,
    ms.month,
    ms.total_quantity,
    ms.total_amount,
    pr.rank
FROM top_products tp
JOIN products p ON p.id = tp.product_id
JOIN monthly_sales ms ON ms.product_id = tp.product_id
JOIN product_ranks pr ON pr.product_id = tp.product_id AND pr.month = ms.month
ORDER BY ms.month, pr.rank`,
						},
						Upstreams: []pipeline.Upstream{{Value: "orders"}, {Value: "products"}},
					},
					{
						Name: "orders",
						Columns: []pipeline.Column{
							{Name: "order_date", Type: "timestamp"},
							{Name: "product_id", Type: "int64"},
							{Name: "quantity", Type: "int64"},
							{Name: "amount", Type: "float64"},
						},
					},
					{
						Name: "products",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "category", Type: "str"},
						},
					},
				},
			},
			after: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "sales_report",
						Columns: []pipeline.Column{
							{Name: "name", Type: "str"},
							{Name: "category", Type: "str"},
							{Name: "month", Type: "timestamp"},
							{Name: "total_quantity", Type: "int64"},
							{Name: "total_amount", Type: "float64"},
							{Name: "rank", Type: "int64"},
						},
						Upstreams: []pipeline.Upstream{{Value: "orders"}, {Value: "products"}},
					},
				},
			},
			want: nil,
		},
	}
	runLineageTests(t, tests)
}

func testAdvancedSQLFeatures(t *testing.T) {
	tests := []struct {
		name     string
		pipeline *pipeline.Pipeline
		after    *pipeline.Pipeline
		want     error
	}{
		{
			name: "lateral joins with array operations",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "user_preferences",
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
WITH user_tags AS (
    SELECT 
        user_id,
        ARRAY_AGG(DISTINCT tag) as tags
    FROM user_activity
    GROUP BY user_id
),
exploded_tags AS (
    SELECT 
        u.user_id,
        u.name,
        t.tag,
        p.preference_value
    FROM users u
    CROSS JOIN LATERAL UNNEST(
        (SELECT tags FROM user_tags WHERE user_id = u.id)
    ) as t(tag)
    LEFT JOIN preferences p ON p.user_id = u.id AND p.tag = t.tag
)
SELECT 
    user_id,
    name,
    ARRAY_AGG(STRUCT(tag, COALESCE(preference_value, 0.0) as score)) as preferences
FROM exploded_tags
GROUP BY user_id, name`,
						},
						Upstreams: []pipeline.Upstream{
							{Value: "users"},
							{Value: "user_activity"},
							{Value: "preferences"},
						},
					},
					{
						Name: "users",
						Columns: []pipeline.Column{
							{Name: "id", Type: "int64"},
							{Name: "name", Type: "str"},
						},
					},
					{
						Name: "user_activity",
						Columns: []pipeline.Column{
							{Name: "user_id", Type: "int64"},
							{Name: "tag", Type: "str"},
						},
					},
					{
						Name: "preferences",
						Columns: []pipeline.Column{
							{Name: "user_id", Type: "int64"},
							{Name: "tag", Type: "str"},
							{Name: "preference_value", Type: "float64"},
						},
					},
				},
			},
			after: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "user_preferences",
						Columns: []pipeline.Column{
							{Name: "user_id", Type: "int64"},
							{Name: "name", Type: "str"},
							{Name: "preferences", Type: "array"},
						},
						Upstreams: []pipeline.Upstream{
							{Value: "users"},
							{Value: "user_activity"},
							{Value: "preferences"},
						},
					},
				},
			},
			want: nil,
		},
		{
			name: "window functions with complex partitioning",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "customer_metrics",
						ExecutableFile: pipeline.ExecutableFile{
							Content: `
SELECT 
    customer_id,
    SUM(amount) OVER (
        PARTITION BY customer_id 
        ORDER BY transaction_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) as total_spent
FROM orders
GROUP BY customer_id`,
						},
						Upstreams: []pipeline.Upstream{{Value: "orders"}},
					},
					{
						Name: "orders",
						Columns: []pipeline.Column{
							{Name: "customer_id", Type: "int64"},
							{Name: "transaction_date", Type: "timestamp"},
							{Name: "amount", Type: "float64"},
						},
					},
				},
			},
			after: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "customer_metrics",
						Columns: []pipeline.Column{
							{Name: "customer_id", Type: "int64"},
							{Name: "total_spent", Type: "float64"},
						},
						Upstreams: []pipeline.Upstream{{Value: "orders"}},
					},
				},
			},
			want: nil,
		},
	}
	runLineageTests(t, tests)
}

func assertColumns(t *testing.T, got, want []pipeline.Column, wantCount int) {
	t.Helper()

	if len(got) != wantCount {
		t.Errorf("Column count mismatch: got %d, want %d", len(got), wantCount)
	}

	columnMap := make(map[string]pipeline.Column)
	for _, col := range want {
		columnMap[col.Name] = col
	}

	for _, col := range got {
		wantCol, exists := columnMap[col.Name]
		if !exists {
			t.Errorf("Unexpected column %s found", col.Name)
			continue
		}

		if col.Type != wantCol.Type {
			t.Errorf("Column %s type mismatch: got %s, want %s", col.Name, col.Type, wantCol.Type)
		}
		if col.PrimaryKey != wantCol.PrimaryKey {
			t.Errorf("Column %s primary key mismatch: got %v, want %v", col.Name, col.PrimaryKey, wantCol.PrimaryKey)
		}
	}
}
