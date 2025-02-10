package pipeline

import (
	"sync"
	"testing"

	"github.com/bruin-data/bruin/pkg/sqlparser"
)

var (
	SQLParser *sqlparser.SQLParser
	mu        sync.Mutex
)

func SetupSQLParser() error {
	mu.Lock()
	defer mu.Unlock()

	if SQLParser == nil {
		var err error
		sqlParser, err := sqlparser.NewSQLParser(true)
		if err != nil {
			return err
		}
		err = sqlParser.Start()
		if err != nil {
			return err
		}
		SQLParser = sqlParser
	}
	return nil
}

func TestParseLineageRecursively(t *testing.T) {
	t.Parallel()

	err := SetupSQLParser()
	if err != nil {
		t.Errorf("error initializing SQL parser: %v", err)
	}
	testCases := map[string]func(*testing.T){
		"basic recursive parsing":   testBasicRecursiveParsing,
		"joins and complex queries": testJoinsAndComplexQueries,
		"advanced SQL features":     testAdvancedSQLFeatures,
		"dialect specific features": testDialectSpecificFeatures,
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			tc(t)
		})
	}
}

func runLineageTests(t *testing.T, tests []TestCase) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			runSingleLineageTest(t, tt.pipeline, tt.after, tt.want)
		})
	}
}

func runSingleLineageTest(t *testing.T, p, after *Pipeline, want error) {
	t.Helper()

	extractor := NewLineageExtractor(SQLParser)
	for _, asset := range p.Assets {
		err := extractor.ColumnLineage(p, asset, make(map[string]bool))
		assertLineageError(t, err, want)

		assertColumns(t, asset.Columns, after.GetAssetByName(asset.Name).Columns, len(asset.Columns))
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

func assertAssetExists(t *testing.T, afterPipeline *Pipeline, asset *Asset) {
	t.Helper()

	assetFound := afterPipeline.GetAssetByName(asset.Name)
	if assetFound == nil {
		t.Errorf("Asset %s not found in pipeline", asset.Name)
		return
	}

	if len(asset.Upstreams) == len(assetFound.Upstreams) {
		foundUpstreams := make(map[string]bool)

		for _, upstreamFound := range assetFound.Upstreams {
			for _, upstreamFoundCol := range upstreamFound.Columns {
				foundUpstreams[upstreamFoundCol.Name] = true
			}
		}

		for _, upstream := range asset.Upstreams {
			for _, upstreamFound := range upstream.Columns {
				if !foundUpstreams[upstreamFound.Name] {
					t.Errorf("Upstream %s not found in asset %s and column %s", upstreamFound.Name, assetFound.Name, upstream.Value)
				}
			}
		}
	} else {
		t.Errorf("Upstream count mismatch for asset %s: got %d, want %d",
			asset.Name, len(asset.Upstreams), len(assetFound.Upstreams))
	}

	if len(asset.Columns) != len(assetFound.Columns) {
		t.Errorf("Column count mismatch for asset %s: got %d, want %d",
			asset.Name, len(asset.Columns), len(assetFound.Columns))
		return
	}

	columnMap := make(map[string]Column)
	for _, col := range asset.Columns {
		columnMap[col.Name] = col
	}

	for _, gotCol := range assetFound.Columns {
		wantCol, exists := columnMap[gotCol.Name]
		if !exists {
			t.Errorf("Unexpected column %s found in asset %s", gotCol.Name, asset.Name)
			continue
		}

		if gotCol.Type != wantCol.Type {
			t.Errorf("Column %s.%s type mismatch: got %s, want %s",
				asset.Name, gotCol.Name, gotCol.Type, wantCol.Type)
		}
		if gotCol.PrimaryKey != wantCol.PrimaryKey {
			t.Errorf("Column %s.%s primary key mismatch: got %v, want %v",
				asset.Name, gotCol.Name, gotCol.PrimaryKey, wantCol.PrimaryKey)
		}
	}
}

func assertColumns(t *testing.T, got, want []Column, wantCount int) {
	t.Helper()

	if len(got) != wantCount {
		t.Errorf("Column count mismatch: got %d, want %d", len(got), wantCount)
	}

	columnMap := make(map[string]Column)
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

type TestCase struct {
	name     string
	pipeline *Pipeline
	after    *Pipeline
	want     error
}

func testBasicRecursiveParsing(t *testing.T) {
	gen := func() []TestCase {
		return []TestCase{
			{
				name: "successful recursive lineage parsing",
				pipeline: &Pipeline{
					Assets: []*Asset{
						{
							Name: "table1",
							Type: "bq.sql",
							ExecutableFile: ExecutableFile{
								Content: "SELECT * FROM table2",
							},
							Upstreams: []Upstream{{Value: "table2"}},
						},
						{
							Name: "table2",
							Type: "bq.sql",
							ExecutableFile: ExecutableFile{
								Content: "SELECT * FROM table3",
							},
							Upstreams: []Upstream{{Value: "table3"}},
						},
						{
							Name: "table3",
							Columns: []Column{
								{Name: "id", Type: "int64", PrimaryKey: true, Description: "Just a number", UpdateOnMerge: false, Checks: []ColumnCheck{
									{Name: "not_null"},
								}},
								{Name: "name", Type: "str", Description: "Just a name", UpdateOnMerge: false, Checks: []ColumnCheck{
									{Name: "not_null"},
								}},
								{Name: "age", Type: "int64", Description: "Just an age", UpdateOnMerge: false, Checks: []ColumnCheck{
									{Name: "not_null"},
								}},
							},
							ExecutableFile: ExecutableFile{
								Content: "SELECT id,name,age FROM table4",
							},
						},
					},
				},
				after: &Pipeline{
					Assets: []*Asset{
						{
							Name: "table1",
							Type: "bq.sql",
							ExecutableFile: ExecutableFile{
								Content: "SELECT * FROM table2",
							},
							Columns: []Column{
								{Name: "id", Type: "int64", PrimaryKey: false, Upstreams: []*UpstreamColumn{{Column: "id", Table: "table2"}}, UpdateOnMerge: false, Description: "Just a number", Checks: []ColumnCheck{}},
								{Name: "name", Type: "str", Upstreams: []*UpstreamColumn{{Column: "name", Table: "table2"}}, UpdateOnMerge: false, Description: "Just a name", Checks: []ColumnCheck{}},
								{Name: "age", Type: "int64", Upstreams: []*UpstreamColumn{{Column: "age", Table: "table2"}}, UpdateOnMerge: false, Description: "Just an age", Checks: []ColumnCheck{}},
							},
							Upstreams: []Upstream{{Value: "table2", Columns: []DependsColumn{{Name: "id"}, {Name: "name"}, {Name: "age"}}}},
						},
						{
							Name: "table2",
							Type: "bq.sql",
							ExecutableFile: ExecutableFile{
								Content: "SELECT * FROM table3",
							},
							Columns: []Column{
								{Name: "id", Type: "int64", PrimaryKey: false, Upstreams: []*UpstreamColumn{{Column: "id", Table: "table3"}}, UpdateOnMerge: false, Description: "Just a number", Checks: []ColumnCheck{}},
								{Name: "name", Type: "str", Upstreams: []*UpstreamColumn{{Column: "name", Table: "table3"}}, UpdateOnMerge: false, Description: "Just a name", Checks: []ColumnCheck{}},
								{Name: "age", Type: "int64", Upstreams: []*UpstreamColumn{{Column: "age", Table: "table3"}}, UpdateOnMerge: false, Description: "Just an age", Checks: []ColumnCheck{}},
							},
							Upstreams: []Upstream{{Value: "table3", Columns: []DependsColumn{}}},
						},

						{
							Name: "table3",
							Columns: []Column{
								{Name: "id", Type: "int64", PrimaryKey: true, Description: "Just a number", UpdateOnMerge: false, Checks: []ColumnCheck{
									{Name: "not_null"},
								}},
								{Name: "name", Type: "str", Description: "Just a name", UpdateOnMerge: false, Checks: []ColumnCheck{
									{Name: "not_null"},
								}},
								{Name: "age", Type: "int64", Description: "Just an age", UpdateOnMerge: false, Checks: []ColumnCheck{
									{Name: "not_null"},
								}},
							},
							ExecutableFile: ExecutableFile{
								Content: "SELECT id,name,age FROM table4",
							},
						},
					},
				},
				want: nil,
			},
		}
	}

	for range 1000 {
		runLineageTests(t, gen())
	}
}

func testJoinsAndComplexQueries(t *testing.T) {
	tests := []TestCase{
		{
			name: "complex joins with multiple dependencies",
			pipeline: &Pipeline{
				Assets: []*Asset{
					{
						Name: "final_report",
						Type: "bq.sql",
						ExecutableFile: ExecutableFile{
							Content: `
								SELECT 
									o.order_id,
									c.customer_name,
									p.product_name,
									o.quantity * p.price as total_amount,
									s.status_description
								FROM orders o
								JOIN customers c ON o.customer_id = c.customer_id
								JOIN products p ON o.product_id = p.product_id
								LEFT JOIN order_status s ON o.status_id = s.status_id
							`,
						},
						Upstreams: []Upstream{
							{Value: "orders"},
							{Value: "customers"},
							{Value: "products"},
							{Value: "order_status"},
						},
					},
					{
						Name: "orders",
						Type: "bq.sql",
						ExecutableFile: ExecutableFile{
							Content: `
								SELECT 
									order_id,
									customer_id,
									product_id,
									quantity,
									status_id
								FROM raw_orders
								WHERE is_valid = true
							`,
						},
						Upstreams: []Upstream{{Value: "raw_orders"}},
					},
					{
						Name: "customers",
						Columns: []Column{
							{Name: "customer_id", Type: "int64", PrimaryKey: true, Description: "Customer ID"},
							{Name: "customer_name", Type: "str", Description: "Customer full name"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM customers",
						},
					},
					{
						Name: "products",
						Columns: []Column{
							{Name: "product_id", Type: "int64", PrimaryKey: true, Description: "Product ID"},
							{Name: "product_name", Type: "str", Description: "Product name"},
							{Name: "price", Type: "float64", Description: "Product price"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM products",
						},
					},
					{
						Name: "order_status",
						Columns: []Column{
							{Name: "status_id", Type: "int64", PrimaryKey: true, Description: "Status ID"},
							{Name: "status_description", Type: "str", Description: "Status description"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM order_status",
						},
					},
					{
						Name: "raw_orders",
						Columns: []Column{
							{Name: "order_id", Type: "int64", PrimaryKey: true, Description: "Order ID"},
							{Name: "customer_id", Type: "int64", Description: "Customer ID"},
							{Name: "product_id", Type: "int64", Description: "Product ID"},
							{Name: "quantity", Type: "int64", Description: "Order quantity"},
							{Name: "status_id", Type: "int64", Description: "Status ID"},
							{Name: "is_valid", Type: "bool", Description: "Order validity flag"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM raw_orders",
						},
					},
				},
			},
			after: &Pipeline{
				Assets: []*Asset{
					{
						Name: "final_report",
						Type: "bq.sql",
						Columns: []Column{
							{
								Name:        "order_id",
								Type:        "int64",
								Description: "Order ID",
								Upstreams:   []*UpstreamColumn{{Column: "order_id", Table: "orders"}},
							},
							{
								Name:        "customer_name",
								Type:        "str",
								Description: "Customer full name",
								Upstreams:   []*UpstreamColumn{{Column: "customer_name", Table: "customers"}},
							},
							{
								Name:        "product_name",
								Type:        "str",
								Description: "Product name",
								Upstreams:   []*UpstreamColumn{{Column: "product_name", Table: "products"}},
							},
							{
								Name:        "total_amount",
								Type:        "float64",
								Description: "Total order amount",
								Upstreams: []*UpstreamColumn{
									{Column: "quantity", Table: "orders"},
									{Column: "price", Table: "products"},
								},
							},
							{
								Name:        "status_description",
								Type:        "str",
								Description: "Status description",
								Upstreams:   []*UpstreamColumn{{Column: "status_description", Table: "order_status"}},
							},
						},
						Upstreams: []Upstream{
							{Value: "orders", Columns: []DependsColumn{{Name: "order_id"}, {Name: "customer_id"}, {Name: "product_id"}, {Name: "quantity"}, {Name: "status_id"}}},
							{Value: "customers", Columns: []DependsColumn{{Name: "customer_id"}, {Name: "customer_name"}}},
							{Value: "products", Columns: []DependsColumn{{Name: "product_id"}, {Name: "product_name"}, {Name: "price"}}},
							{Value: "order_status", Columns: []DependsColumn{{Name: "status_id"}, {Name: "status_description"}}},
						},
					},
					{
						Name: "orders",
						Type: "bq.sql",
						Columns: []Column{
							{
								Name:        "order_id",
								Type:        "int64",
								Description: "Order ID",
								Upstreams:   []*UpstreamColumn{{Column: "order_id", Table: "raw_orders"}},
							},
							{
								Name:        "customer_id",
								Type:        "int64",
								Description: "Customer ID",
								Upstreams:   []*UpstreamColumn{{Column: "customer_id", Table: "raw_orders"}},
							},
							{
								Name:        "product_id",
								Type:        "int64",
								Description: "Product ID",
								Upstreams:   []*UpstreamColumn{{Column: "product_id", Table: "raw_orders"}},
							},
							{
								Name:        "quantity",
								Type:        "int64",
								Description: "Order quantity",
								Upstreams:   []*UpstreamColumn{{Column: "quantity", Table: "raw_orders"}},
							},
							{
								Name:        "status_id",
								Type:        "int64",
								Description: "Status ID",
								Upstreams:   []*UpstreamColumn{{Column: "status_id", Table: "raw_orders"}},
							},
						},
						Upstreams: []Upstream{{Value: "raw_orders", Columns: []DependsColumn{{Name: "order_id"}, {Name: "customer_id"}, {Name: "product_id"}, {Name: "quantity"}, {Name: "status_id"}, {Name: "is_valid"}}}},
					},
					{
						Name: "customers",
						Columns: []Column{
							{Name: "customer_id", Type: "int64", PrimaryKey: true, Description: "Customer ID"},
							{Name: "customer_name", Type: "str", Description: "Customer full name"},
						},
					},
					{
						Name: "products",
						Columns: []Column{
							{Name: "product_id", Type: "int64", PrimaryKey: true, Description: "Product ID"},
							{Name: "product_name", Type: "str", Description: "Product name"},
							{Name: "price", Type: "float64", Description: "Product price"},
						},
					},
					{
						Name: "order_status",
						Columns: []Column{
							{Name: "status_id", Type: "int64", PrimaryKey: true, Description: "Status ID"},
							{Name: "status_description", Type: "str", Description: "Status description"},
						},
					},
					{
						Name: "raw_orders",
						Columns: []Column{
							{Name: "order_id", Type: "int64", PrimaryKey: true, Description: "Order ID"},
							{Name: "customer_id", Type: "int64", Description: "Customer ID"},
							{Name: "product_id", Type: "int64", Description: "Product ID"},
							{Name: "quantity", Type: "int64", Description: "Order quantity"},
							{Name: "status_id", Type: "int64", Description: "Status ID"},
							{Name: "is_valid", Type: "bool", Description: "Order validity flag"},
						},
					},
				},
			},
			want: nil,
		},
	}
	runLineageTests(t, tests)
}

func testAdvancedSQLFeatures(t *testing.T) {
	tests := []TestCase{
		{
			name: "snowflake complex condition",
			pipeline: &Pipeline{
				Assets: []*Asset{
					{
						Name: "sales_summary",
						Type: "bq.sql",
						ExecutableFile: ExecutableFile{
							Content: `
		        SELECT
		            case
		                when raw_sales.CancelledAt is not null
		                then coalesce(raw_sales.CancellationReason, 'Empty Reason')
		            end as CancellationReason,
		            case
		                when
		                    raw_sales.Id is not null and
		                    bookingCreditRefundedAt is null and
		                    raw_sales.Accepted
		                then 1
		                else 0
		            end as credits_spent
		        FROM raw_sales
									`,
						},
						Upstreams: []Upstream{{Value: "raw_sales"}},
					},
					{
						Name: "raw_sales",
						Type: "bq.sql",
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM data_sales",
						},
						Columns: []Column{
							{Name: "Id", Type: "STRING", Description: "Unique identifier"},
							{Name: "CancelledAt", Type: "TIMESTAMP", Description: "Cancellation timestamp"},
							{Name: "CancellationReason", Type: "STRING", Description: "Reason for cancellation"},
							{Name: "bookingCreditRefundedAt", Type: "TIMESTAMP", Description: "Timestamp when booking credit was refunded"},
							{Name: "Accepted", Type: "BOOLEAN", Description: "Whether the booking was accepted"},
						},
					},
				},
			},
			after: &Pipeline{
				Assets: []*Asset{
					{
						Name: "sales_summary",
						ExecutableFile: ExecutableFile{
							Content: `
		        SELECT
		            case
		                when raw_sales.CancelledAt is not null
		                then coalesce(raw_sales.CancellationReason, 'Empty Reason')
		            end as CancellationReason,
		            case
		                when
		                    raw_sales.Id is not null and
		                    bookingCreditRefundedAt is null and
		                    raw_sales.Accepted
		                then 1
		                else 0
		            end as credits_spent
		        FROM raw_sales
									`,
						},
						Columns: []Column{
							{Name: "cancellationreason", Type: "STRING", Description: "Reason for cancellation", Upstreams: []*UpstreamColumn{{Column: "CancellationReason", Table: "raw_sales"}, {Column: "CancelledAt", Table: "raw_sales"}}},
							{Name: "credits_spent", Type: "BOOLEAN", Description: "Whether the booking was accepted", Upstreams: []*UpstreamColumn{{Column: "Accepted", Table: "raw_sales"}, {Column: "bookingCreditRefundedAt", Table: "raw_sales"}, {Column: "Id", Table: "raw_sales"}}},
						},
						Upstreams: []Upstream{{Value: "raw_sales", Columns: []DependsColumn{{Name: "accepted"}, {Name: "bookingcreditrefundedat"}, {Name: "cancellationreason"}, {Name: "cancelledat"}, {Name: "id"}}}},
					},
					{
						Name: "raw_sales",
						Columns: []Column{
							{Name: "Id", Type: "STRING", Description: "Unique identifier"},
							{Name: "CancelledAt", Type: "TIMESTAMP", Description: "Cancellation timestamp"},
							{Name: "CancellationReason", Type: "STRING", Description: "Reason for cancellation"},
							{Name: "bookingCreditRefundedAt", Type: "TIMESTAMP", Description: "Timestamp when booking credit was refunded"},
							{Name: "Accepted", Type: "BOOLEAN", Description: "Whether the booking was accepted"},
						},
					},
				},
			},
			want: nil,
		},
		{
			name: "snowflake column name with as",
			pipeline: &Pipeline{
				Assets: []*Asset{
					{
						Name: "sales_summary",
						Type: "bq.sql",
						ExecutableFile: ExecutableFile{
							Content: `
		       SELECT
		    t.event_date,
		    t.location_code as location,
		    t.session_id as session,
		    COUNT(DISTINCT t.customer_id) as visitor_count,
		    SUM(t.activity_count) as total_activities,
		    SUM(t.interaction_count) as total_interactions,
		    CURRENT_TIMESTAMP() as created_at
		FROM raw_sales t
		GROUP BY 1, 2, 3
		ORDER BY 1, 2, 3
									`,
						},
						Upstreams: []Upstream{{Value: "raw_sales"}},
					},
					{
						Name: "raw_sales",
						Type: "bq.sql",
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM data_sales",
						},
						Columns: []Column{
							{Name: "event_date", Type: "date", Description: "Event date"},
							{Name: "location_code", Type: "string", Description: "Location code"},
							{Name: "session_id", Type: "integer", Description: "Session identifier"},
							{Name: "customer_id", Type: "integer", Description: "Count of unique visitors"},
							{Name: "activity_count", Type: "integer", Description: "Sum of activity counts"},
							{Name: "interaction_count", Type: "integer", Description: "Sum of activity counts"},
							{Name: "created_at", Type: "timestamp", Description: "Record creation timestamp"},
						},
					},
				},
			},
			after: &Pipeline{
				Assets: []*Asset{
					{
						Name: "sales_summary",
						ExecutableFile: ExecutableFile{
							Content: `
		       SELECT
		    t.event_date,
		    t.location_code as location,
		    t.session_id as session,
		    COUNT(DISTINCT t.customer_id) as visitor_count,
		    SUM(t.activity_count) as total_activities,
		    SUM(t.interaction_count) as total_interactions,
		    CURRENT_TIMESTAMP() as created_at
		FROM raw_sales t
		GROUP BY 1, 2, 3
		ORDER BY 1, 2, 3
									`,
						},
						Columns: []Column{
							{Name: "event_date", Type: "date", Description: "Event date", Upstreams: []*UpstreamColumn{{Column: "event_date", Table: "raw_sales"}}},
							{Name: "location", Type: "string", Description: "Location code", Upstreams: []*UpstreamColumn{{Column: "location_code", Table: "raw_sales"}}},
							{Name: "session", Type: "integer", Description: "Session identifier", Upstreams: []*UpstreamColumn{{Column: "session_id", Table: "raw_sales"}}},
							{Name: "visitor_count", Type: "integer", Description: "Count of unique visitors", Upstreams: []*UpstreamColumn{{Column: "customer_id", Table: "raw_sales"}}},
							{Name: "total_activities", Type: "integer", Description: "Sum of activity counts", Upstreams: []*UpstreamColumn{{Column: "activity_count", Table: "raw_sales"}}},
							{Name: "total_interactions", Type: "integer", Description: "Sum of interaction counts", Upstreams: []*UpstreamColumn{{Column: "interaction_count", Table: "raw_sales"}}},
							{Name: "created_at", Type: "TIMESTAMP", Description: "Record creation timestamp", Upstreams: []*UpstreamColumn{{}}},
						},
						Upstreams: []Upstream{{Value: "raw_sales", Columns: []DependsColumn{{Name: "event_date", Usage: "raw_sales"}, {Name: "location_code", Usage: "raw_sales"}, {Name: "session_id", Usage: "raw_sales"}, {Name: "customer_id", Usage: "raw_sales"}, {Name: "activity_count", Usage: "raw_sales"}, {Name: "interaction_count", Usage: "raw_sales"}}}},
					},
					{
						Name: "raw_sales",
						Columns: []Column{
							{Name: "event_date", Type: "date", Description: "Event date"},
							{Name: "location_code", Type: "string", Description: "Location code"},
							{Name: "session_id", Type: "integer", Description: "Session identifier"},
							{Name: "customer_id", Type: "integer", Description: "Customer identifier"},
							{Name: "activity_count", Type: "integer", Description: "Number of activities"},
							{Name: "interaction_count", Type: "integer", Description: "Number of interactions"},
							{Name: "created_at", Type: "timestamp", Description: "Record creation timestamp"},
						},
					},
				},
			},
			want: nil,
		},
		{
			name: "advanced SQL functions and aggregations",
			pipeline: &Pipeline{
				Assets: []*Asset{
					{
						Name: "sales_summary",
						Type: "bq.sql",
						ExecutableFile: ExecutableFile{
							Content: `
								SELECT
									DATE_TRUNC(order_date, MONTH) as month,
									COUNT(DISTINCT customer_id) as unique_customers,
									SUM(amount) as total_sales,
									AVG(amount) as avg_sale,
									CONCAT(
										CAST(COUNT(*) as STRING),
										' orders worth $',
										CAST(SUM(amount) as STRING)
									) as summary,
									NOW() as report_generated_at
								FROM raw_sales
								GROUP BY DATE_TRUNC(order_date, MONTH)
							`,
						},

						Upstreams: []Upstream{{Value: "raw_sales"}},
					},
					{
						Name: "raw_sales",
						Columns: []Column{
							{Name: "order_date", Type: "timestamp", Description: "Order timestamp"},
							{Name: "customer_id", Type: "int64", Description: "Customer identifier"},
							{Name: "amount", Type: "float64", Description: "Sale amount"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM data_sales",
						},
					},
				},
			},
			after: &Pipeline{
				Assets: []*Asset{
					{
						Name: "sales_summary",
						Type: "bq.sql",
						Columns: []Column{
							{
								Name:        "month",
								Type:        "timestamp",
								Description: "Order timestamp",
								Upstreams:   []*UpstreamColumn{{Column: "order_date", Table: "raw_sales"}},
							},
							{
								Name:        "unique_customers",
								Type:        "int64",
								Description: "Customer identifier",
								Upstreams:   []*UpstreamColumn{{Column: "customer_id", Table: "raw_sales"}},
							},
							{
								Name:        "total_sales",
								Type:        "float64",
								Description: "Sale amount",
								Upstreams:   []*UpstreamColumn{{Column: "amount", Table: "raw_sales"}},
							},
							{
								Name:        "avg_sale",
								Type:        "float64",
								Description: "Sale amount",
								Upstreams:   []*UpstreamColumn{{Column: "amount", Table: "raw_sales"}},
							},
							{
								Name:        "summary",
								Type:        "float64",
								Description: "Sale amount",
								Upstreams:   []*UpstreamColumn{{Column: "amount", Table: "raw_sales"}},
							},
							{
								Name:      "report_generated_at",
								Type:      "UNKNOWN",
								Upstreams: []*UpstreamColumn{{Column: "", Table: ""}},
							},
						},
						Upstreams: []Upstream{{Value: "raw_sales", Columns: []DependsColumn{{Name: "order_date"}, {Name: "customer_id"}, {Name: "amount"}}}},
					},
					{
						Name: "raw_sales",
						Columns: []Column{
							{Name: "order_date", Type: "timestamp", Description: "Order timestamp"},
							{Name: "customer_id", Type: "int64", Description: "Customer identifier"},
							{Name: "amount", Type: "float64", Description: "Sale amount"},
						},
					},
				},
			},
			want: nil,
		},
	}
	runLineageTests(t, tests)
}

func testDialectSpecificFeatures(t *testing.T) {
	tests := []TestCase{
		{
			name: "redshift specific syntax",
			pipeline: &Pipeline{
				Assets: []*Asset{
					{
						Name: "sales_report",
						Type: "rs.sql",
						ExecutableFile: ExecutableFile{
							Content: `
                        SELECT 
                            DATE_TRUNC('month', sale_date) as sale_month,
                            LISTAGG(DISTINCT category, ', ') WITHIN GROUP (ORDER BY category) as categories,
                            SUM(amount) as total_sales,
                            AVG(amount) as avg_sale,
                            COUNT(DISTINCT customer_id) as unique_customers
                        FROM raw_sales
                        WHERE sale_date BETWEEN GETDATE() - INTERVAL '1 year' AND GETDATE()
                        GROUP BY 1
                    `,
						},
						Upstreams: []Upstream{{Value: "raw_sales"}},
					},
					{
						Name: "raw_sales",
						Type: "rs.sql",
						Columns: []Column{
							{Name: "sale_date", Type: "timestamp", PrimaryKey: true, Description: "Sale timestamp"},
							{Name: "category", Type: "varchar(max)", Description: "Product category"},
							{Name: "amount", Type: "decimal(18,2)", Description: "Sale amount"},
							{Name: "customer_id", Type: "bigint", Description: "Customer identifier"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM data_sales",
						},
					},
				},
			},
			after: &Pipeline{
				Assets: []*Asset{
					{
						Name: "sales_report",
						Type: "rs.sql",
						Columns: []Column{
							{
								Name:        "sale_month",
								Type:        "timestamp",
								Description: "Truncated sale month",
								Upstreams: []*UpstreamColumn{
									{Column: "sale_date", Table: "raw_sales"},
								},
								UpdateOnMerge: false,
							},
							{
								Name:        "categories",
								Type:        "varchar(max)",
								Description: "Aggregated categories",
								Upstreams: []*UpstreamColumn{
									{Column: "category", Table: "raw_sales"},
								},
								UpdateOnMerge: false,
							},
							{
								Name:        "total_sales",
								Type:        "decimal(18,2)",
								Description: "Sum of sales",
								Upstreams: []*UpstreamColumn{
									{Column: "amount", Table: "raw_sales"},
								},
								UpdateOnMerge: false,
								Checks:        []ColumnCheck{{Name: "positive"}},
							},
							{
								Name:        "avg_sale",
								Type:        "decimal(18,2)",
								Description: "Average sale amount",
								Upstreams: []*UpstreamColumn{
									{Column: "amount", Table: "raw_sales"},
								},
								UpdateOnMerge: false,
							},
							{
								Name:        "unique_customers",
								Type:        "bigint",
								Description: "Count of unique customers",
								Upstreams: []*UpstreamColumn{
									{Column: "customer_id", Table: "raw_sales"},
								},
								UpdateOnMerge: false,
								Checks:        []ColumnCheck{{Name: "positive"}},
							},
						},
						Upstreams: []Upstream{{Value: "raw_sales", Columns: []DependsColumn{{Name: "sale_date"}, {Name: "category"}, {Name: "amount"}, {Name: "customer_id"}}}},
					},
					{
						Name: "raw_sales",
						Type: "rs.sql",
						Columns: []Column{
							{Name: "sale_date", Type: "timestamp", PrimaryKey: true, Description: "Sale timestamp"},
							{Name: "category", Type: "varchar(max)", Description: "Product category"},
							{Name: "amount", Type: "decimal(18,2)", Description: "Sale amount"},
							{Name: "customer_id", Type: "bigint", Description: "Customer identifier"},
						},
					},
				},
			},
			want: nil,
		},

		{
			name: "postgres specific syntax",
			pipeline: &Pipeline{
				Assets: []*Asset{
					{
						Name: "user_stats",
						Type: "pg.sql",
						ExecutableFile: ExecutableFile{
							Content: `
								 WITH RECURSIVE user_hierarchy AS (
        SELECT id, manager_id, name, hire_date, 1 as level
        FROM users
        WHERE manager_id IS NULL
        UNION ALL
        SELECT u.id, u.manager_id, u.name, u.hire_date, uh.level + 1
        FROM users u
        JOIN user_hierarchy uh ON u.manager_id = uh.id
    )
    SELECT 
        name,
        level,
        array_agg(DISTINCT department) as departments,
        jsonb_object_agg(
            department,
            jsonb_build_object(
                'count', COUNT(*),
                'avg_tenure', AVG(EXTRACT(YEAR FROM age(current_date, user_hierarchy.hire_date)))
            )
        ) as dept_stats
    FROM user_hierarchy
    LEFT JOIN user_departments ud ON ud.user_id = user_hierarchy.id
    GROUP BY name, level;
							`,
						},
						Upstreams: []Upstream{
							{Value: "users"},
							{Value: "user_departments"},
						},
					},
					{
						Name: "users",
						Type: "pg.sql",
						Columns: []Column{
							{Name: "id", Type: "integer", PrimaryKey: true, Description: "User ID"},
							{Name: "manager_id", Type: "integer", Description: "Manager's user ID"},
							{Name: "name", Type: "text", Description: "User's name"},
							{Name: "hire_date", Type: "date", Description: "Hire date"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM data_users",
						},
					},
					{
						Name: "user_departments",
						Type: "pg.sql",
						Columns: []Column{
							{Name: "user_id", Type: "integer", Description: "User ID"},
							{Name: "department", Type: "text", Description: "Department name"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM data_user_departments",
						},
					},
				},
			},
			after: &Pipeline{
				Assets: []*Asset{
					{
						Name: "user_stats",
						Type: "pg.sql",
						Columns: []Column{
							{
								Name:        "name",
								Type:        "text",
								Description: "User's name",
								Upstreams: []*UpstreamColumn{
									{Column: "name", Table: "users"},
								},
								UpdateOnMerge: false,
							},
							{
								Name:        "departments",
								Type:        "text",
								Description: "Array of departments",
								Upstreams: []*UpstreamColumn{
									{Column: "department", Table: "user_departments"},
								},
								UpdateOnMerge: false,
							},
							{
								Name:          "level",
								Upstreams:     []*UpstreamColumn{},
								UpdateOnMerge: false,
								Type:          "INT",
							},
							{
								Name:        "dept_stats",
								Type:        "text",
								Description: "Department statistics",
								Upstreams: []*UpstreamColumn{
									{Column: "department", Table: "user_departments"},
									{Column: "hire_date", Table: "users"},
								},
								UpdateOnMerge: false,
							},
						},
						Upstreams: []Upstream{
							{Value: "users", Columns: []DependsColumn{{Name: "manager_id"}, {Name: "name"}, {Name: "hire_date"}}},
							{Value: "user_departments", Columns: []DependsColumn{{Name: "user_id"}, {Name: "department"}}},
						},
					},
					{
						Name: "users",
						Type: "pg.sql",
						Columns: []Column{
							{Name: "id", Type: "integer", PrimaryKey: true, Description: "User ID"},
							{Name: "manager_id", Type: "integer", Description: "Manager's user ID"},
							{Name: "name", Type: "text", Description: "User's name"},
							{Name: "hire_date", Type: "date", Description: "Hire date"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM data_users",
						},
					},
					{
						Name: "user_departments",
						Type: "pg.sql",
						Columns: []Column{
							{Name: "user_id", Type: "integer", Description: "User ID"},
							{Name: "department", Type: "text", Description: "Department name"},
						},
						ExecutableFile: ExecutableFile{
							Content: "SELECT * FROM data_user_departments",
						},
					},
				},
			},
			want: nil,
		},
	}
	runLineageTests(t, tests)
}

func TestAddColumnToAsset(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		asset         *Asset
		colName       string
		upstreamAsset *Asset
		upstreamCol   *Column
		after         *Asset
		want          error
	}{
		{
			name: "the existing values should not be overridden",
			asset: &Asset{
				Name: "test",
				ID:   "test",
				Upstreams: []Upstream{
					{Value: "test2"},
				},
				Columns: []Column{
					{Name: "id", Type: "integer", Description: "Just a number"},
				},
			},
			colName: "id",
			upstreamCol: &Column{
				Name:        "id",
				Type:        "integer",
				Description: "Just a test",
				Upstreams: []*UpstreamColumn{
					{Column: "id", Table: "test2"},
				},
			},
			upstreamAsset: &Asset{
				Name: "test2",
				Columns: []Column{
					{Name: "id", Type: "integer", Description: "Just a test"},
				},
			},
			after: &Asset{
				Name: "test",
				ID:   "test",
				Type: "duckdb.sql",
				Upstreams: []Upstream{
					{
						Value: "test2",
						Columns: []DependsColumn{
							{Name: "id"},
						},
					},
				},
				Columns: []Column{
					{
						Name:        "id",
						Type:        "integer",
						Description: "Just a number",
						Upstreams: []*UpstreamColumn{
							{Column: "id", Table: "test2"},
						},
					},
				},
			},
		},
		{
			name: "the existing values should not be overridden but the new column should be added",
			asset: &Asset{
				Name: "test",
				ID:   "test",
				Upstreams: []Upstream{
					{Value: "test2"},
				},
				Type: "duckdb.sql",
				Columns: []Column{
					{Name: "id", Type: "integer"},
				},
			},
			colName: "id",
			upstreamCol: &Column{
				Name:        "id",
				Type:        "integer",
				Description: "Just a test",
				Upstreams: []*UpstreamColumn{
					{Column: "id", Table: "test2"},
				},
			},
			upstreamAsset: &Asset{
				Name: "test2",
				Type: "duckdb.sql",
				Columns: []Column{
					{Name: "id", Type: "integer", Description: "Just a test"},
				},
			},
			after: &Asset{
				Name:      "test",
				ID:        "test",
				Upstreams: []Upstream{{Value: "test2"}},
				Type:      "duckdb.sql",
				Columns: []Column{
					{
						Name: "id", Type: "integer", Description: "Just a test", EntityAttribute: nil,
						Upstreams: []*UpstreamColumn{
							{Column: "id", Table: "test2"},
						},
					},
				},
			},
		},
		{
			name: "the upstream column type should be changed",
			asset: &Asset{
				Name: "test",
				Upstreams: []Upstream{
					{Value: "test2"},
				},
				Columns: []Column{
					{Name: "id", Type: "integer", Description: "Just a number"},
				},
			},
			colName: "id",
			upstreamCol: &Column{
				Name:        "id",
				Type:        "bigint",
				Description: "Just a test",
				Upstreams: []*UpstreamColumn{
					{Column: "id", Table: "test2"},
				},
			},
			upstreamAsset: &Asset{
				Name: "test2",
				Columns: []Column{
					{Name: "id", Type: "bigint", Description: "Just a test"},
				},
			},
			after: &Asset{Name: "test", Upstreams: []Upstream{{Value: "test2"}}, Type: "duckdb.sql", Columns: []Column{{Name: "id", Type: "integer", Description: "Just a number", Upstreams: []*UpstreamColumn{
				{Column: "id", Table: "test2"},
			}}}},
		},
		{
			name: "the new column should be added",
			asset: &Asset{
				Name: "test",
				ID:   "test",
				Upstreams: []Upstream{
					{Value: "test2"},
				},
				Columns: []Column{},
			},
			colName: "new_col",
			upstreamCol: &Column{
				Name:        "new_col",
				Type:        "string",
				Description: "New column",
				Upstreams: []*UpstreamColumn{
					{Column: "new_col", Table: "test2"},
				},
			},
			upstreamAsset: &Asset{
				Name: "test2",
				Columns: []Column{
					{Name: "id", Type: "integer", Description: "Just a test"},
					{Name: "new_col", Type: "string", Description: "New column"},
				},
			},
			after: &Asset{
				Name:      "test",
				ID:        "test",
				Upstreams: []Upstream{{Value: "test2"}},
				Type:      "duckdb.sql",
				Columns: []Column{{Name: "new_col", Type: "string", Description: "New column", Upstreams: []*UpstreamColumn{
					{Column: "new_col", Table: "test2"},
				}}},
			},
		},
	}
	err := SetupSQLParser()
	if err != nil {
		t.Errorf("error initializing SQL parser: %v", err)
	}
	lineage := NewLineageExtractor(SQLParser)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := lineage.addColumnToAsset(test.asset, test.colName, test.upstreamCol)
			if err != nil {
				t.Errorf("error adding column to asset: %v", err)
			}
			for _, col := range test.asset.Columns {
				upstreamCol := test.after.GetColumnWithName(col.Name)
				if upstreamCol == nil {
					t.Errorf("upstream column not found: %v", col.Name)
					continue
				}
				if col.Name != upstreamCol.Name {
					t.Errorf("upstream column mismatch name: %v %v", col.Name, upstreamCol.Name)
				}
				if col.Description != upstreamCol.Description {
					t.Errorf("upstream column mismatch description: %v %v", col.Description, upstreamCol.Description)
				}

				if col.Type != upstreamCol.Type {
					t.Errorf("upstream column mismatch type: %v %v", col.Type, upstreamCol.Type)
				}

				if len(upstreamCol.Upstreams) != len(col.Upstreams) {
					t.Errorf("upstream column mismatch upstreams: %v %v", col.Upstreams, upstreamCol.Upstreams)
				}
			}
		})
	}
}

func TestHandleExistingOrNewColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		asset       *Asset
		upstreamCol *Column
		existingCol *Column
		want        *Column
		wantErr     error
	}{
		{
			name: "update existing column with new upstream",
			asset: &Asset{
				Name: "test_table",
				Columns: []Column{
					{
						Name:        "id",
						Type:        "integer",
						Description: "Existing description",
						Upstreams: []*UpstreamColumn{
							{Column: "old_id", Table: "old_table"},
						},
					},
				},
			},
			upstreamCol: &Column{
				Name:        "id",
				Type:        "bigint",
				Description: "New description",
				Upstreams: []*UpstreamColumn{
					{Column: "new_id", Table: "new_table"},
				},
			},
			existingCol: &Column{
				Name:        "id",
				Type:        "integer",
				Description: "Existing description",
				Upstreams: []*UpstreamColumn{
					{Column: "old_id", Table: "old_table"},
				},
			},
			want: &Column{
				Name:        "id",
				Type:        "integer",
				Description: "Existing description",
				Upstreams: []*UpstreamColumn{
					{Column: "old_id", Table: "old_table"},
					{Column: "new_id", Table: "new_table"},
				},
			},
			wantErr: nil,
		},
		{
			name: "update existing column with duplicate upstream",
			asset: &Asset{
				Name: "test_table",
				Columns: []Column{
					{
						Name:        "id",
						Type:        "integer",
						Description: "Existing description",
						Upstreams: []*UpstreamColumn{
							{Column: "old_id", Table: "old_table"},
						},
					},
				},
			},
			upstreamCol: &Column{
				Name:        "id",
				Type:        "bigint",
				Description: "New description",
				Upstreams: []*UpstreamColumn{
					{Column: "old_id", Table: "old_table"},
				},
			},
			existingCol: &Column{
				Name:        "id",
				Type:        "integer",
				Description: "Existing description",
				Upstreams: []*UpstreamColumn{
					{Column: "old_id", Table: "old_table"},
				},
			},
			want: &Column{
				Name:        "id",
				Type:        "integer",
				Description: "Existing description",
				Upstreams: []*UpstreamColumn{
					{Column: "old_id", Table: "old_table"},
				},
			},
			wantErr: nil,
		},
		{
			name: "update existing column with multiple new upstreams",
			asset: &Asset{
				Name: "test_table",
				Columns: []Column{
					{
						Name:        "id",
						Type:        "integer",
						Description: "Existing description",
						Upstreams: []*UpstreamColumn{
							{Column: "old_id", Table: "old_table"},
						},
					},
				},
			},
			upstreamCol: &Column{
				Name:        "id",
				Type:        "bigint",
				Description: "New description",
				Upstreams: []*UpstreamColumn{
					{Column: "new_id1", Table: "new_table1"},
					{Column: "new_id2", Table: "new_table2"},
				},
			},
			existingCol: &Column{
				Name:        "id",
				Type:        "integer",
				Description: "Existing description",
				Upstreams: []*UpstreamColumn{
					{Column: "old_id", Table: "old_table"},
				},
			},
			want: &Column{
				Name:        "id",
				Type:        "integer",
				Description: "Existing description",
				Upstreams: []*UpstreamColumn{
					{Column: "old_id", Table: "old_table"},
					{Column: "new_id1", Table: "new_table1"},
					{Column: "new_id2", Table: "new_table2"},
				},
			},
			wantErr: nil,
		},
	}

	err := SetupSQLParser()
	if err != nil {
		t.Errorf("error initializing SQL parser: %v", err)
	}
	lineage := NewLineageExtractor(SQLParser)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := lineage.handleExistingOrNewColumn(tt.asset, tt.upstreamCol, tt.existingCol)
			if (err != nil) != (tt.wantErr != nil) {
				t.Errorf("handleExistingOrNewColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.existingCol.Name != tt.want.Name {
				t.Errorf("Column name = %v, want %v", tt.existingCol.Name, tt.want.Name)
			}
			if tt.existingCol.Type != tt.want.Type {
				t.Errorf("Column type = %v, want %v", tt.existingCol.Type, tt.want.Type)
			}
			if tt.existingCol.Description != tt.want.Description {
				t.Errorf("Column description = %v, want %v", tt.existingCol.Description, tt.want.Description)
			}
			if len(tt.existingCol.Upstreams) != len(tt.want.Upstreams) {
				t.Errorf("Column upstreams length = %v, want %v", len(tt.existingCol.Upstreams), len(tt.want.Upstreams))
			}

			for _, wantUpstream := range tt.want.Upstreams {
				found := false
				for _, gotUpstream := range tt.existingCol.Upstreams {
					if gotUpstream.Column == wantUpstream.Column && gotUpstream.Table == wantUpstream.Table {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected upstream {Column: %v, Table: %v} not found", wantUpstream.Column, wantUpstream.Table)
				}
			}
		})
	}
}

func TestUpdateExistingColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		existingCol *Column
		upstreamCol *Column
		want        *Column
	}{
		{
			name: "empty existing column should be updated with upstream values",
			existingCol: &Column{
				Name: "test_col",
			},
			upstreamCol: &Column{
				Name:            "test_col",
				Description:     "Test description",
				Type:            "integer",
				EntityAttribute: &EntityAttribute{Entity: "test_entity"},
				UpdateOnMerge:   true,
			},
			want: &Column{
				Name:            "test_col",
				Description:     "Test description",
				Type:            "integer",
				EntityAttribute: &EntityAttribute{Entity: "test_entity"},
				UpdateOnMerge:   true,
			},
		},
		{
			name: "existing values should not be overwritten",
			existingCol: &Column{
				Name:            "test_col",
				Description:     "Existing description",
				Type:            "string",
				EntityAttribute: &EntityAttribute{Entity: "existing_entity"},
				UpdateOnMerge:   false,
			},
			upstreamCol: &Column{
				Name:            "test_col",
				Description:     "New description",
				Type:            "integer",
				EntityAttribute: &EntityAttribute{Entity: "new_entity"},
				UpdateOnMerge:   true,
			},
			want: &Column{
				Name:            "test_col",
				Description:     "Existing description",
				Type:            "string",
				EntityAttribute: &EntityAttribute{Entity: "existing_entity"},
				UpdateOnMerge:   true,
			},
		},
		{
			name: "partial existing values should be updated",
			existingCol: &Column{
				Name:        "test_col",
				Description: "Existing description",
			},
			upstreamCol: &Column{
				Name:            "test_col",
				Description:     "New description",
				Type:            "integer",
				EntityAttribute: &EntityAttribute{Entity: "new_entity"},
				UpdateOnMerge:   true,
			},
			want: &Column{
				Name:            "test_col",
				Description:     "Existing description",
				Type:            "integer",
				EntityAttribute: &EntityAttribute{Entity: "new_entity"},
				UpdateOnMerge:   true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			updateExistingColumn(tt.existingCol, tt.upstreamCol)

			if tt.existingCol.Description != tt.want.Description {
				t.Errorf("Description = %v, want %v", tt.existingCol.Description, tt.want.Description)
			}
			if tt.existingCol.Type != tt.want.Type {
				t.Errorf("Type = %v, want %v", tt.existingCol.Type, tt.want.Type)
			}
			if tt.existingCol.UpdateOnMerge != tt.want.UpdateOnMerge {
				t.Errorf("UpdateOnMerge = %v, want %v", tt.existingCol.UpdateOnMerge, tt.want.UpdateOnMerge)
			}

			if tt.want.EntityAttribute == nil {
				if tt.existingCol.EntityAttribute != nil {
					t.Errorf("EntityAttribute = %v, want nil", tt.existingCol.EntityAttribute)
				}
			} else {
				if tt.existingCol.EntityAttribute == nil {
					t.Errorf("EntityAttribute is nil, want %v", tt.want.EntityAttribute)
				} else if tt.existingCol.EntityAttribute.Entity != tt.want.EntityAttribute.Entity {
					t.Errorf("EntityAttribute.Name = %v, want %v", tt.existingCol.EntityAttribute.Entity, tt.want.EntityAttribute.Entity)
				}
			}
		})
	}
}

func TestUpdateAssetColumn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		asset    *Asset
		newCol   *Column
		expected []Column
	}{
		{
			name: "update existing column",
			asset: &Asset{
				Columns: []Column{
					{Name: "id", Type: "integer", Description: "old description"},
					{Name: "name", Type: "string"},
				},
			},
			newCol: &Column{
				Name:        "id",
				Type:        "bigint",
				Description: "new description",
			},
			expected: []Column{
				{Name: "id", Type: "bigint", Description: "new description"},
				{Name: "name", Type: "string"},
			},
		},
		{
			name: "case insensitive column match",
			asset: &Asset{
				Columns: []Column{
					{Name: "ID", Type: "integer"},
					{Name: "name", Type: "string"},
				},
			},
			newCol: &Column{
				Name: "id",
				Type: "bigint",
			},
			expected: []Column{
				{Name: "id", Type: "bigint"},
				{Name: "name", Type: "string"},
			},
		},
		{
			name: "no matching column",
			asset: &Asset{
				Columns: []Column{
					{Name: "id", Type: "integer"},
					{Name: "name", Type: "string"},
				},
			},
			newCol: &Column{
				Name: "age",
				Type: "integer",
			},
			expected: []Column{
				{Name: "id", Type: "integer"},
				{Name: "name", Type: "string"},
			},
		},
	}

	lineage := NewLineageExtractor(SQLParser)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			lineage.updateAssetColumn(tt.asset, tt.newCol)

			if len(tt.asset.Columns) != len(tt.expected) {
				t.Errorf("got %d columns, want %d", len(tt.asset.Columns), len(tt.expected))
				return
			}

			for i, got := range tt.asset.Columns {
				want := tt.expected[i]
				if got.Name != want.Name {
					t.Errorf("column[%d].Name = %v, want %v", i, got.Name, want.Name)
				}
				if got.Type != want.Type {
					t.Errorf("column[%d].Type = %v, want %v", i, got.Type, want.Type)
				}
				if got.Description != want.Description {
					t.Errorf("column[%d].Description = %v, want %v", i, got.Description, want.Description)
				}
			}
		})
	}
}

func TestUpstreamExists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		upstreams   []*UpstreamColumn
		newUpstream *UpstreamColumn
		wantExists  bool
	}{
		{
			name: "exact match exists",
			upstreams: []*UpstreamColumn{
				{Column: "id", Table: "users"},
				{Column: "name", Table: "profiles"},
			},
			newUpstream: &UpstreamColumn{
				Column: "id",
				Table:  "users",
			},
			wantExists: true,
		},
		{
			name: "case insensitive match exists",
			upstreams: []*UpstreamColumn{
				{Column: "ID", Table: "Users"},
				{Column: "name", Table: "profiles"},
			},
			newUpstream: &UpstreamColumn{
				Column: "id",
				Table:  "users",
			},
			wantExists: true,
		},
		{
			name: "no match - different column",
			upstreams: []*UpstreamColumn{
				{Column: "id", Table: "users"},
				{Column: "name", Table: "profiles"},
			},
			newUpstream: &UpstreamColumn{
				Column: "age",
				Table:  "users",
			},
			wantExists: false,
		},
		{
			name: "no match - different table",
			upstreams: []*UpstreamColumn{
				{Column: "id", Table: "users"},
				{Column: "name", Table: "profiles"},
			},
			newUpstream: &UpstreamColumn{
				Column: "id",
				Table:  "employees",
			},
			wantExists: false,
		},
		{
			name:      "empty upstreams list",
			upstreams: []*UpstreamColumn{},
			newUpstream: &UpstreamColumn{
				Column: "id",
				Table:  "users",
			},
			wantExists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := upstreamExists(tt.upstreams, tt.newUpstream)
			if got != tt.wantExists {
				t.Errorf("upstreamExists() = %v, want %v", got, tt.wantExists)
			}
		})
	}
}
