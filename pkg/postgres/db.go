package postgres

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/diff"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

type Client struct {
	connection    connection
	config        PgConfig
	schemaCreator *ansisql.SchemaCreator
	typeMapper    *diff.DatabaseTypeMapper
}

type PgConfig interface {
	ToDBConnectionURI() string
	GetIngestrURI() string
	GetDatabase() string
}

type connection interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func NewClient(ctx context.Context, c PgConfig) (*Client, error) {
	conn, err := pgxpool.New(ctx, c.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}

	return &Client{
		connection:    conn,
		config:        c,
		schemaCreator: ansisql.NewSchemaCreator(),
		typeMapper:    diff.NewPostgresTypeMapper(),
	}, nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := c.connection.Exec(ctx, query.String())
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}

// Select runs a query and returns the results.
func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	rows, err := c.connection.Query(ctx, query.String())
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]interface{}, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}

	if len(collectedRows) == 0 {
		return make([][]interface{}, 0), nil
	}

	return collectedRows, nil
}

func (c *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	rows, err := c.connection.Query(ctx, queryObj.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to execute query")
	}
	defer rows.Close()
	// Retrieve column metadata using FieldDescriptions
	fieldDescriptions := rows.FieldDescriptions()
	if fieldDescriptions == nil {
		return nil, errors.New("field descriptions are not available")
	}
	typeMap := pgtype.NewMap()
	// Extract column names
	columns := make([]string, len(fieldDescriptions))
	columnTypes := make([]string, len(fieldDescriptions))
	for i, field := range fieldDescriptions {
		columns[i] = field.Name
		dataType, ok := typeMap.TypeForOID(field.DataTypeOID)
		if !ok {
			columnTypes[i] = ""
		} else {
			columnTypes[i] = dataType.Name
		}
	}

	// Collect rows
	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]interface{}, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}
	result := &query.QueryResult{
		Columns:     columns,
		Rows:        collectedRows,
		ColumnTypes: columnTypes,
	}
	return result, nil
}

// Test runs a simple query (SELECT 1) to validate the connection.
func (c *Client) Ping(ctx context.Context) error {
	q := query.Query{
		Query: "SELECT 1",
	}
	err := c.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Postgres connection")
	}

	return nil
}

func (c *Client) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	rows, err := c.connection.Query(ctx, query.ToExplainQuery())
	if err == nil {
		err = rows.Err()
	}

	if rows != nil {
		defer rows.Close()
	}

	return err == nil, err
}

func (c *Client) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	db := c.config.GetDatabase()
	q := `
SELECT
    table_schema,
    table_name
FROM
    information_schema.tables
WHERE
	table_catalog = $1 AND table_schema NOT IN ('pg_catalog', 'information_schema')
ORDER BY table_schema, table_name;
`

	rows, err := c.connection.Query(ctx, q, db)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]any, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}

	summary := &ansisql.DBDatabase{
		Name:    db,
		Schemas: []*ansisql.DBSchema{},
	}
	schemas := make(map[string]*ansisql.DBSchema)

	for _, row := range collectedRows {
		if len(row) != 2 {
			continue
		}

		schemaName := row[0].(string)
		tableName := row[1].(string)

		// Create schema if it doesn't exist
		schemaKey := db + "." + schemaName
		if _, exists := schemas[schemaKey]; !exists {
			schema := &ansisql.DBSchema{
				Name:   schemaName,
				Tables: []*ansisql.DBTable{},
			}
			schemas[schemaKey] = schema
		}

		// Add table to schema
		table := &ansisql.DBTable{
			Name: tableName,
		}
		schemas[schemaKey].Tables = append(schemas[schemaKey].Tables, table)
	}

	for _, schema := range schemas {
		summary.Schemas = append(summary.Schemas, schema)
	}

	// Sort schemas by name
	sort.Slice(summary.Schemas, func(i, j int) bool {
		return summary.Schemas[i].Name < summary.Schemas[j].Name
	})

	return summary, nil
}

func (c *Client) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	return c.schemaCreator.CreateSchemaIfNotExist(ctx, c, asset)
}

func (c *Client) GetTableSummary(ctx context.Context, tableName string, schemaOnly bool) (*diff.TableSummaryResult, error) {
	var rowCount int64

	// Get row count only if not in schema-only mode
	if !schemaOnly {
		rows, err := c.connection.Query(ctx, "SELECT COUNT(*) as row_count FROM "+tableName)
		if err != nil {
			return nil, fmt.Errorf("failed to execute count query for table '%s': %w", tableName, err)
		}
		defer rows.Close()

		if rows.Next() {
			if err := rows.Scan(&rowCount); err != nil {
				return nil, fmt.Errorf("failed to scan row count for table '%s': %w", tableName, err)
			}
		}
		if err = rows.Err(); err != nil {
			return nil, fmt.Errorf("error after iterating rows for count query on table '%s': %w", tableName, err)
		}
	}

	// Get table schema using information_schema
	schemaQuery := `
	SELECT 
		column_name,
		data_type,
		is_nullable,
		column_default,
		character_maximum_length,
		numeric_precision,
		numeric_scale
	FROM information_schema.columns 
	WHERE table_name = $1
	ORDER BY ordinal_position`

	schemaRows, err := c.connection.Query(ctx, schemaQuery, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to execute schema query for table '%s': %w", tableName, err)
	}
	defer schemaRows.Close()

	var columns []*diff.Column
	for schemaRows.Next() {
		var (
			columnName       string
			dataType         string
			isNullable       string
			columnDefault    *string
			charMaxLength    *int
			numericPrecision *int
			numericScale     *int
		)

		if err := schemaRows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &charMaxLength, &numericPrecision, &numericScale); err != nil {
			return nil, fmt.Errorf("failed to scan schema info for table '%s': %w", tableName, err)
		}

		// Build the full type name with precision/scale if available
		fullType := dataType
		if charMaxLength != nil && *charMaxLength > 0 {
			fullType = fmt.Sprintf("%s(%d)", dataType, *charMaxLength)
		} else if numericPrecision != nil && numericScale != nil && *numericPrecision > 0 {
			if *numericScale > 0 {
				fullType = fmt.Sprintf("%s(%d,%d)", dataType, *numericPrecision, *numericScale)
			} else {
				fullType = fmt.Sprintf("%s(%d)", dataType, *numericPrecision)
			}
		}

		normalizedType := c.typeMapper.MapType(dataType)
		nullable := strings.ToUpper(isNullable) == "YES"

		// TODO: Add logic to detect primary keys and unique constraints
		// This would require additional queries to information_schema.table_constraints
		// and information_schema.key_column_usage

		var stats diff.ColumnStatistics
		if schemaOnly {
			// In schema-only mode, don't collect statistics
			stats = nil
		} else {
			switch normalizedType {
			case diff.CommonTypeNumeric:
				stats, err = c.fetchNumericalStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch numerical stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeString:
				stats, err = c.fetchStringStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch string stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeBoolean:
				stats, err = c.fetchBooleanStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch boolean stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeDateTime:
				stats, err = c.fetchDateTimeStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch datetime stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeJSON:
				stats, err = c.fetchJSONStats(ctx, tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch JSON stats for column '%s': %w", columnName, err)
				}
			case diff.CommonTypeBinary, diff.CommonTypeUnknown:
				stats = &diff.UnknownStatistics{}
			}
		}

		columns = append(columns, &diff.Column{
			Name:           columnName,
			Type:           fullType,
			NormalizedType: normalizedType,
			Nullable:       nullable,
			PrimaryKey:     false, // TODO: Implement PK detection
			Unique:         false, // TODO: Implement unique constraint detection
			Stats:          stats,
		})
	}
	if err = schemaRows.Err(); err != nil {
		return nil, fmt.Errorf("error after iterating schema rows for table '%s': %w", tableName, err)
	}

	dbTable := &diff.Table{
		Name:    tableName,
		Columns: columns,
	}

	return &diff.TableSummaryResult{
		RowCount: rowCount,
		Table:    dbTable,
	}, nil
}

func (c *Client) fetchNumericalStats(ctx context.Context, tableName, columnName string) (*diff.NumericalStatistics, error) {
	stats := &diff.NumericalStatistics{}
	query := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            MIN(%s) as min_val,
            MAX(%s) as max_val,
            AVG(%s::float) as avg_val,
            SUM(%s::float) as sum_val,
            STDDEV(%s::float) as stddev_val
        FROM %s`,
		columnName, columnName, columnName, columnName, columnName, columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var minVal, maxVal, avgVal, sumVal, stddevVal *float64
		err := rows.Scan(&stats.Count, &stats.NullCount, &minVal, &maxVal, &avgVal, &sumVal, &stddevVal)
		if err != nil {
			return nil, err
		}

		stats.Min = minVal
		stats.Max = maxVal
		stats.Avg = avgVal
		stats.Sum = sumVal
		stats.StdDev = stddevVal
	}

	return stats, rows.Err()
}

func (c *Client) fetchStringStats(ctx context.Context, tableName, columnName string) (*diff.StringStatistics, error) {
	stats := &diff.StringStatistics{}
	query := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            COUNT(DISTINCT %s) as distinct_count,
            COUNT(CASE WHEN %s = '' THEN 1 END) as empty_count,
            MIN(LENGTH(%s)) as min_length,
            MAX(LENGTH(%s)) as max_length,
            AVG(LENGTH(%s)) as avg_length
        FROM %s`,
		columnName, columnName, columnName, columnName, columnName, columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var avgLength float64
		err := rows.Scan(&stats.Count, &stats.NullCount, &stats.DistinctCount,
			&stats.EmptyCount, &stats.MinLength, &stats.MaxLength, &avgLength)
		if err != nil {
			return nil, err
		}

		stats.AvgLength = avgLength
	}

	return stats, rows.Err()
}

func (c *Client) fetchBooleanStats(ctx context.Context, tableName, columnName string) (*diff.BooleanStatistics, error) {
	stats := &diff.BooleanStatistics{}
	query := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            COUNT(CASE WHEN %s = true THEN 1 END) as true_count,
            COUNT(CASE WHEN %s = false THEN 1 END) as false_count
        FROM %s`,
		columnName, columnName, columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&stats.Count, &stats.NullCount, &stats.TrueCount, &stats.FalseCount)
		if err != nil {
			return nil, err
		}
	}

	return stats, rows.Err()
}

func (c *Client) fetchDateTimeStats(ctx context.Context, tableName, columnName string) (*diff.DateTimeStatistics, error) {
	stats := &diff.DateTimeStatistics{}
	query := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count,
            COUNT(DISTINCT %s) as unique_count,
            MIN(%s)::text as earliest_date,
            MAX(%s)::text as latest_date
        FROM %s`,
		columnName, columnName, columnName, columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var earliestDate, latestDate *string
		err := rows.Scan(&stats.Count, &stats.NullCount, &stats.UniqueCount,
			&earliestDate, &latestDate)
		if err != nil {
			return nil, err
		}

		stats.EarliestDate = earliestDate
		stats.LatestDate = latestDate
	}

	return stats, rows.Err()
}

func (c *Client) fetchJSONStats(ctx context.Context, tableName, columnName string) (*diff.JSONStatistics, error) {
	stats := &diff.JSONStatistics{}
	query := fmt.Sprintf(`
        SELECT 
            COUNT(*) as count,
            COUNT(*) - COUNT(%s) as null_count
        FROM %s`,
		columnName, tableName)

	rows, err := c.connection.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		err := rows.Scan(&stats.Count, &stats.NullCount)
		if err != nil {
			return nil, err
		}
	}

	return stats, rows.Err()
}

func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''") // Escape single quotes for SQL safety
}

func (c *Client) PushColumnDescriptions(ctx context.Context, asset *pipeline.Asset) error {
	tableComponents := strings.Split(asset.Name, ".")
	var schemaName string
	var tableName string
	switch len(tableComponents) {
	case 2:
		schemaName = strings.ToUpper(tableComponents[0])
		tableName = strings.ToUpper(tableComponents[1])
	case 3:
		schemaName = strings.ToUpper(tableComponents[1])
		tableName = strings.ToUpper(tableComponents[2])
	default:
		return nil
	}

	if asset.Description == "" && len(asset.Columns) == 0 {
		return errors.New("no metadata to push: table and columns have no descriptions")
	}

	queryStr := fmt.Sprintf(`
			SELECT 
				cols.column_name,
				pgd.description
			FROM 
				pg_catalog.pg_statio_all_tables AS st
			JOIN 
				pg_catalog.pg_description pgd 
				ON pgd.objoid = st.relid
			JOIN 
				information_schema.columns cols 
				ON cols.table_schema = st.schemaname
				AND cols.table_name = st.relname
				AND cols.ordinal_position = pgd.objsubid
			WHERE 
				cols.table_name = '%s'
				AND cols.table_schema = '%s';`,
		tableName, schemaName)

	rows, err := c.Select(ctx, &query.Query{Query: queryStr})
	if err != nil {
		return errors.Wrapf(err, "failed to query column metadata for %s.%s", schemaName, tableName)
	}

	existingComments := make(map[string]string)
	for _, row := range rows {
		columnName := row[0].(string)
		comment := ""
		if row[1] != nil {
			comment = row[1].(string)
		}
		existingComments[columnName] = comment
	}

	// Find columns that need updates
	var updateQueries []string
	for _, col := range asset.Columns {
		if col.Description != "" && existingComments[col.Name] != col.Description {
			query := fmt.Sprintf(
				`COMMENT ON COLUMN %s.%s.%s IS '%s'`,
				schemaName, tableName, col.Name, escapeSQLString(col.Description),
			)
			updateQueries = append(updateQueries, query)
		}
	}
	if len(updateQueries) > 0 {
		batchQuery := strings.Join(updateQueries, "; ")
		if err := c.RunQueryWithoutResult(ctx, &query.Query{Query: batchQuery}); err != nil {
			return errors.Wrap(err, "failed to update column descriptions")
		}
	}

	if asset.Description != "" {
		updateTableQuery := fmt.Sprintf(
			`COMMENT ON TABLE %s.%s.%s IS '%s'`,
			schemaName, tableName, escapeSQLString(asset.Description),
		)
		if err := c.RunQueryWithoutResult(ctx, &query.Query{Query: updateTableQuery}); err != nil {
			return errors.Wrap(err, "failed to update table description")
		}
	}

	err = c.PrintTableMetadata(ctx, asset.Name)

	return nil
}

// PrintTableMetadata queries and prints all columns with their metadata for the given 'schema.table'.
func (c *Client) PrintTableMetadata(ctx context.Context, schemaTable string) error {
	tableParts := strings.Split(schemaTable, ".")
	var schemaName, tableName string
	if len(tableParts) == 2 {
		schemaName = tableParts[0]
		tableName = tableParts[1]
	} else if len(tableParts) == 1 {
		schemaName = "public"
		tableName = tableParts[0]
	} else {
		return errors.Errorf("table name must be in schema.table or table format, '%s' given", schemaTable)
	}

	queryStr := `
			SELECT c.column_name, pgd.description
			FROM information_schema.columns c
			LEFT JOIN pg_catalog.pg_statio_all_tables as st
			  ON c.table_schema = st.schemaname AND c.table_name = st.relname
			LEFT JOIN pg_catalog.pg_description pgd
			  ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
			WHERE c.table_name = $1 AND c.table_schema = $2
			ORDER BY c.ordinal_position`

	rows, err := c.connection.Query(ctx, queryStr, tableName, schemaName)
	if err != nil {
		return errors.Wrapf(err, "failed to query table metadata for %s.%s", schemaName, tableName)
	}
	defer rows.Close()

	fmt.Printf("Columns for table '%s.%s':\n", schemaName, tableName)
	fmt.Printf("%-32s %-s\n", "Name", "Description")
	for rows.Next() {
		var name string
		var description *string
		if err := rows.Scan(&name, &description); err != nil {
			return errors.Wrap(err, "failed to scan column metadata")
		}
		descVal := ""
		if description != nil {
			descVal = *description
		}
		fmt.Printf("%-32s %-s\n", name, descVal)
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "error iterating over column metadata rows")
	}
	return nil
}
