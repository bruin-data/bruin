package athena

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	drv "github.com/uber/athenadriver/go"
)

type DB struct {
	conn   *sqlx.DB
	config *Config
	mutex  sync.Mutex
}

func NewDB(c *Config) *DB {
	return &DB{
		config: c,
		mutex:  sync.Mutex{},
	}
}

func (db *DB) GetResultsLocation() string {
	return db.config.OutputBucket
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	err := db.initializeDB()
	if err != nil {
		return err
	}
	_, err = db.Select(ctx, query)
	return err
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	err := db.initializeDB()
	if err != nil {
		return nil, err
	}
	queryString := query.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	if err == nil {
		err = rows.Err()
	}

	if err != nil {
		errorMessage := err.Error()
		err = errors.New(strings.ReplaceAll(errorMessage, "\n", "  -  "))
	}

	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		return nil, err
	}

	var result [][]interface{}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		// Scan the result into the column pointers...
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, err
		}

		result = append(result, columns)
	}

	return result, err
}

func (db *DB) SelectWithSchema(ctx context.Context, queryObject *query.Query) (*query.QueryResult, error) {
	// Initialize the database connection
	err := db.initializeDB()
	if err != nil {
		return nil, err
	}

	// Prepare and execute the query
	queryString := queryObject.String()
	rows, err := db.conn.QueryContext(ctx, queryString)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Initialize the QueryResult struct
	result := &query.QueryResult{
		Columns:     []string{},
		Rows:        [][]interface{}{},
		ColumnTypes: []string{},
	}

	// Retrieve column names (schema)
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve column names: %w", err)
	}
	result.Columns = columns

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve column types: %w", err)
	}
	typeStrings := make([]string, len(columnTypes))
	for i, ct := range columnTypes {
		typeStrings[i] = ct.DatabaseTypeName()
	}
	result.ColumnTypes = typeStrings

	// Fetch rows and add them to the result
	for rows.Next() {
		// Create a slice for column values
		columnValues := make([]interface{}, len(columns))
		columnPointers := make([]interface{}, len(columns))
		for i := range columnValues {
			columnPointers[i] = &columnValues[i]
		}

		// Scan the row into column pointers
		if err := rows.Scan(columnPointers...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Append the row to the result
		result.Rows = append(result.Rows, columnValues)
	}

	// Check for any row errors
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error occurred while reading rows: %w", err)
	}

	return result, nil
}

func (db *DB) initializeDB() error {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	if db.conn != nil {
		return nil
	}

	athenaURI, err := db.config.ToDBConnectionURI()
	if err != nil {
		return errors.Wrap(err, "failed to create DSN for Athena")
	}

	if athenaURI == "" {
		return errors.New("failed to create DSN for Athena")
	}

	conn, err := sqlx.Open(drv.DriverName, athenaURI)
	if err != nil {
		return errors.Errorf("Failed to open database connection: %v", err)
	}

	db.conn = conn
	return nil
}

func (db *DB) Ping(ctx context.Context) error {
	q := query.Query{
		Query: "SELECT 1",
	}
	err := db.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Athena connection")
	}

	return nil
}
