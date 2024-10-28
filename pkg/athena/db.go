package athena

import (
	"context"
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

func (db *DB) Test(ctx context.Context) error {
	q := query.Query{
		Query: "SELECT 1",
	}
	err := db.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Athena connection")
	}

	return nil
}
