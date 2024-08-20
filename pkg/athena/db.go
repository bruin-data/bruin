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

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := db.Select(ctx, query)
	return err
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	err := db.initializeDb()
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

func (d *DB) initializeDb() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.conn != nil {
		return nil
	}

	athenaUri := d.config.ToDBConnectionURI()
	if athenaUri == "" {
		return errors.New("failed to create DSN for Athena")
	}

	conn, err := sqlx.Open(drv.DriverName, athenaUri)
	if err != nil {
		return errors.Errorf("Failed to open database connection: %v", err)
	}

	d.conn = conn
	return nil
}
