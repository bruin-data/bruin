package mssql

import (
	"context"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
)

type DB struct {
	conn   *sqlx.DB
	config *Config
}

type Limiter interface {
	Limit(query string, limit int64) string
}

func NewDB(c *Config) (*DB, error) {
	conn, err := sqlx.Open("mssql", c.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}

	return &DB{conn: conn, config: c}, nil
}

func (db *DB) GetIngestrURI() (string, error) {
	return db.config.GetIngestrURI(), nil
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	_, err := db.Select(ctx, query)
	return err
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
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

func (db *DB) Limit(query string, limit int64) string {
	query = strings.TrimRight(query, "; \n\t")
	return fmt.Sprintf("SELECT TOP %d * FROM (\n%s\n) as t", limit, query)
}
