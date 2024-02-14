package mssql

import (
	"context"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"

	_ "github.com/microsoft/go-mssqldb"
)

type DB struct {
	conn *sqlx.DB
}

func NewDB(c *Config) (*DB, error) {
	conn, err := sqlx.Open("mssql", c.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}

	return &DB{conn: conn}, nil
}

func (db *DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	return nil
}

func (db *DB) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	return nil, nil
}
