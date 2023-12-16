package snowflake

import (
	"context"
	"io"
	"strings"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/snowflakedb/gosnowflake"
)

const (
	invalidQueryError = "SQL compilation error"
)

type DB struct {
	conn *sqlx.DB
}

func NewDB(c *Config) (*DB, error) {
	dsn, err := c.DSN()
	if err != nil {
		return nil, errors.Wrap(err, "failed to create DSN")
	}

	gosnowflake.GetLogger().SetOutput(io.Discard)

	db, err := sqlx.Connect("snowflake", dsn)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to snowflake")
	}

	return &DB{conn: db}, nil
}

func (db DB) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return errors.Wrap(err, "failed to create snowflake context")
	}

	rows, err := db.conn.QueryContext(ctx, query.String())
	if err == nil {
		err = rows.Err()
	}

	if err != nil {
		errorMessage := err.Error()
		if strings.Contains(errorMessage, invalidQueryError) {
			errorSegments := strings.Split(errorMessage, "\n")
			if len(errorSegments) > 1 {
				err = errors.New(errorSegments[1])
			}
		}
	}

	if rows != nil {
		defer rows.Close()
	}

	return err
}

func (db DB) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	ctx, err := gosnowflake.WithMultiStatement(ctx, 0)
	if err != nil {
		return false, errors.Wrap(err, "failed to create snowflake context")
	}

	rows, err := db.conn.QueryContext(ctx, query.ToExplainQuery())
	if err == nil {
		err = rows.Err()
	}

	if err != nil {
		errorMessage := err.Error()
		if strings.Contains(errorMessage, invalidQueryError) {
			errorSegments := strings.Split(errorMessage, "\n")
			if len(errorSegments) > 1 {
				err = errors.New(errorSegments[1])
			}
		}
	}

	if rows != nil {
		defer rows.Close()
	}

	return err == nil, err
}
