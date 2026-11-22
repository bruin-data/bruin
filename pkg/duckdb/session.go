//go:build !bruin_no_duckdb

package duck

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/bruin-data/bruin/pkg/config"
)

// adbcQuerySession executes multiple SQL statements on a single pinned ADBC connection.
type adbcQuerySession struct {
	conn adbc.Connection
}

func (s *adbcQuerySession) Select(ctx context.Context, queryStr string) ([][]interface{}, error) {
	stmt, err := s.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	if err := stmt.SetSqlQuery(queryStr); err != nil {
		return nil, transactionError(err)
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, transactionError(err)
	}
	if reader == nil {
		return [][]interface{}{}, nil
	}
	defer reader.Release()

	buffered, err := bufferArrowReader(reader)
	if err != nil {
		return nil, err
	}
	return buffered.data, nil
}

func (s *adbcQuerySession) Exec(ctx context.Context, sqlStr string) error {
	return transactionError(execADBCStatement(ctx, s.conn, sqlStr))
}

func (e *EphemeralConnection) RunInSession(ctx context.Context, hintSQL string, fn func(ctx context.Context, s duckDBSession) error) error {
	conn, cleanup, err := e.openADBC(ctx, hintSQL)
	if err != nil {
		return err
	}
	defer cleanup()
	return fn(ctx, &adbcQuerySession{conn: conn})
}

func (c *Client) RunInSession(ctx context.Context, hintSQL string, fn func(ctx context.Context, s duckDBSession) error) error {
	runner, ok := c.connection.(duckDBSessionRunner)
	if !ok {
		return errNoPinnedSession
	}
	return runner.RunInSession(ctx, hintSQL, fn)
}

func (c *Client) LakehouseFormat() config.LakehouseFormat {
	cfg, ok := c.config.(Config)
	if !ok || !cfg.HasLakehouse() {
		return ""
	}
	return cfg.Lakehouse.Format
}
