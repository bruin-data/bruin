package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/require"
)

func TestReadOnlyConnectionQuery(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"success", "query error", "row error", "early close", "canceled context"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			mock.ExpectBeginTx(pgx.TxOptions{AccessMode: pgx.ReadOnly})
			expected := mock.ExpectQuery("SELECT").WithArgs(pgx.QueryExecModeExec, 42)
			if name == "query error" {
				expected.WillReturnError(errors.New("query failed"))
			} else {
				rows := pgxmock.NewRows([]string{"value"}).AddRow(42).AddRow(43)
				if name == "row error" {
					rows.RowError(0, errors.New("row failed"))
				}
				expected.WillReturnRows(rows).RowsWillBeClosed()
			}
			mock.ExpectRollback()
			conn := &readOnlyConnection{pool: mock}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			rows, err := conn.Query(ctx, "SELECT $1", 42)
			if name == "query error" {
				require.EqualError(t, err, "query failed")
			} else {
				require.NoError(t, err)
				if name == "canceled context" {
					cancel()
				}
				if name == "success" || name == "row error" {
					for rows.Next() {
						_, err := rows.Values()
						if name == "row error" {
							require.EqualError(t, err, "row failed")
							break
						}
						require.NoError(t, err)
					}
					if name == "row error" {
						require.EqualError(t, rows.Err(), "row failed")
					} else {
						require.NoError(t, rows.Err())
					}
				}
				rows.Close()
				rows.Close()
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadOnlyConnectionExec(t *testing.T) {
	t.Parallel()
	for _, fails := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "write rejected"}[fails], func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			require.NoError(t, err)
			defer mock.Close()
			mock.ExpectBeginTx(pgx.TxOptions{AccessMode: pgx.ReadOnly})
			expected := mock.ExpectQuery("DELETE FROM items").WithArgs(pgx.QueryExecModeExec)
			if fails {
				expected.WillReturnError(errors.New("read-only transaction"))
			} else {
				expected.WillReturnRows(pgxmock.NewRows([]string{"value"}).AddRow(1)).RowsWillBeClosed()
			}
			mock.ExpectRollback()
			client := &Client{connection: &readOnlyConnection{pool: mock}}
			err = client.RunQueryWithoutResult(t.Context(), &query.Query{Query: "DELETE FROM items"})
			if fails {
				require.EqualError(t, err, "read-only transaction")
			} else {
				require.NoError(t, err)
			}
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestReadOnlyConnectionBeginFailure(t *testing.T) {
	t.Parallel()
	for _, exec := range []bool{false, true} {
		mock, err := pgxmock.NewPool()
		require.NoError(t, err)
		defer mock.Close()
		mock.ExpectBeginTx(pgx.TxOptions{AccessMode: pgx.ReadOnly}).WillReturnError(errors.New("unavailable"))
		conn := &readOnlyConnection{pool: mock}
		if exec {
			_, err = conn.Exec(t.Context(), "SELECT 1")
		} else {
			rows, queryErr := conn.Query(t.Context(), "SELECT 1")
			if rows != nil {
				rows.Close()
			}
			err = queryErr
		}
		require.EqualError(t, err, "failed to start read-only transaction: unavailable")
		require.NoError(t, mock.ExpectationsWereMet())
	}
}
