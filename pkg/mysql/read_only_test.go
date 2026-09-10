package mysql

import (
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
)

func TestConfigReadOnlyDSN(t *testing.T) {
	t.Parallel()
	for _, readOnly := range []bool{false, true} {
		cfg := Config{Username: "user", Password: "password", Host: "localhost", Database: "test", ReadOnly: readOnly}
		dsn, err := mysql.ParseDSN(cfg.ToDBConnectionURI())
		require.NoError(t, err)
		require.Equal(t, !readOnly, dsn.MultiStatements)
		if readOnly {
			require.Equal(t, "1", dsn.Params["transaction_read_only"])
		} else {
			require.NotContains(t, dsn.Params, "transaction_read_only")
		}
	}
}

func TestClientReadOnlyQueries(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"select", "schema", "exec"} {
		for _, outcome := range []string{"success", "begin error", "query error", "row error"} {
			t.Run(method+"/"+outcome, func(t *testing.T) {
				t.Parallel()
				db, mock, err := sqlmock.New()
				require.NoError(t, err)
				defer db.Close()
				client := &Client{conn: sqlx.NewDb(db, "sqlmock"), config: Config{ReadOnly: true}}
				begin := mock.ExpectBegin()
				if outcome == "begin error" {
					begin.WillReturnError(errors.New("begin failed"))
				} else {
					if method == "exec" {
						expected := mock.ExpectExec("SELECT 1")
						if outcome == "query error" {
							expected.WillReturnError(errors.New("query failed"))
						} else {
							expected.WillReturnResult(sqlmock.NewResult(0, 0))
						}
					} else {
						expected := mock.ExpectQuery("SELECT 1")
						if outcome == "query error" {
							expected.WillReturnError(errors.New("query failed"))
						} else {
							rows := sqlmock.NewRows([]string{"value"}).AddRow(1)
							if outcome == "row error" {
								rows.RowError(0, errors.New("row failed"))
							}
							expected.WillReturnRows(rows).RowsWillBeClosed()
						}
					}
					mock.ExpectRollback()
				}
				q := &query.Query{Query: "SELECT 1"}
				switch method {
				case "select":
					_, err = client.Select(t.Context(), q)
				case "schema":
					_, err = client.SelectWithSchema(t.Context(), q)
				case "exec":
					err = client.RunQueryWithoutResult(t.Context(), q)
				}
				if outcome == "success" || (method == "exec" && outcome == "row error") {
					require.NoError(t, err)
				} else {
					require.Error(t, err)
				}
				require.NoError(t, mock.ExpectationsWereMet())
			})
		}
	}
}
