package postgres

import (
	"context"
	"errors"
	"testing"

	_ "github.com/DATA-DOG/go-sqlmock"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pashagolub/pgxmock/v3"
	"github.com/stretchr/testify/assert"
)

func TestClient_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		expected  string
		setupMock func(mock pgxmock.PgxPoolIface)
		wantErr   string
		want      [][]interface{}
	}{
		{
			name:    "Test Select rows",
			query:   "SELECT * FROM table",
			wantErr: "",
			want:    [][]interface{}{{1, "John Doe"}, {2, "Jane Doe"}},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				).AddRow(1, "John Doe").AddRow(2, "Jane Doe")
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
		{
			name:    "Test Select single row",
			query:   "SELECT * FROM table",
			wantErr: "",
			want:    [][]interface{}{{1, "John Doe"}},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				).AddRow(1, "John Doe")
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
		{
			name:    "Test Select empty rows",
			query:   "SELECT * FROM table",
			wantErr: "",
			want:    [][]interface{}{},
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				)
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
		{
			name:    "Test Select Errors",
			query:   "SELECT * FROM table",
			wantErr: "Some error",
			want:    nil,
			setupMock: func(mock pgxmock.PgxPoolIface) {
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnError(errors.New("Some error"))
			},
		},
		{
			name:    "Test Fail Scanning rows Errors",
			query:   "SELECT * FROM table",
			wantErr: "failed to collect row values: Some scan error",
			want:    nil,
			setupMock: func(mock pgxmock.PgxPoolIface) {
				rows := pgxmock.NewRowsWithColumnDefinition(
					pgconn.FieldDescription{Name: "id"},
					pgconn.FieldDescription{Name: "name"},
				).AddRow(1, "John Doe")
				rows.RowError(1, errors.New("Some scan error"))
				mock.ExpectQuery("SELECT \\* FROM table").WillReturnRows(rows)
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			mock, err := pgxmock.NewPool()
			if err != nil {
				t.Fatal(err)
			}
			defer mock.Close()

			tt.setupMock(mock)

			client := Client{connection: mock}

			result, err := client.Select(context.TODO(), &query.Query{
				Query: tt.query,
			})

			if tt.wantErr == "" {
				assert.NoError(t, err)
			} else {
				assert.Equal(t, tt.wantErr, err.Error())
				assert.Error(t, err)
			}

			assert.Equal(t, tt.want, result)
		})
	}
}
