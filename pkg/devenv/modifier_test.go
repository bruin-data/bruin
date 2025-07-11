package devenv

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockConnectionFetcher struct {
	mock.Mock
}

func (m *mockConnectionFetcher) GetConnection(name string) (interface{}, error) {
	args := m.Called(name)
	get := args.Get(0)
	if get == nil {
		return nil, args.Error(1)
	}

	return get, args.Error(1)
}

type mockSQLParser struct {
	mock.Mock
}

func (m *mockSQLParser) UsedTables(sql, dialect string) ([]string, error) {
	args := m.Called(sql, dialect)
	return args.Get(0).([]string), args.Error(1)
}

func (m *mockSQLParser) RenameTables(sql, dialect string, tableMapping map[string]string) (string, error) {
	args := m.Called(sql, dialect, tableMapping)
	return args.Get(0).(string), args.Error(1)
}

type mockConnectionInstance struct {
	mock.Mock
}

func (m *mockConnectionInstance) GetDatabaseSummary(ctx context.Context) (*ansisql.DBDatabase, error) {
	args := m.Called(ctx)
	return args.Get(0).(*ansisql.DBDatabase), args.Error(1)
}

type mockConnectionWithoutDatabaseSummary struct{}

func TestDevEnvQueryModifier_Modify(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{
		DefaultConnections: map[string]string{
			"postgres": "postgres-default",
		},
	}
	a := &pipeline.Asset{
		Name: "schema1.table1",
		Type: pipeline.AssetTypePostgresQuery,
	}

	type fields struct {
		Conn   *mockConnectionFetcher
		Parser *mockSQLParser
	}
	tests := []struct {
		name        string
		selectedEnv *config.Environment
		setupFields func(f *fields)
		inputQuery  string
		outputQuery string
		error       string
	}{
		{
			name:        "no environment, nothing changes",
			selectedEnv: nil,
			setupFields: nil,
			inputQuery:  "select 1",
			outputQuery: "select 1",
		},
		{
			name:        "environment exist but no prefix, nothing changes",
			selectedEnv: &config.Environment{},
			setupFields: nil,
			inputQuery:  "select 1",
			outputQuery: "select 1",
		},
		{
			name:        "connection not found, error",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			setupFields: func(f *fields) {
				f.Conn.On("GetConnection", "postgres-default").
					Return(nil, errors.New("failed to get connection"))
			},
			error: "failed to get connection",
		},
		{
			name:        "connection found but it cannot be used for devenv, error",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			setupFields: func(f *fields) {
				f.Conn.On("GetConnection", "postgres-default").
					Return(new(mockConnectionWithoutDatabaseSummary), nil)
			},
			error: "the asset type 'pg.sql' does not support developer environments, please create an issue if you'd like that",
		},
		{
			name:        "query parser returns an error",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			inputQuery:  "select * from schema1.table1 t1 join schema2.table1 t2 using (someid)",
			setupFields: func(f *fields) {
				c := new(mockConnectionInstance)
				c.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
					Name: "db1",
					Schemas: []*ansisql.DBSchema{
						{
							Name: "schema1",
							Tables: []*ansisql.DBTable{
								{Name: "table1"},
								{Name: "table2"},
							},
						},
						{
							Name: "schema2",
							Tables: []*ansisql.DBTable{
								{Name: "table1"},
								{Name: "table2"},
							},
						},
						{
							Name: "dev_schema1",
							Tables: []*ansisql.DBTable{
								{Name: "table1"},
								{Name: "table2"},
							},
						},
					},
				}, nil)
				f.Conn.On("GetConnection", "postgres-default").Return(c, nil)

				f.Parser.On("UsedTables", "select * from schema1.table1 t1 join schema2.table1 t2 using (someid)", "postgres").
					Return([]string{}, errors.New("failed to get used tables"))
			},
			error: "failed to get used tables",
		},
		{
			name:        "db summary returns an error",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			inputQuery:  "select * from schema1.table1 t1 join schema2.table1 t2 using (someid)",
			setupFields: func(f *fields) {
				c := new(mockConnectionInstance)
				c.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{}, errors.New("failed to get db summary"))
				f.Conn.On("GetConnection", "postgres-default").Return(c, nil)

				f.Parser.On("UsedTables", "select * from schema1.table1 t1 join schema2.table1 t2 using (someid)", "postgres").
					Return([]string{}, nil)
			},
			error: "failed to get db summary",
		},
		{
			name:        "renaming works fine when a partial table exists",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			inputQuery:  "select * from schema1.table1 t1 join schema2.table1 t2 using (someid)",
			outputQuery: "select * from dev_schema1.table1 t1 join schema2.table1 t2 using (someid)",
			setupFields: func(f *fields) {
				c := new(mockConnectionInstance)
				c.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
					Name: "db1",
					Schemas: []*ansisql.DBSchema{
						{
							Name: "schema1",
							Tables: []*ansisql.DBTable{
								{Name: "table1"},
								{Name: "table2"},
							},
						},
						{
							Name: "schema2",
							Tables: []*ansisql.DBTable{
								{Name: "table1"},
								{Name: "table2"},
							},
						},
						{
							Name: "dev_schema1",
							Tables: []*ansisql.DBTable{
								{Name: "table1"},
								{Name: "table2"},
							},
						},
					},
				}, nil)
				f.Conn.On("GetConnection", "postgres-default").Return(c, nil)

				f.Parser.On("UsedTables", "select * from schema1.table1 t1 join schema2.table1 t2 using (someid)", "postgres").
					Return([]string{"schema1.table1", "schema2.table1"}, nil)

				f.Parser.On(
					"RenameTables",
					"select * from schema1.table1 t1 join schema2.table1 t2 using (someid)",
					"postgres",
					map[string]string{"schema1.table1": "dev_schema1.table1"},
				).Return("select * from dev_schema1.table1 t1 join schema2.table1 t2 using (someid)", nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fields{
				Conn:   new(mockConnectionFetcher),
				Parser: new(mockSQLParser),
			}

			if tt.setupFields != nil {
				tt.setupFields(f)
			}

			d := &DevEnvQueryModifier{
				Dialect: "postgres",
				Conn:    f.Conn,
				Parser:  f.Parser,
			}

			ctx := context.WithValue(context.Background(), config.EnvironmentContextKey, tt.selectedEnv)

			got, err := d.Modify(ctx, p, a, &query.Query{Query: tt.inputQuery})
			if tt.error != "" && (err == nil || tt.error != err.Error()) {
				t.Errorf("Modify() error = %v, wantErr %v", err, tt.error)
				return
			}

			if tt.outputQuery == "" {
				assert.Nil(t, got)
				return
			}

			if !reflect.DeepEqual(got, &query.Query{Query: tt.outputQuery}) {
				t.Errorf("Modify() got = %v, want %v", got, tt.outputQuery)
			}
		})
	}
}
