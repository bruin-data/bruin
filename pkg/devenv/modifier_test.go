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

func (m *mockConnectionFetcher) GetConnection(name string) any {
	args := m.Called(name)
	return args.Get(0)
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
					Return(nil)
			},
			error: "connection 'postgres-default' not found in config file '.bruin.yml' under environment 'default'",
		},
		{
			name:        "connection found but it cannot be used for devenv, error",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			setupFields: func(f *fields) {
				f.Conn.On("GetConnection", "postgres-default").
					Return(new(mockConnectionWithoutDatabaseSummary))
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
				f.Conn.On("GetConnection", "postgres-default").Return(c)

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
				f.Conn.On("GetConnection", "postgres-default").Return(c)

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
				f.Conn.On("GetConnection", "postgres-default").Return(c)

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

			ctx := context.WithValue(t.Context(), config.EnvironmentContextKey, tt.selectedEnv)

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

func TestDevEnvQueryModifier_Modify_ThreePartNames(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{
		DefaultConnections: map[string]string{
			"mssql": "mssql-default",
		},
	}
	a := &pipeline.Asset{
		Name: "dev_myschema.mytable",
		Type: pipeline.AssetTypeMsSQLQuery,
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
			name:        "3-part name rewrites schema part only",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			inputQuery:  "select * from mydb.myschema.mytable",
			outputQuery: "select * from mydb.dev_myschema.mytable",
			setupFields: func(f *fields) {
				c := new(mockConnectionInstance)
				c.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
					Name: "mydb",
					Schemas: []*ansisql.DBSchema{
						{
							Name: "myschema",
							Tables: []*ansisql.DBTable{
								{Name: "mytable"},
							},
						},
						{
							Name: "dev_myschema",
							Tables: []*ansisql.DBTable{
								{Name: "mytable"},
							},
						},
					},
				}, nil)
				f.Conn.On("GetConnection", "mssql-default").Return(c)

				f.Parser.On("UsedTables", "select * from mydb.myschema.mytable", "tsql").
					Return([]string{"mydb.myschema.mytable"}, nil)

				// self-referencing mapping for the asset name is also included
				f.Parser.On(
					"RenameTables",
					"select * from mydb.myschema.mytable",
					"tsql",
					map[string]string{
						"mydb.myschema.mytable": "mydb.dev_myschema.mytable",
						"myschema.mytable":      "dev_myschema.mytable",
					},
				).Return("select * from mydb.dev_myschema.mytable", nil)
			},
		},
		{
			name:        "3-part name not rewritten when dev schema does not exist",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			inputQuery:  "select * from mydb.myschema.mytable",
			outputQuery: "select * from mydb.myschema.mytable",
			setupFields: func(f *fields) {
				c := new(mockConnectionInstance)
				c.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
					Name: "mydb",
					Schemas: []*ansisql.DBSchema{
						{
							Name: "myschema",
							Tables: []*ansisql.DBTable{
								{Name: "mytable"},
							},
						},
					},
				}, nil)
				f.Conn.On("GetConnection", "mssql-default").Return(c)

				f.Parser.On("UsedTables", "select * from mydb.myschema.mytable", "tsql").
					Return([]string{"mydb.myschema.mytable"}, nil)

				// self-referencing mapping is still present even though 3-part rewrite is not possible
				f.Parser.On(
					"RenameTables",
					"select * from mydb.myschema.mytable",
					"tsql",
					map[string]string{
						"myschema.mytable": "dev_myschema.mytable",
					},
				).Return("select * from mydb.myschema.mytable", nil)
			},
		},
		{
			name:        "mix of 2-part and 3-part names",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			inputQuery:  "select * from mydb.myschema.mytable t1 join otherschema.othertable t2 on t1.id = t2.id",
			outputQuery: "select * from mydb.dev_myschema.mytable t1 join dev_otherschema.othertable t2 on t1.id = t2.id",
			setupFields: func(f *fields) {
				c := new(mockConnectionInstance)
				c.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
					Name: "mydb",
					Schemas: []*ansisql.DBSchema{
						{
							Name: "myschema",
							Tables: []*ansisql.DBTable{
								{Name: "mytable"},
							},
						},
						{
							Name: "dev_myschema",
							Tables: []*ansisql.DBTable{
								{Name: "mytable"},
							},
						},
						{
							Name: "otherschema",
							Tables: []*ansisql.DBTable{
								{Name: "othertable"},
							},
						},
						{
							Name: "dev_otherschema",
							Tables: []*ansisql.DBTable{
								{Name: "othertable"},
							},
						},
					},
				}, nil)
				f.Conn.On("GetConnection", "mssql-default").Return(c)

				f.Parser.On("UsedTables",
					"select * from mydb.myschema.mytable t1 join otherschema.othertable t2 on t1.id = t2.id",
					"tsql",
				).Return([]string{"mydb.myschema.mytable", "otherschema.othertable"}, nil)

				f.Parser.On(
					"RenameTables",
					"select * from mydb.myschema.mytable t1 join otherschema.othertable t2 on t1.id = t2.id",
					"tsql",
					map[string]string{
						"myschema.mytable":       "dev_myschema.mytable",
						"mydb.myschema.mytable":  "mydb.dev_myschema.mytable",
						"otherschema.othertable": "dev_otherschema.othertable",
					},
				).Return("select * from mydb.dev_myschema.mytable t1 join dev_otherschema.othertable t2 on t1.id = t2.id", nil)
			},
		},
		{
			name:        "3-part name with partial rewrite when only some dev schemas exist",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			inputQuery:  "select * from db1.schema1.table1 t1 join db2.schema2.table2 t2 on t1.id = t2.id",
			outputQuery: "select * from db1.dev_schema1.table1 t1 join db2.schema2.table2 t2 on t1.id = t2.id",
			setupFields: func(f *fields) {
				c := new(mockConnectionInstance)
				c.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
					Name: "db1",
					Schemas: []*ansisql.DBSchema{
						{
							Name: "schema1",
							Tables: []*ansisql.DBTable{
								{Name: "table1"},
							},
						},
						{
							Name: "dev_schema1",
							Tables: []*ansisql.DBTable{
								{Name: "table1"},
							},
						},
						{
							Name: "schema2",
							Tables: []*ansisql.DBTable{
								{Name: "table2"},
							},
						},
					},
				}, nil)
				f.Conn.On("GetConnection", "mssql-default").Return(c)

				f.Parser.On("UsedTables",
					"select * from db1.schema1.table1 t1 join db2.schema2.table2 t2 on t1.id = t2.id",
					"tsql",
				).Return([]string{"db1.schema1.table1", "db2.schema2.table2"}, nil)

				f.Parser.On(
					"RenameTables",
					"select * from db1.schema1.table1 t1 join db2.schema2.table2 t2 on t1.id = t2.id",
					"tsql",
					map[string]string{
						"myschema.mytable":   "dev_myschema.mytable",
						"db1.schema1.table1": "db1.dev_schema1.table1",
					},
				).Return("select * from db1.dev_schema1.table1 t1 join db2.schema2.table2 t2 on t1.id = t2.id", nil)
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
				Dialect: "tsql",
				Conn:    f.Conn,
				Parser:  f.Parser,
			}

			ctx := context.WithValue(t.Context(), config.EnvironmentContextKey, tt.selectedEnv)

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
