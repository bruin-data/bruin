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
	"github.com/stretchr/testify/require"
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

type mockTableCheckingConnectionInstance struct {
	mockConnectionInstance
}

func (m *mockTableCheckingConnectionInstance) BuildTableExistsQuery(tableName string) (string, error) {
	args := m.Called(tableName)
	return args.String(0), args.Error(1)
}

func (m *mockTableCheckingConnectionInstance) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
	args := m.Called(ctx, q)
	return args.Get(0).([][]interface{}), args.Error(1)
}

type mockMetadataTableCheckingConnection struct {
	mock.Mock
}

func (m *mockMetadataTableCheckingConnection) TableExists(ctx context.Context, tableName string) (bool, error) {
	args := m.Called(ctx, tableName)
	return args.Bool(0), args.Error(1)
}

type mockBulkTableCheckingConnection struct {
	mockConnectionInstance
}

func (m *mockBulkTableCheckingConnection) TablesExist(
	ctx context.Context,
	tableNames []string,
) (map[string]bool, error) {
	args := m.Called(ctx, tableNames)
	return args.Get(0).(map[string]bool), args.Error(1)
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
			name:        "3-part cross-database name rewrites when dev table exists",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			inputQuery:  "select * from upstreamdb.myschema.mytable",
			outputQuery: "select * from upstreamdb.dev_myschema.mytable",
			setupFields: func(f *fields) {
				c := new(mockTableCheckingConnectionInstance)
				c.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
					Name:    "currentdb",
					Schemas: []*ansisql.DBSchema{},
				}, nil)
				c.On("BuildTableExistsQuery", "upstreamdb.dev_myschema.mytable").
					Return("table exists query", nil)
				c.On("Select", mock.Anything, &query.Query{Query: "table exists query"}).
					Return([][]interface{}{{int64(1)}}, nil)
				f.Conn.On("GetConnection", "mssql-default").Return(c)

				f.Parser.On("UsedTables", "select * from upstreamdb.myschema.mytable", "tsql").
					Return([]string{"upstreamdb.myschema.mytable"}, nil)

				f.Parser.On(
					"RenameTables",
					"select * from upstreamdb.myschema.mytable",
					"tsql",
					map[string]string{
						"myschema.mytable":            "dev_myschema.mytable",
						"upstreamdb.myschema.mytable": "upstreamdb.dev_myschema.mytable",
					},
				).Return("select * from upstreamdb.dev_myschema.mytable", nil)
			},
		},
		{
			name:        "3-part cross-database name is not rewritten when dev table does not exist upstream",
			selectedEnv: &config.Environment{SchemaPrefix: "dev_"},
			inputQuery:  "select * from upstreamdb.myschema.mytable",
			outputQuery: "select * from upstreamdb.myschema.mytable",
			setupFields: func(f *fields) {
				c := new(mockTableCheckingConnectionInstance)
				c.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
					Name: "currentdb",
					Schemas: []*ansisql.DBSchema{
						{
							Name: "dev_myschema",
							Tables: []*ansisql.DBTable{
								{Name: "mytable"},
							},
						},
					},
				}, nil)
				c.On("BuildTableExistsQuery", "upstreamdb.dev_myschema.mytable").
					Return("table exists query", nil)
				c.On("Select", mock.Anything, &query.Query{Query: "table exists query"}).
					Return([][]interface{}{{int64(0)}}, nil)
				f.Conn.On("GetConnection", "mssql-default").Return(c)

				f.Parser.On("UsedTables", "select * from upstreamdb.myschema.mytable", "tsql").
					Return([]string{"upstreamdb.myschema.mytable"}, nil)

				f.Parser.On(
					"RenameTables",
					"select * from upstreamdb.myschema.mytable",
					"tsql",
					map[string]string{
						"myschema.mytable": "dev_myschema.mytable",
					},
				).Return("select * from upstreamdb.myschema.mytable", nil)
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

				f.Parser.On(
					"UsedTables",
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

				f.Parser.On(
					"UsedTables",
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

func TestDevEnvQueryModifier_Modify_ThreePartAssetWithCatalogQualifiedSummary(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{
		DefaultConnections: map[string]string{"spark": "spark-default"},
	}
	asset := &pipeline.Asset{
		Name: "local.dev_target.output",
		Type: pipeline.AssetTypeSparkQuery,
	}
	connection := new(mockConnectionInstance)
	connection.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
		Name: "local",
		Schemas: []*ansisql.DBSchema{{
			Name:   "local.dev_source",
			Tables: []*ansisql.DBTable{{Name: "events"}},
		}},
	}, nil)
	connectionFetcher := new(mockConnectionFetcher)
	connectionFetcher.On("GetConnection", "spark-default").Return(connection)
	sqlParser := new(mockSQLParser)
	inputQuery := "SELECT * FROM source.events UNION ALL SELECT * FROM local.source.events"
	sqlParser.On("UsedTables", inputQuery, "spark").
		Return([]string{"source.events", "local.source.events"}, nil)
	expectedMapping := map[string]string{
		"local.target.output": "local.dev_target.output",
		"source.events":       "dev_source.events",
		"local.source.events": "local.dev_source.events",
	}
	outputQuery := "SELECT * FROM dev_source.events UNION ALL SELECT * FROM local.dev_source.events"
	sqlParser.On("RenameTables", inputQuery, "spark", expectedMapping).Return(outputQuery, nil)

	modifier := &DevEnvQueryModifier{
		Dialect: "spark",
		Conn:    connectionFetcher,
		Parser:  sqlParser,
	}
	ctx := context.WithValue(
		t.Context(),
		config.EnvironmentContextKey,
		&config.Environment{SchemaPrefix: "dev_"},
	)

	got, err := modifier.Modify(ctx, p, asset, &query.Query{Query: inputQuery})
	require.NoError(t, err)
	require.Equal(t, &query.Query{Query: outputQuery}, got)
	connection.AssertExpectations(t)
	connectionFetcher.AssertExpectations(t)
	sqlParser.AssertExpectations(t)
}

func TestDevEnvQueryModifier_RegisterThreePartAssetInCatalogQualifiedSummary(t *testing.T) {
	t.Parallel()

	summary := &ansisql.DBDatabase{
		Name: "local",
		Schemas: []*ansisql.DBSchema{{
			Name: "local.default",
		}},
	}
	modifier := &DevEnvQueryModifier{
		connSchemaCache: map[string]*ansisql.DBDatabase{"spark-default": summary},
	}
	p := &pipeline.Pipeline{
		DefaultConnections: map[string]string{"spark": "spark-default"},
	}
	asset := &pipeline.Asset{
		Name: "local.dev_target.output",
		Type: pipeline.AssetTypeSparkQuery,
	}

	require.NoError(t, modifier.RegisterAssetForSchemaCache(t.Context(), p, asset, &query.Query{}))
	require.Len(t, summary.Schemas, 2)
	require.Equal(t, "local.dev_target", summary.Schemas[1].Name)
	require.Equal(t, []*ansisql.DBTable{{Name: "output"}}, summary.Schemas[1].Tables)
}

func TestDevEnvQueryModifier_RegisterCrossCatalogAssetSeparately(t *testing.T) {
	t.Parallel()

	summary := &ansisql.DBDatabase{
		Name: "local",
		Schemas: []*ansisql.DBSchema{{
			Name:   "dev_target",
			Tables: []*ansisql.DBTable{{Name: "existing"}},
		}},
	}
	modifier := &DevEnvQueryModifier{
		connSchemaCache: map[string]*ansisql.DBDatabase{"spark-default": summary},
	}
	p := &pipeline.Pipeline{
		DefaultConnections: map[string]string{"spark": "spark-default"},
	}
	asset := &pipeline.Asset{
		Name: "other.dev_target.output",
		Type: pipeline.AssetTypeSparkQuery,
	}

	require.NoError(t, modifier.RegisterAssetForSchemaCache(t.Context(), p, asset, &query.Query{}))
	require.Len(t, summary.Schemas, 2)
	require.Equal(t, "other.dev_target", summary.Schemas[1].Name)
	require.Equal(t, []*ansisql.DBTable{{Name: "output"}}, summary.Schemas[1].Tables)
	require.Equal(t, []*ansisql.DBTable{{Name: "existing"}}, summary.Schemas[0].Tables)
}

func TestDevEnvQueryModifier_SparkSummaryLookupIsCaseInsensitive(t *testing.T) {
	t.Parallel()

	summary := &ansisql.DBDatabase{
		Name: "local",
		Schemas: []*ansisql.DBSchema{{
			Name:   "local.dev_source",
			Tables: []*ansisql.DBTable{{Name: "events"}},
		}},
	}
	sparkModifier := &DevEnvQueryModifier{Dialect: "spark"}
	postgresModifier := &DevEnvQueryModifier{Dialect: "postgres"}

	require.True(t, sparkModifier.databaseSummaryTableExists(summary, "LOCAL", "DEV_SOURCE", "EVENTS"))
	require.False(t, postgresModifier.databaseSummaryTableExists(summary, "LOCAL", "DEV_SOURCE", "EVENTS"))
}

func TestDevEnvQueryModifier_SparkSummaryLookupKeepsCatalogsSeparate(t *testing.T) {
	t.Parallel()

	summary := &ansisql.DBDatabase{
		Name: "local",
		Schemas: []*ansisql.DBSchema{{
			Name:   "dev_source",
			Tables: []*ansisql.DBTable{{Name: "events"}},
		}},
	}
	modifier := &DevEnvQueryModifier{Dialect: "spark"}

	require.True(t, modifier.databaseSummaryTableExists(summary, "LOCAL", "DEV_SOURCE", "EVENTS"))
	require.False(t, modifier.databaseSummaryTableExists(summary, "other", "dev_source", "events"))
}

func TestTableExistsInDatabasePrefersMetadataChecker(t *testing.T) {
	t.Parallel()

	connection := new(mockMetadataTableCheckingConnection)
	connection.On("TableExists", mock.Anything, "spark_catalog.dev_source.events").Return(true, nil)

	exists, err := tableExistsInDatabase(t.Context(), connection, "spark_catalog.dev_source.events")
	require.NoError(t, err)
	require.True(t, exists)
	connection.AssertExpectations(t)
}

func TestDevEnvQueryModifierBatchesCrossCatalogTableChecks(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{
		DefaultConnections: map[string]string{"spark": "spark-default"},
	}
	asset := &pipeline.Asset{
		Name: "dev_target.output",
		Type: pipeline.AssetTypeSparkQuery,
	}
	connection := new(mockBulkTableCheckingConnection)
	connection.On("GetDatabaseSummary", mock.Anything).Return(&ansisql.DBDatabase{
		Name:    "local",
		Schemas: []*ansisql.DBSchema{},
	}, nil)
	crossCatalogTables := []string{
		"other.dev_source.events",
		"other.dev_source.users",
	}
	connection.On("TablesExist", mock.Anything, crossCatalogTables).Return(map[string]bool{
		"other.dev_source.events": true,
		"other.dev_source.users":  false,
	}, nil).Once()
	connectionFetcher := new(mockConnectionFetcher)
	connectionFetcher.On("GetConnection", "spark-default").Return(connection)
	sqlParser := new(mockSQLParser)
	inputQuery := "SELECT * FROM other.source.events UNION ALL SELECT * FROM other.source.users"
	sqlParser.On("UsedTables", inputQuery, "spark").Return(
		[]string{"other.source.events", "other.source.users"},
		nil,
	)
	expectedMapping := map[string]string{
		"target.output":       "dev_target.output",
		"other.source.events": "other.dev_source.events",
	}
	outputQuery := "SELECT * FROM other.dev_source.events UNION ALL SELECT * FROM other.source.users"
	sqlParser.On("RenameTables", inputQuery, "spark", expectedMapping).Return(outputQuery, nil)
	modifier := &DevEnvQueryModifier{
		Dialect: "spark",
		Conn:    connectionFetcher,
		Parser:  sqlParser,
	}
	ctx := context.WithValue(
		t.Context(),
		config.EnvironmentContextKey,
		&config.Environment{SchemaPrefix: "dev_"},
	)

	got, err := modifier.Modify(ctx, p, asset, &query.Query{Query: inputQuery})
	require.NoError(t, err)
	require.Equal(t, &query.Query{Query: outputQuery}, got)
	connection.AssertExpectations(t)
	connectionFetcher.AssertExpectations(t)
	sqlParser.AssertExpectations(t)
}
