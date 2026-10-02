package mssql

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	mssqldb "github.com/microsoft/go-mssqldb"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestRunMaterializedQuery(t *testing.T) {
	t.Parallel()
	deadlock := mssqldb.Error{Number: 1205, Message: "deadlock victim"}
	duplicateColumn := mssqldb.Error{Number: 8156, Message: "column specified multiple times for source"}
	tests := []struct {
		name     string
		mat      pipeline.Materialization
		hooks    pipeline.Hooks
		failures int
		err      error
		wantErr  error
	}{
		{name: "default table retries", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable}, failures: 1, err: deadlock},
		{name: "create replace retries wrapped driver errors", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyCreateReplace}, failures: 1, err: errors.Wrap(deadlock, "execute")},
		{name: "append retries", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyAppend}, failures: 1, err: deadlock},
		{name: "merge retries", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyMerge}, failures: 1, err: deadlock},
		{name: "delete insert retries", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDeleteInsert}, failures: 1, err: deadlock},
		{name: "time interval retries", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTimeInterval}, failures: 1, err: deadlock},
		{name: "truncate insert retries", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyTruncateInsert}, failures: 1, err: deadlock},
		{name: "view retries", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeView}, failures: 1, err: deadlock},
		{name: "last retry succeeds", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable}, failures: 10, err: deadlock},
		{name: "retry limit", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable}, failures: 11, err: deadlock, wantErr: deadlock},
		{name: "permanent error", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable}, failures: 1, err: duplicateColumn, wantErr: duplicateColumn},
		{name: "script not replayed", failures: 1, err: deadlock, wantErr: deadlock},
		{name: "ddl not replayed", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable, Strategy: pipeline.MaterializationStrategyDDL}, failures: 1, err: deadlock, wantErr: deadlock},
		{name: "pre hook not replayed", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable}, hooks: pipeline.Hooks{Pre: []pipeline.Hook{{Query: "INSERT INTO audit VALUES (1)"}}}, failures: 1, err: deadlock, wantErr: deadlock},
		{name: "post hook not replayed", mat: pipeline.Materialization{Type: pipeline.MaterializationTypeTable}, hooks: pipeline.Hooks{Post: []pipeline.Hook{{Query: "INSERT INTO audit VALUES (1)"}}}, failures: 1, err: deadlock, wantErr: deadlock},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			conn := new(mockQuerierWithResult)
			q := &query.Query{Query: "materialized SQL", Args: []interface{}{42}}
			conn.On("RunQueryWithoutResult", mock.Anything, q).Return(tt.err).Times(tt.failures)
			if tt.wantErr == nil {
				conn.On("RunQueryWithoutResult", mock.Anything, q).Return(nil).Once()
			}
			synctest.Test(t, func(t *testing.T) {
				err := runMaterializedQuery(t.Context(), conn, &pipeline.Asset{Materialization: tt.mat, Hooks: tt.hooks}, q)
				if tt.wantErr == nil {
					require.NoError(t, err)
				} else {
					require.Equal(t, tt.wantErr, err)
				}
			})
			conn.AssertExpectations(t)
		})
	}
}

func TestRunMaterializedQueryCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	conn := new(mockQuerierWithResult)
	q := &query.Query{Query: "materialized SQL"}
	conn.On("RunQueryWithoutResult", mock.Anything, q).
		Run(func(mock.Arguments) { cancel() }).
		Return(mssqldb.Error{Number: 1205}).Once()
	asset := &pipeline.Asset{Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable}}
	require.ErrorIs(t, runMaterializedQuery(ctx, conn, asset, q), context.Canceled)
	conn.AssertExpectations(t)
}

type mockExtractor struct {
	mock.Mock
}

func (m *mockExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	res := m.Called(content)
	return res.Get(0).([]*query.Query), res.Error(1)
}

func (m *mockExtractor) CloneForAsset(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) (query.QueryExtractor, error) {
	return m, nil
}

func (m *mockExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	res := m.Called(content)
	return res.Get(0).([]string), res.Error(1)
}

type mockMaterializer struct {
	mock.Mock
}

func (m *mockMaterializer) Render(t *pipeline.Asset, query string) (string, error) {
	res := m.Called(t, query)
	return res.Get(0).(string), res.Error(1)
}

func TestBasicOperator_RunTask(t *testing.T) {
	t.Parallel()

	type args struct {
		t *pipeline.Asset
	}

	type fields struct {
		q *mockQuerierWithResult
		e *mockExtractor
		m *mockMaterializer
	}

	tests := []struct {
		name              string
		setup             func(f *fields)
		setupQueries      func(m *mockQuerierWithResult)
		setupExtractor    func(m *mockExtractor)
		setupMaterializer func(m *mockMaterializer)
		args              args
		wantErr           bool
	}{
		{
			name: "failed to extract queries",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{}, errors.New("failed to extract queries"))
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no queries found in file",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{}, nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "no queries found in DDL asset runs materialized metadata query",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "").
					Return([]*query.Query{}, nil)

				f.m.On("Render", mock.Anything, "").
					Return("CREATE TABLE cfg.ProfileTarget (TargetId integer)", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE cfg.ProfileTarget (TargetId integer)"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeMsSQLQuery,
					Materialization: pipeline.Materialization{
						Type:     pipeline.MaterializationTypeTable,
						Strategy: pipeline.MaterializationStrategyDDL,
					},
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "multiple queries found but materialization is enabled, should fail",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{
						{Query: "query 1"},
						{Query: "query 2"},
					}, nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
					Materialization: pipeline.Materialization{
						Type: pipeline.MaterializationTypeTable,
					},
				},
			},
			wantErr: true,
		},
		{
			name: "query returned an error",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)

				f.m.On("Render", mock.Anything, "select * from users").
					Return("select * from users", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(errors.New("failed to run query"))
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeMsSQLQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "query successfully executed",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)

				f.m.On("Render", mock.Anything, "select * from users").
					Return("select * from users", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeMsSQLQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "query successfully executed with materialization",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some content").
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)

				f.m.On("Render", mock.Anything, "select * from users").
					Return("CREATE TABLE x AS select * from users", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE x AS select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeMsSQLQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some content",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := new(mockQuerierWithResult)
			extractor := new(mockExtractor)
			mat := new(mockMaterializer)
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", "mssql-default").Return(client)

			if tt.setup != nil {
				tt.setup(&fields{
					q: client,
					e: extractor,
					m: mat,
				})
			}

			o := BasicOperator{
				connection:   conn,
				extractor:    extractor,
				materializer: mat,
			}

			err := o.RunTask(t.Context(), &pipeline.Pipeline{}, tt.args.t)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
