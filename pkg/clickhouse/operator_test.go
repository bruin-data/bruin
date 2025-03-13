package clickhouse

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockExtractor struct {
	mock.Mock
}

func (m *mockExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	res := m.Called(content)
	return res.Get(0).([]*query.Query), res.Error(1)
}

func (m *mockExtractor) ExtractQueriesFromSlice(content []string) ([]*query.Query, error) {
	res := m.Called(content)
	return res.Get(0).([]*query.Query), res.Error(1)
}

type mockMaterializer struct {
	mock.Mock
}

func (m *mockMaterializer) Render(t *pipeline.Asset, query string) ([]string, error) {
	res := m.Called(t, query)
	return res.Get(0).([]string), res.Error(1)
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
				f.m.On("Render", mock.AnythingOfType("*pipeline.Asset"), "some query").
					Return([]string{"some query"}, nil)
				f.e.On("ExtractQueriesFromSlice", []string{"some query"}).
					Return([]*query.Query{}, errors.New("failed to extract queries"))
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "no queries found in file",
			setup: func(f *fields) {
				f.m.On("Render", mock.AnythingOfType("*pipeline.Asset"), "some query").
					Return([]string{"some query"}, nil)
				f.e.On("ExtractQueriesFromSlice", []string{"some query"}).
					Return([]*query.Query{}, nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
					},
				},
			},
			wantErr: false,
		},

		{
			name: "query returned an error",
			setup: func(f *fields) {
				f.m.On("Render", mock.AnythingOfType("*pipeline.Asset"), "some query").
					Return([]string{"some query"}, nil)
				f.e.On("ExtractQueriesFromSlice", mock.Anything).
					Return([]*query.Query{
						{Query: "select * from users"},
					}, nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(errors.New("failed to run query"))
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypePostgresQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
					},
				},
			},
			wantErr: true,
		},
		{
			name: "query successfully executed",
			setup: func(f *fields) {
				f.m.On("Render", mock.AnythingOfType("*pipeline.Asset"), "some query").
					Return([]string{"some query"}, nil)

				f.e.On("ExtractQueriesFromSlice", mock.MatchedBy(func(s []string) bool {
					return len(s) == 1 && s[0] == "some query"
				})).Return([]*query.Query{
					{Query: "select * from users"},
				}, nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypePostgresQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "query successfully executed with materialization",
			setup: func(f *fields) {
				f.m.On("Render", mock.AnythingOfType("*pipeline.Asset"), "select * from users").
					Return([]string{"CREATE TABLE x AS select * from users"}, nil)

				f.e.On("ExtractQueriesFromSlice", mock.MatchedBy(func(s []string) bool {
					return len(s) == 1 && s[0] == "CREATE TABLE x AS select * from users"
				})).Return([]*query.Query{
					{Query: "CREATE TABLE x AS select * from users"},
				}, nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE x AS select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypePostgresQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "select * from users",
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
			conn.On("GetClickHouseConnection", mock.Anything).Return(client, nil)

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

			err := o.RunTask(context.Background(), &pipeline.Pipeline{}, tt.args.t)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
