package duck

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

func (m *mockMaterializer) LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error {
	return nil
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
				f.e.On("ExtractQueriesFromString", "some query").
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
				f.e.On("ExtractQueriesFromString", "some query").
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
			name: "multiple queries found but materialization is enabled, should fail",
			setup: func(f *fields) {
				f.e.On("ExtractQueriesFromString", "some query").
					Return([]*query.Query{
						{Query: "query 1"},
						{Query: "query 2"},
					}, nil)
			},
			args: args{
				t: &pipeline.Asset{
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
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
				f.e.On("ExtractQueriesFromString", "some query").
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
					Type: pipeline.AssetTypeDuckDBQuery,
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
				f.e.On("ExtractQueriesFromString", "some query").
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
					Type: pipeline.AssetTypeDuckDBQuery,
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
				f.e.On("ExtractQueriesFromString", "some query").
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
					Type: pipeline.AssetTypeDuckDBQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "some query",
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
			conn.On("GetConnection", mock.Anything).Return(client, nil)
			client.On("CreateSchemaIfNotExist", mock.Anything, mock.Anything).Return(nil)
			client.On("Close").Return()

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

func TestBasicOperator_RunTask_RendersIncrementalPredicate(t *testing.T) {
	t.Parallel()

	macros := "{% macro partition(alias='') -%}\n" +
		"{{ alias ~ '.' if alias }}region = '{{ var.region }}' AND {{ alias ~ '.' if alias }}period = {{ var.period }}\n" +
		"{%- endmacro %}"

	tests := []struct {
		name      string
		predicate string
	}{
		{
			name:      "variables",
			predicate: "target.region = '{{ var.region }}' AND target.period = {{ var.period }}",
		},
		{
			name:      "project macro",
			predicate: "{{ partition('target') }}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			renderer := jinja.NewRendererWithMacros(jinja.Context{
				"var": map[string]any{"region": "eu", "period": 1086},
			}, macros)
			client := new(mockQuerierWithResult)
			conn := new(mockConnectionFetcher)
			conn.On("GetConnection", mock.Anything).Return(client, nil)
			client.On("CreateSchemaIfNotExist", mock.Anything, mock.Anything).Return(nil)
			client.On("Close").Return()

			var ran []string
			client.On("RunQueryWithoutResult", mock.Anything, mock.Anything).
				Run(func(args mock.Arguments) { ran = append(ran, args.Get(1).(*query.Query).Query) }).
				Return(nil)

			o := BasicOperator{
				connection:   conn,
				extractor:    &query.WholeFileExtractor{Renderer: renderer},
				materializer: NewMaterializer(false),
			}

			asset := &pipeline.Asset{
				Name: "bronze.board_member",
				Type: pipeline.AssetTypeDuckDBQuery,
				ExecutableFile: pipeline.ExecutableFile{
					Content: "SELECT * FROM raw.board_member WHERE {{ partition() }}",
				},
				Materialization: pipeline.Materialization{
					Type:                 pipeline.MaterializationTypeTable,
					Strategy:             pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey:       "batch_id",
					IncrementalPredicate: tt.predicate,
				},
			}

			require.NoError(t, o.RunTask(t.Context(), &pipeline.Pipeline{}, asset))
			require.Len(t, ran, 1)
			assert.Contains(t, ran[0], "SELECT * FROM raw.board_member WHERE region = 'eu' AND period = 1086;")
			assert.Regexp(t,
				`DELETE FROM bronze\.board_member AS target WHERE batch_id in \(SELECT DISTINCT batch_id FROM __bruin_tmp_\w+\) AND \(target\.region = 'eu' AND target\.period = 1086\);`,
				ran[0])
			assert.Equal(t, tt.predicate, asset.Materialization.IncrementalPredicate, "the shared asset keeps its template")
		})
	}
}
