package bigquery

import (
	"context"
	"io"
	"testing"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockExtractor struct {
	mock.Mock
	renderer *jinja.Renderer
}

func (m *mockExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	// Check if the renderer is set
	if m.renderer != nil {
		// Use the renderer to render the content
		renderedQuery, err := m.renderer.Render(content)
		if err != nil {
			return nil, err
		}
		// Return the rendered query wrapped in a query.Query struct
		return []*query.Query{
			{Query: renderedQuery},
		}, nil
	}

	// Fallback to the mock behavior if renderer is not set
	res := m.Called(content)
	return res.Get(0).([]*query.Query), res.Error(1)
}

func (m *mockExtractor) CloneForAsset(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) query.QueryExtractor {
	return m
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

func (m *mockMaterializer) IsFullRefresh() bool {
	res := m.Called()
	return res.Bool(0)
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

				f.m.On("LogIfFullRefreshAndDDL", mock.Anything, mock.Anything).
					Return(nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(errors.New("failed to run query"))
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
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
				f.m.On("LogIfFullRefreshAndDDL", mock.Anything, mock.Anything).
					Return(nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "select * from users"}).
					Return(nil)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
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
				f.m.On("IsFullRefresh").Return(false)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
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
			mat.On("IsFullRefresh").Return(false)
			client.On("CreateDataSetIfNotExist", mock.AnythingOfType("*pipeline.Asset"), mock.Anything).Return(nil)
			conn := new(mockConnectionFetcher)
			conn.On("GetBqConnection", "gcp-default").Return(client, nil)
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

func TestMetadataPushOperator_Run(t *testing.T) {
	t.Parallel()

	type fields struct {
		q *mockQuerierWithResult
	}

	asset := &pipeline.Asset{
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "test-file.sql",
			Content: "some content",
		},
	}

	tests := []struct {
		name    string
		setup   func(f *fields)
		t       *pipeline.Asset
		wantErr bool
	}{
		{
			name: "no metadata to push",
			setup: func(f *fields) {
				f.q.On("UpdateTableMetadataIfNotExist", mock.Anything, asset).
					Return(NoMetadataUpdatedError{})
			},
			t:       asset,
			wantErr: false,
		},
		{
			name: "other errors are propagated",
			setup: func(f *fields) {
				f.q.On("UpdateTableMetadataIfNotExist", mock.Anything, asset).
					Return(errors.New("something failed"))
			},
			t:       asset,
			wantErr: true,
		},
		{
			name: "metadata is pushed successfully",
			setup: func(f *fields) {
				f.q.On("UpdateTableMetadataIfNotExist", mock.Anything, asset).
					Return(nil)
			},
			t:       asset,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := new(mockQuerierWithResult)
			conn := new(mockConnectionFetcher)
			conn.On("GetBqConnection", "gcp-default").Return(client, nil)

			if tt.setup != nil {
				tt.setup(&fields{
					q: client,
				})
			}

			o := MetadataPushOperator{
				connection: conn,
			}

			taskInstance := scheduler.AssetInstance{Asset: tt.t, Pipeline: &pipeline.Pipeline{}}

			ctx := context.WithValue(context.Background(), executor.KeyPrinter, io.Discard)

			err := o.Run(ctx, &taskInstance)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBasicOperator_RunTask_WithRenderer(t *testing.T) {
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
			name: "query successfully executed with rendering",
			setup: func(f *fields) {
				f.m.On("Render", mock.Anything, "SELECT 1").
					Return("CREATE TABLE x AS SELECT 1", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE x AS SELECT 1"}).
					Return(nil)
				f.m.On("IsFullRefresh").Return(false)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "{%- set sinks = [\n  'instant_cash',\n] -%}\n\nSELECT 1",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "query successfully executed with rendering 2",
			setup: func(f *fields) {
				f.m.On("Render", mock.Anything, "SELECT 1").
					Return("CREATE TABLE x AS SELECT 1", nil)

				f.q.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "CREATE TABLE x AS SELECT 1"}).
					Return(nil)
				f.m.On("IsFullRefresh").Return(false)
			},
			args: args{
				t: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path:    "test-file.sql",
						Content: "{%- set sinks = [\n  'instant_cash',\n] -%}\n\nSELECT 1",
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
			extractor.renderer = jinja.NewRenderer(jinja.Context{})
			mat := new(mockMaterializer)
			mat.On("IsFullRefresh").Return(false)
			client.On("CreateDataSetIfNotExist", mock.AnythingOfType("*pipeline.Asset"), mock.Anything).Return(nil)
			conn := new(mockConnectionFetcher)
			conn.On("GetBqConnection", "gcp-default").Return(client, nil)
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
