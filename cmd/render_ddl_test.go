package cmd

import (
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRenderDDLCommand_Run(t *testing.T) {
	t.Parallel()

	bqAsset := &pipeline.Asset{
		Name: "asset1",
		Type: pipeline.AssetTypeBigqueryQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path: "/path/to/executable",
		},
	}

	snowflakeAsset := &pipeline.Asset{
		Name: "snowflake-asset",
		Type: pipeline.AssetTypeSnowflakeQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path: "/path/to/executable2",
		},
	}

	mssqlAsset := &pipeline.Asset{
		Name: "mssql-asset",
		Type: pipeline.AssetTypeMsSQLQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path: "/path/to/executable3",
		},
	}

	type fields struct {
		extractor      *mockExtractor
		bqMaterializer *mockMaterializer
		sfMaterializer *mockMaterializer
		builder        *mockBuilder
		writer         *mockWriter
	}
	type args struct {
		task *pipeline.Asset
	}
	tests := []struct {
		name    string
		setup   func(*fields)
		args    args
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "should return error if task path is empty",
			args: args{
				task: nil,
			},
			wantErr: assert.Error,
		},
		{
			name: "should return error if failed to extract queries from file",
			args: args{
				task: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path: "/path/to/executable",
					},
					Name: "asset1",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromString", bqAsset.ExecutableFile.Content).
					Return(nil, assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should return error if DDL materialization fails",
			args: args{
				task: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path: "/path/to/executable",
					},
					Name: "asset1",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromString", bqAsset.ExecutableFile.Content).
					Return([]*query.Query{{Query: "some query"}}, nil)

				f.bqMaterializer.On("Render", mock.Anything, "some query").
					Return("", assert.AnError)
			},
			wantErr: assert.Error,
		},
		{
			name: "should render DDL for BigQuery asset",
			args: args{
				task: &pipeline.Asset{
					Type: pipeline.AssetTypeBigqueryQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path: "/path/to/executable",
					},
					Name: "asset1",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromString", bqAsset.ExecutableFile.Content).
					Return([]*query.Query{{Query: "extracted query"}}, nil)
				f.bqMaterializer.On("Render", mock.Anything, "extracted query").
					Return("CREATE TABLE asset1 AS (extracted query)", nil)

				f.writer.On("Write", []byte("CREATE TABLE asset1 AS (extracted query)\n")).
					Return(0, nil)
			},
			wantErr: assert.NoError,
		},
		{
			name: "should render DDL for Snowflake asset",
			args: args{
				task: &pipeline.Asset{
					Type: pipeline.AssetTypeSnowflakeQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path: "/path/to/executable2",
					},
					Name: "snowflake-asset",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromString", snowflakeAsset.ExecutableFile.Content).
					Return([]*query.Query{{Query: "SELECT * FROM sf_table"}}, nil)
				f.sfMaterializer.On("Render", mock.Anything, "SELECT * FROM sf_table").
					Return("CREATE TABLE snowflake-asset AS (SELECT * FROM sf_table)", nil)

				f.writer.On("Write", []byte("CREATE TABLE snowflake-asset AS (SELECT * FROM sf_table)\n")).
					Return(0, nil)
			},
			wantErr: assert.NoError,
		},
		{
			name: "should return raw SQL for unsupported platform (MSSQL)",
			args: args{
				task: &pipeline.Asset{
					Type: pipeline.AssetTypeMsSQLQuery,
					ExecutableFile: pipeline.ExecutableFile{
						Path: "/path/to/executable3",
					},
					Name: "mssql-asset",
				},
			},
			setup: func(f *fields) {
				f.extractor.On("ExtractQueriesFromString", mssqlAsset.ExecutableFile.Content).
					Return([]*query.Query{{Query: "SELECT * FROM mssql_table"}}, nil)

				f.writer.On("Write", []byte("SELECT * FROM mssql_table\n")).
					Return(0, nil)
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := &fields{
				extractor:      new(mockExtractor),
				bqMaterializer: new(mockMaterializer),
				sfMaterializer: new(mockMaterializer),
				builder:        new(mockBuilder),
				writer:         new(mockWriter),
			}

			if tt.setup != nil {
				tt.setup(f)
			}

			render := &RenderCommand{
				extractor: f.extractor,
				materializers: map[pipeline.AssetType]queryMaterializer{
					pipeline.AssetTypeBigqueryQuery:  f.bqMaterializer,
					pipeline.AssetTypeSnowflakeQuery: f.sfMaterializer,
				},
				builder: f.builder,
				writer:  f.writer,
			}

			// Create an instance of ModifierInfo
			params := ModifierInfo{
				StartDate:      time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				EndDate:        time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
				ApplyModifiers: false,
			}

			tt.wantErr(t, render.Run(&pipeline.Pipeline{}, tt.args.task, params))
			f.extractor.AssertExpectations(t)
			f.bqMaterializer.AssertExpectations(t)
			f.sfMaterializer.AssertExpectations(t)
			f.builder.AssertExpectations(t)
			f.writer.AssertExpectations(t)
		})
	}
}