package pipeline_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustRead(t *testing.T, file string) string {
	content, err := afero.ReadFile(afero.NewOsFs(), file)
	require.NoError(t, err)
	return strings.TrimSpace(string(content))
}

func Test_createTaskFromFile(t *testing.T) {
	t.Parallel()

	type args struct {
		filePath string
	}

	intValueForCustomCheck := 16

	absPath := func(path string) string {
		absolutePath, _ := filepath.Abs(path)
		return absolutePath
	}

	tests := []struct {
		name    string
		args    args
		want    *pipeline.Asset
		wantErr bool
	}{
		{
			name: "file does not exist",
			args: args{
				filePath: "testdata/comments/some-file-that-doesnt-exist.sql",
			},
			wantErr: true,
		},
		{
			name: "existing file with no comments is skipped",
			args: args{
				filePath: "testdata/comments/nocomments.py",
			},
			wantErr: false,
		},
		{
			name: "SQL file parsed",
			args: args{
				filePath: "testdata/comments/test.sql",
			},
			want: &pipeline.Asset{
				ID:          "5812ba61bb0f08ce192bf074c9de21c19355e08cd52e75d008bbff59e5729e5b",
				Name:        "some-sql-task",
				Description: "some description goes here",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "test.sql",
					Path:    absPath("testdata/comments/test.sql"),
					Content: mustRead(t, "testdata/comments/test.sql"),
				},
				Parameters: map[string]string{
					"param1":       "first-parameter",
					"param2":       "second-parameter",
					"s3_file_path": "s3://bucket/path",
				},
				Connection: "conn2",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"task1", "task2", "task3", "task4", "task5", "task3"},
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					PartitionBy:    "dt",
					IncrementalKey: "dt",
					ClusterBy:      []string{"event_name"},
				},
				Columns:      []pipeline.Column{},
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "SQL file with embedded yaml content is parsed",
			args: args{
				filePath: "testdata/comments/embeddedyaml.sql",
			},
			want: &pipeline.Asset{
				ID:          "5812ba61bb0f08ce192bf074c9de21c19355e08cd52e75d008bbff59e5729e5b",
				Name:        "some-sql-task",
				Description: "some description goes here",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "embeddedyaml.sql",
					Path:    absPath("testdata/comments/embeddedyaml.sql"),
					Content: "select *\nfrom foo;",
				},
				Parameters: map[string]string{
					"param1":       "first-parameter",
					"param2":       "second-parameter",
					"s3_file_path": "s3://bucket/path",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"task1", "task2", "task3", "task4", "task5", "task3"},
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					PartitionBy:    "dt",
					IncrementalKey: "dt",
					ClusterBy:      []string{"event_name"},
				},
				Columns: make([]pipeline.Column, 0),
				CustomChecks: []pipeline.CustomCheck{
					{
						ID:    "480f365424205654f7108f2d0ddf6418faed97652bba106ba4080a967a50e5cf",
						Name:  "check1",
						Query: "select * from table1",
						Value: pipeline.ColumnCheckValue{
							Int: &intValueForCustomCheck,
						},
					},
				},
			},
		},
		{
			name: "Python file parsed",
			args: args{
				filePath: absPath("testdata/comments/test.py"), // giving an absolute path here tests the case of double-absolute paths
			},
			want: &pipeline.Asset{
				ID:          "21f2fa1b09d584a6b4fe30cd82b4540b769fd777da7c547353386e2930291ef9",
				Name:        "some-python-task",
				Description: "some description goes here",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "test.py",
					Path:    absPath("testdata/comments/test.py"),
					Content: mustRead(t, "testdata/comments/test.py"),
				},
				Parameters: map[string]string{
					"param1": "first-parameter",
					"param2": "second-parameter",
					"param3": "third-parameter",
				},
				Connection: "conn1",
				Image:      "python:3.11",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"task1", "task2", "task3", "task4", "task5", "task3"},
				Columns: []pipeline.Column{
					{
						Name: "col1",
						Type: "string",
						Checks: []pipeline.ColumnCheck{
							{
								Name: "not_null",
							},
							{
								Name: "positive",
							},
							{
								Name: "unique",
							},
						},
					},
					{
						Name: "col2", Checks: []pipeline.ColumnCheck{
							{
								Name: "not_null",
							},
							{
								Name: "unique",
							},
						},
					},
				},
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := pipeline.CreateTaskFromFileComments(afero.NewOsFs())(tt.args.filePath)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.want == nil {
				assert.Nil(t, got)
				return
			}

			assert.EqualExportedValues(t, *tt.want, *got)
		})
	}
}

func BenchmarkCreateTaskFromFileComments(b *testing.B) {
	b.ReportAllocs()

	file := "testdata/comments/test.py"

	for i := 0; i < b.N; i++ {
		_, err := pipeline.CreateTaskFromFileComments(afero.NewOsFs())(file)
		require.NoError(b, err)
	}
}
