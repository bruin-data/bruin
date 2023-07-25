package pipeline_test

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
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
				Columns: map[string]pipeline.Column{},
			},
		},
		{
			name: "SQL file with embedded yaml content is parsed",
			args: args{
				filePath: "testdata/comments/embeddedyaml.sql",
			},
			want: &pipeline.Asset{
				Name:        "some-sql-task",
				Description: "some description goes here",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "embeddedyaml.sql",
					Path:    absPath("testdata/comments/embeddedyaml.sql"),
					Content: mustRead(t, "testdata/comments/embeddedyaml.sql"),
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
				Columns: map[string]pipeline.Column{},
			},
		},
		{
			name: "Python file parsed",
			args: args{
				filePath: absPath("testdata/comments/test.py"), // giving an absolute path here tests the case of double-absolute paths
			},
			want: &pipeline.Asset{
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
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"task1", "task2", "task3", "task4", "task5", "task3"},
				Schedule:   pipeline.TaskSchedule{Days: []string{"SUNDAY", "MONDAY", "TUESDAY"}},
				Columns:    map[string]pipeline.Column{},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := pipeline.CreateTaskFromFileComments(afero.NewOsFs())(tt.args.filePath)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
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
