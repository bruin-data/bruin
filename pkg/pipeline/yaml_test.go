package pipeline_test

import (
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestCreateTaskFromYamlDefinition(t *testing.T) {
	t.Parallel()

	absPath := func(path string) string {
		absolutePath, _ := filepath.Abs(path)
		return absolutePath
	}

	type args struct {
		filePath string
	}

	tests := []struct {
		name    string
		args    args
		want    *pipeline.Asset
		wantErr bool
		err     error
	}{
		{
			name: "fails for paths that do not exist",
			args: args{
				filePath: "testdata/some/path/that/doesnt/exist",
			},
			wantErr: true,
		},
		{
			name: "fails for non-yaml files",
			args: args{
				filePath: "testdata/yaml/task1/hello.sql",
			},
			wantErr: true,
		},
		{
			name: "reads a valid simple file",
			args: args{
				filePath: "testdata/yaml/task1/task.yml",
			},
			want: &pipeline.Asset{
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sql",
					Path:    absPath("testdata/yaml/task1/hello.sql"),
					Content: mustRead(t, "testdata/yaml/task1/hello.sql"),
				},
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"gcs-to-bq"},
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyCreateReplace,
					ClusterBy:      []string{"key1", "key2"},
					PartitionBy:    "dt",
					IncrementalKey: "dt",
				},
				Columns: map[string]pipeline.Column{
					"col1": {
						Name:        "col1",
						Description: "column one",
						Checks: []pipeline.ColumnCheck{
							{
								Name: "unique",
							},
							{
								Name: "not_null",
							},
							{
								Name: "accepted_values",
								Value: pipeline.ColumnCheckValue{
									StringArray: &[]string{"a", "b", "c"},
								},
							},
							{
								Name: "min",
								Value: pipeline.ColumnCheckValue{
									Int: &[]int{3}[0],
								},
							},
							{
								Name: "pi",
								Value: pipeline.ColumnCheckValue{
									Float: &[]float64{3.14}[0],
								},
							},
							{
								Name: "intarrays",
								Value: pipeline.ColumnCheckValue{
									IntArray: &[]int{1, 2, 3},
								},
							},
						},
					},
					"col2": {
						Name:        "col2",
						Description: "column two",
						Checks:      []pipeline.ColumnCheck{},
					},
				},
			},
		},
		{
			name: "nested runfile paths work correctly",
			args: args{
				filePath: "testdata/yaml/task-with-nested/task.yml",
			},
			want: &pipeline.Asset{
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sh",
					Path:    absPath("testdata/yaml/task-with-nested/some/dir/hello.sh"),
					Content: mustRead(t, "testdata/yaml/task-with-nested/some/dir/hello.sh"),
				},
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"gcs-to-bq"},
				Columns:    map[string]pipeline.Column{},
			},
		},
		{
			name: "top-level runfile paths are still joined correctly",
			args: args{
				filePath: "testdata/yaml/task-with-toplevel-runfile/task.yml",
			},
			want: &pipeline.Asset{
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sh",
					Path:    absPath("testdata/yaml/task-with-toplevel-runfile/hello.sh"),
					Content: mustRead(t, "testdata/yaml/task-with-toplevel-runfile/hello.sh"),
				},
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"gcs-to-bq"},
				Schedule:   pipeline.TaskSchedule{Days: []string{"sunday", "monday", "tuesday"}},
				Columns:    map[string]pipeline.Column{},
			},
		},
		{
			name: "the ones with missing runfile are ignored",
			args: args{
				filePath: "testdata/yaml/task-with-no-runfile/task.yml",
			},
			want: &pipeline.Asset{
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"gcs-to-bq"},
				Columns:    map[string]pipeline.Column{},
			},
		},
		{
			name: "depends must be an array of strings",
			args: args{
				filePath: "testdata/yaml/random-structure/task.yml",
			},
			wantErr: true,
			err:     errors.New("`depends` field must be an array of strings"),
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			creator := pipeline.CreateTaskFromYamlDefinition(afero.NewOsFs())
			got, err := creator(tt.args.filePath)
			if tt.wantErr {
				require.Error(t, err)
				if tt.err != nil {
					require.EqualError(t, err, tt.err.Error())
				}
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}
