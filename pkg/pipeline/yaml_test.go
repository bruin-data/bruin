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
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
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
				CustomChecks: make([]pipeline.CustomCheck, 0),
				Columns: []pipeline.Column{
					{
						Name:        "col1",
						Description: "column one",
						Type:        "string",
						Checks: []pipeline.ColumnCheck{
							{
								ID:   "47f67e812439f6d7b1a8a33a8995547e7c5dc5d9cf11f80191f5200b6d6cf030",
								Name: "unique",
							},
							{
								ID:   "6eb532e28e52eaebd4b0fc67a2fdc4e4e22b53f2b611422806160ff427fe2c45",
								Name: "not_null",
							},
							{
								ID:   "b0681ccb3ede10841af57f2dfd787e6f007dc1368a9e13cfebaafb86c4fc185c",
								Name: "accepted_values",
								Value: pipeline.ColumnCheckValue{
									StringArray: &[]string{"a", "b", "c"},
								},
							},
							{
								ID:   "b050553c53e0d093800e0e23d76a0870eb3388fd037908d0caa46a8adbd8b74f",
								Name: "min",
								Value: pipeline.ColumnCheckValue{
									Int: &[]int{3}[0],
								},
							},
						},
					},
					{
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
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
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
				Connection:   "conn1",
				Secrets:      []pipeline.SecretMapping{},
				DependsOn:    []string{"gcs-to-bq"},
				Columns:      make([]pipeline.Column, 0),
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "top-level runfile paths are still joined correctly",
			args: args{
				filePath: "testdata/yaml/task-with-toplevel-runfile/task.yml",
			},
			want: &pipeline.Asset{
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
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
				Connection:   "conn1",
				Secrets:      []pipeline.SecretMapping{},
				DependsOn:    []string{"gcs-to-bq"},
				Columns:      make([]pipeline.Column, 0),
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "the ones with missing runfile are ignored",
			args: args{
				filePath: "testdata/yaml/task-with-no-runfile/task.yml",
			},
			want: &pipeline.Asset{
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connection:   "conn1",
				Secrets:      []pipeline.SecretMapping{},
				DependsOn:    []string{"gcs-to-bq"},
				Columns:      make([]pipeline.Column, 0),
				CustomChecks: make([]pipeline.CustomCheck, 0),
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
