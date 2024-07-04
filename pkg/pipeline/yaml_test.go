package pipeline_test

import (
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestCreateTaskFromYamlDefinition(t *testing.T) {
	t.Parallel()

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
				filePath: filepath.Join("testdata", "yaml", "task1", "hello.sql"),
			},
			wantErr: true,
		},
		{
			name: "reads a valid simple file",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "task1", "task.yml"),
			},
			want: &pipeline.Asset{
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sql",
					Path:    path.AbsPathForTests(t, filepath.Join("testdata", "yaml", "task1", "hello.sql")),
					Content: mustRead(t, filepath.Join("testdata", "yaml", "task1", "hello.sql")),
				},
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"gcs-to-bq"},
				Upstreams: []pipeline.Upstream{
					{
						Type:  "asset",
						Value: "gcs-to-bq",
					},
				},
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
								ID:       "1cde48c4bee4ad881c5d315dbfd136c708bfe4522ca7b74997017302b38ba763",
								Name:     "unique",
								Blocking: true,
							},
							{
								ID:       "7838b56dee5c090c3c04c356c8c1c249be0830efb1df36eb427b91eeb905875a",
								Name:     "not_null",
								Blocking: true,
							},
							{
								ID:   "8656942c1be105de34e9c1f500ad64b34f2c0ecf31e762ac6319e6b8cf9bbdcd",
								Name: "accepted_values",
								Value: pipeline.ColumnCheckValue{
									StringArray: &[]string{"a", "b", "c"},
								},
								Blocking: true,
							},
							{
								ID:   "ea4d2fa734a840d95f0d1a65cfc451d8f8e9d121ea972c5d2c6bf365a6a24b74",
								Name: "min",
								Value: pipeline.ColumnCheckValue{
									Int: &[]int{3}[0],
								},
								Blocking: true,
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
				filePath: filepath.Join("testdata", "yaml", "task-with-nested", "task.yml"),
			},
			want: &pipeline.Asset{
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sh",
					Path:    path.AbsPathForTests(t, filepath.Join("testdata", "yaml", "task-with-nested", "some", "dir", "hello.sh")),
					Content: mustRead(t, filepath.Join("testdata", "yaml", "task-with-nested", "some", "dir", "hello.sh")),
				},
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"gcs-to-bq"},
				Upstreams: []pipeline.Upstream{
					{
						Type:  "asset",
						Value: "gcs-to-bq",
					},
				},
				Columns:      make([]pipeline.Column, 0),
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "top-level runfile paths are still joined correctly",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "task-with-toplevel-runfile", "task.yml"),
			},
			want: &pipeline.Asset{
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sh",
					Path:    path.AbsPathForTests(t, filepath.Join("testdata", "yaml", "task-with-toplevel-runfile", "hello.sh")),
					Content: mustRead(t, filepath.Join("testdata", "yaml", "task-with-toplevel-runfile", "hello.sh")),
				},
				Parameters: map[string]string{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"gcs-to-bq"},
				Upstreams: []pipeline.Upstream{
					{
						Type:  "asset",
						Value: "gcs-to-bq",
					},
				},
				Columns:      make([]pipeline.Column, 0),
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "the ones with missing runfile are ignored",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "task-with-no-runfile", "task.yml"),
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
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				DependsOn:  []string{"gcs-to-bq"},
				Upstreams: []pipeline.Upstream{
					{
						Type:  "asset",
						Value: "gcs-to-bq",
					},
				},
				Columns:      make([]pipeline.Column, 0),
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "depends must be an array of strings",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "random-structure", "task.yml"),
			},
			wantErr: true,
			err:     errors.New("`depends` field must be an array of strings or mappings with `value` and `type` keys"),
		},
	}
	for _, tt := range tests {
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

func TestUpstreams(t *testing.T) {
	t.Parallel()
	creator := pipeline.CreateTaskFromYamlDefinition(afero.NewOsFs())
	got, err := creator(filepath.Join("testdata", "yaml", "upstream.yml"))
	require.NoError(t, err)

	expected := &pipeline.Asset{
		ID:      "5e51ec24663355d3b76b287f2c5eca1bfa17ac01da6134dbd1251c3b6ee99b56",
		Name:    "upstream.something",
		Secrets: make([]pipeline.SecretMapping, 0),
		DependsOn: []string{
			"some_asset",
			"bigquery://project.database/schema",
			"some_other_asset",
		},
		Columns:      make([]pipeline.Column, 0),
		CustomChecks: make([]pipeline.CustomCheck, 0),
		Upstreams: []pipeline.Upstream{
			{
				Type:  "asset",
				Value: "some_asset",
			},
			{
				Type:  "uri",
				Value: "bigquery://project.database/schema",
			},
			{
				Type:  "asset",
				Value: "some_other_asset",
			},
		},
	}

	require.Equal(t, expected, got)
}
