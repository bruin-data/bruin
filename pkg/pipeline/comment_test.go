package pipeline_test

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustRead(t *testing.T, file string) string {
	content, err := afero.ReadFile(afero.NewOsFs(), file)
	require.NoError(t, err)
	return strings.ReplaceAll(strings.TrimSpace(string(content)), "\r\n", "\n")
}

func mustReadWithoutReplacement(t *testing.T, file string) string {
	content, err := afero.ReadFile(afero.NewOsFs(), file)
	require.NoError(t, err)
	return strings.TrimSpace(string(content))
}

func Test_createTaskFromFile(t *testing.T) {
	t.Parallel()

	falseValue := false
	trueValue := true

	type args struct {
		filePath string
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
					Path:    path.AbsPathForTests(t, "testdata/comments/test.sql"),
					Content: "select *\nfrom foo;",
				},
				Parameters: map[string]string{
					"param1":       "first-parameter",
					"param2":       "second-parameter",
					"s3_file_path": "s3://bucket/path",
				},
				Connection: "conn2",
				Secrets:    []pipeline.SecretMapping{},
				Upstreams: []pipeline.Upstream{
					{Value: "task1", Type: "asset"},
					{Value: "task2", Type: "asset"},
					{Value: "task3", Type: "asset"},
					{Value: "task4", Type: "asset"},
					{Value: "task5", Type: "asset"},
					{Value: "task3", Type: "asset"},
				},

				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					PartitionBy:    "dt",
					IncrementalKey: "dt",
					ClusterBy:      []string{"event_name"},
				},
				Columns: []pipeline.Column{
					{Name: "some_column", PrimaryKey: true, Checks: make([]pipeline.ColumnCheck, 0)},
					{Name: "some_other_column", PrimaryKey: false, Checks: make([]pipeline.ColumnCheck, 0)},
				},
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "SQL file failed to parse",
			args: args{
				filePath: "testdata/comments/failparseprimarykey.sql",
			},
			wantErr: true,
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
					Path:    path.AbsPathForTests(t, "testdata/comments/embeddedyaml.sql"),
					Content: "select *\nfrom foo;",
				},
				Parameters: map[string]string{
					"param1":       "first-parameter",
					"param2":       "second-parameter",
					"s3_file_path": "s3://bucket/path",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				Upstreams: []pipeline.Upstream{
					{Value: "task1", Type: "asset"},
					{Value: "task2", Type: "asset"},
					{Value: "task3", Type: "asset"},
					{Value: "task4", Type: "asset"},
					{Value: "task5", Type: "asset"},
					{Value: "task3", Type: "asset"},
				},
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
						Value: 16,
					},
				},
			},
		},
		{
			name: "Python file parsed",
			args: args{
				filePath: path.AbsPathForTests(t, "testdata/comments/test.py"), // giving an absolute path here tests the case of double-absolute paths
			},
			want: &pipeline.Asset{
				ID:          "21f2fa1b09d584a6b4fe30cd82b4540b769fd777da7c547353386e2930291ef9",
				Name:        "some-python-task",
				Description: "some description goes here",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "test.py",
					Path:    path.AbsPathForTests(t, "testdata/comments/test.py"),
					Content: "print('hello world')",
				},
				Parameters: map[string]string{
					"param1": "first-parameter",
					"param2": "second-parameter",
					"param3": "third-parameter",
				},
				Connection: "conn1",
				Image:      "python:3.11",
				Instance:   "b1.nano",
				Secrets:    []pipeline.SecretMapping{},
				Upstreams: []pipeline.Upstream{
					{Value: "task1", Type: "asset"},
					{Value: "task2", Type: "asset"},
					{Value: "task3", Type: "asset"},
					{Value: "task4", Type: "asset"},
					{Value: "task5", Type: "asset"},
					{Value: "task3", Type: "asset"},
				},
				Columns: []pipeline.Column{
					{
						Name: "col1",
						Type: "string",
						Checks: []pipeline.ColumnCheck{
							{
								ID:       "08745666ad3e043ceb0321ed502e9a2d20248d62b2ee7dd1c600fc5c944af238",
								Name:     "not_null",
								Blocking: pipeline.DefaultTrueBool{Value: &trueValue},
							},
							{
								ID:       "29f700e6438c361ab038fcb611a71dab5a6949f3942b75c52402dce7a17cf698",
								Name:     "positive",
								Blocking: pipeline.DefaultTrueBool{Value: &trueValue},
							},
							{
								ID:       "6660a3e1f845f9046ff2cda9ef8ae9357c4008c43724ebaf834186e5c2bd7a35",
								Name:     "unique",
								Blocking: pipeline.DefaultTrueBool{Value: &trueValue},
							},
						},
					},
					{
						Name: "col2", Checks: []pipeline.ColumnCheck{
							{
								ID:       "7870f9ce39b0d29451a41e2d8240c02713ce80647db886fe5e5cc69227dd86d3",
								Name:     "not_null",
								Blocking: pipeline.DefaultTrueBool{Value: &trueValue},
							},
							{
								ID:       "68e80e2b513c908c9c1d3aac2f96bd535f43f2c62a78c6744dee8ae767e60e5d",
								Name:     "unique",
								Blocking: pipeline.DefaultTrueBool{Value: &trueValue},
							},
						},
					},
				},
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "Python file with comment block parsed",
			args: args{
				filePath: path.AbsPathForTests(t, "testdata/comments/testblockcomments.py"),
			},
			want: &pipeline.Asset{
				ID:          "21f2fa1b09d584a6b4fe30cd82b4540b769fd777da7c547353386e2930291ef9",
				Name:        "some-python-task",
				Description: "some description goes here",
				Type:        "python",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "testblockcomments.py",
					Path:    path.AbsPathForTests(t, "testdata/comments/testblockcomments.py"),
					Content: "print('hello world')",
				},
				Parameters: map[string]string{
					"param1": "first-parameter",
					"param2": "second-parameter",
					"param3": "third-parameter",
				},
				Image:    "python:3.11",
				Instance: "b1.nano",
				Secrets: []pipeline.SecretMapping{
					{
						SecretKey:   "secret1",
						InjectedKey: "INJECTED_SECRET1",
					},
					{
						SecretKey:   "secret2",
						InjectedKey: "secret2",
					},
				},
				Upstreams: []pipeline.Upstream{
					{Value: "task1", Type: "asset"},
					{Value: "task2", Type: "asset"},
					{Value: "task3", Type: "asset"},
					{Value: "task4", Type: "asset"},
					{Value: "task5", Type: "asset"},
				},
				Columns: []pipeline.Column{
					{
						Name: "col1",
						Type: "string",
						Checks: []pipeline.ColumnCheck{
							{
								ID:   "08745666ad3e043ceb0321ed502e9a2d20248d62b2ee7dd1c600fc5c944af238",
								Name: "not_null",
							},
							{
								ID:   "29f700e6438c361ab038fcb611a71dab5a6949f3942b75c52402dce7a17cf698",
								Name: "positive",
							},
							{
								ID:   "6660a3e1f845f9046ff2cda9ef8ae9357c4008c43724ebaf834186e5c2bd7a35",
								Name: "unique",
							},
						},
					},
					{
						Name: "col2",
						Type: "string",
						Checks: []pipeline.ColumnCheck{
							{
								ID:   "7870f9ce39b0d29451a41e2d8240c02713ce80647db886fe5e5cc69227dd86d3",
								Name: "not_null",
							},
							{
								ID:       "68e80e2b513c908c9c1d3aac2f96bd535f43f2c62a78c6744dee8ae767e60e5d",
								Name:     "unique",
								Blocking: pipeline.DefaultTrueBool{Value: &falseValue},
							},
						},
					},
				},
				CustomChecks: []pipeline.CustomCheck{
					{
						ID:       "a26c19e73c6b5cdee1b1bfe135a475979f360b9e7fdfc19a7fca1832d034adbc",
						Name:     "check1",
						Query:    "select 5",
						Value:    16,
						Blocking: pipeline.DefaultTrueBool{Value: &falseValue},
					},
					{
						ID:    "cc1040bd694dcb5fae26300d2c0f721e4c4304cb16b9dce3b4ddcaa44498b940",
						Name:  "check2",
						Query: "select 5",
						Value: 16,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := pipeline.CreateTaskFromFileComments(afero.NewOsFs())(tt.args.filePath)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
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
