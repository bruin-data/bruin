package lint

import (
	"os"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var noIssues = make([]*Issue, 0)

func TestEnsureTaskNameIsNotEmpty(t *testing.T) {
	t.Parallel()

	taskWithEmptyName := pipeline.Asset{
		Name: "",
	}

	type args struct {
		pipeline *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "all tasks have names, no error",
			args: args{
				pipeline: &pipeline.Pipeline{
					Name: "test",
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
						},
						{
							Name: "task2",
						},
					},
				},
			},
			want:    make([]*Issue, 0),
			wantErr: false,
		},
		{
			name: "tasks with missing name are reported",
			args: args{
				pipeline: &pipeline.Pipeline{
					Name: "test",
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
						},
						&taskWithEmptyName,
						{
							Name: "some-other-task",
						},
						{
							Name: "task name with spaces",
						},
					},
				},
			},
			want: []*Issue{
				{
					Task:        &taskWithEmptyName,
					Description: taskNameMustExist,
				},
				{
					Task: &pipeline.Asset{
						Name: "task name with spaces",
					},
					Description: taskNameMustBeAlphanumeric,
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsureTaskNameIsValid(tt.args.pipeline)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestEnsureExecutableFileIsValid(t *testing.T) {
	t.Parallel()

	type args struct {
		setupFilesystem func(t *testing.T, fs afero.Fs)
		pipeline        pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "comment task is skipped",
			args: args{
				pipeline: pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.CommentTask,
							},
						},
					},
				},
			},
			want: noIssues,
		},
		{
			name: "task with no executable is skipped",
			args: args{
				pipeline: pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.YamlTask,
							},
						},
					},
				},
			},
			want: noIssues,
		},
		{
			name: "task with no executable is reported for python files",
			args: args{
				pipeline: pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Type: pipeline.AssetTypePython,
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.YamlTask,
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Type: pipeline.AssetTypePython,
						DefinitionFile: pipeline.TaskDefinitionFile{
							Type: pipeline.YamlTask,
						},
					},
					Description: executableFileCannotBeEmpty,
				},
			},
		},
		{
			name: "task with no executable is skipped",
			args: args{
				pipeline: pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.YamlTask,
							},
							ExecutableFile: pipeline.ExecutableFile{
								Name: "some-file.sh",
								Path: "some-path.sh",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						DefinitionFile: pipeline.TaskDefinitionFile{
							Type: pipeline.YamlTask,
						},
						ExecutableFile: pipeline.ExecutableFile{
							Name: "some-file.sh",
							Path: "some-path.sh",
						},
					},
					Description: executableFileDoesNotExist,
				},
			},
		},
		{
			name: "executable is a directory",
			args: args{
				setupFilesystem: func(t *testing.T, fs afero.Fs) {
					err := fs.MkdirAll("some-path/some-file", 0o644)
					require.NoError(t, err, "failed to create the in-memory directory")
				},
				pipeline: pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.YamlTask,
							},
							ExecutableFile: pipeline.ExecutableFile{
								Name: "some-file",
								Path: "some-path/some-file",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						DefinitionFile: pipeline.TaskDefinitionFile{
							Type: pipeline.YamlTask,
						},
						ExecutableFile: pipeline.ExecutableFile{
							Name: "some-file",
							Path: "some-path/some-file",
						},
					},
					Description: executableFileIsADirectory,
				},
			},
		},
		{
			name: "executable is an empty file",
			args: args{
				setupFilesystem: func(t *testing.T, fs afero.Fs) {
					fileName := "some-path/some-file.sh"
					file, err := fs.Create(fileName)
					require.NoError(t, err)
					err = fs.Chmod(fileName, 0o755)
					require.NoError(t, err)
					require.NoError(t, file.Close())
				},
				pipeline: pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.YamlTask,
							},
							ExecutableFile: pipeline.ExecutableFile{
								Name: "some-file.sh",
								Path: "some-path/some-file.sh",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						DefinitionFile: pipeline.TaskDefinitionFile{
							Type: pipeline.YamlTask,
						},
						ExecutableFile: pipeline.ExecutableFile{
							Name: "some-file.sh",
							Path: "some-path/some-file.sh",
						},
					},
					Description: executableFileIsEmpty,
				},
				{
					Task: &pipeline.Asset{
						DefinitionFile: pipeline.TaskDefinitionFile{
							Type: pipeline.YamlTask,
						},
						ExecutableFile: pipeline.ExecutableFile{
							Name: "some-file.sh",
							Path: "some-path/some-file.sh",
						},
					},
					Description: executableFileIsNotExecutable,
				},
			},
		},
		{
			name: "executable file has the wrong permissions",
			args: args{
				setupFilesystem: func(t *testing.T, fs afero.Fs) {
					fileName := "some-path/some-file.sh"
					file, err := fs.Create(fileName)
					require.NoError(t, err)
					err = fs.Chmod(fileName, os.FileMode(0o100))
					require.NoError(t, err)
					defer func() { require.NoError(t, file.Close()) }()

					_, err = file.Write([]byte("some content"))
					require.NoError(t, err)
				},
				pipeline: pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.YamlTask,
							},
							ExecutableFile: pipeline.ExecutableFile{
								Name: "some-file.sh",
								Path: "some-path/some-file.sh",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						DefinitionFile: pipeline.TaskDefinitionFile{
							Type: pipeline.YamlTask,
						},
						ExecutableFile: pipeline.ExecutableFile{
							Name: "some-file.sh",
							Path: "some-path/some-file.sh",
						},
					},
					Description: executableFileIsNotExecutable,
				},
			},
		},
		{
			name: "all good for the executable, no issues found",
			args: args{
				setupFilesystem: func(t *testing.T, fs afero.Fs) {
					file, err := fs.Create("some-path/some-file.sh")
					require.NoError(t, err)
					defer func() { require.NoError(t, file.Close()) }()

					err = fs.Chmod("some-path/some-file.sh", 0o644)
					require.NoError(t, err)

					_, err = file.Write([]byte("some content"))
					require.NoError(t, err)

					file, err = fs.Create("some-path/some-other-file.sh")
					require.NoError(t, err)
					defer func() { require.NoError(t, file.Close()) }()

					err = fs.Chmod("some-path/some-other-file.sh", 0o644)
					require.NoError(t, err)

					_, err = file.Write([]byte("some other content"))
					require.NoError(t, err)
				},
				pipeline: pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.YamlTask,
							},
							ExecutableFile: pipeline.ExecutableFile{
								Name: "some-file.sh",
								Path: "some-path/some-file.sh",
							},
						},
						{
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.YamlTask,
							},
							ExecutableFile: pipeline.ExecutableFile{
								Name: "some-other-file.sh",
								Path: "some-path/some-other-file.sh",
							},
						},
					},
				},
			},
			want: noIssues,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewMemMapFs()
			if tt.args.setupFilesystem != nil {
				tt.args.setupFilesystem(t, fs)
			}

			checker := EnsureExecutableFileIsValid(fs)

			got, err := checker(&tt.args.pipeline)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEnsureDependencyExists(t *testing.T) {
	t.Parallel()

	type args struct {
		p *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "empty pipeline works fine",
			args: args{
				p: &pipeline.Pipeline{},
			},
			want: noIssues,
		},
		{
			name: "pipeline with no dependency has no issues",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
						},
						{
							Name: "task2",
						},
						{
							Name: "task3",
						},
					},
				},
			},
			want: noIssues,
		},
		{
			name: "dependency on a non-existing task is caught",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name:      "task1",
							DependsOn: []string{},
						},
						{
							Name:      "task2",
							DependsOn: []string{"task1", "task3", "task5"},
						},
						{
							Name:      "task3",
							DependsOn: []string{"task1", "task4"},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Name:      "task2",
						DependsOn: []string{"task1", "task3", "task5"},
					},
					Description: "Dependency 'task5' does not exist",
				},
				{
					Task: &pipeline.Asset{
						Name:      "task3",
						DependsOn: []string{"task1", "task4"},
					},
					Description: "Dependency 'task4' does not exist",
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsureDependencyExists(tt.args.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestEnsurePipelineScheduleIsValidCron(t *testing.T) {
	t.Parallel()

	type args struct {
		p *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "empty schedule is skipped",
			args: args{
				p: &pipeline.Pipeline{
					Schedule: "",
				},
			},
			want: noIssues,
		},
		{
			name: "invalid schedule is reported",
			args: args{
				p: &pipeline.Pipeline{
					Schedule: "some random schedule",
				},
			},
			want: []*Issue{
				{
					Description: "Invalid cron schedule 'some random schedule'",
				},
			},
		},
		{
			name: "valid schedule passes the check",
			args: args{
				p: &pipeline.Pipeline{
					Schedule: "* * * 1 *",
				},
			},
			want: noIssues,
		},
		{
			name: "valid descriptor passes the check",
			args: args{
				p: &pipeline.Pipeline{
					Schedule: "@daily",
				},
			},
			want: noIssues,
		},
		{
			name: "valid descriptor passes the check even if it doesnt have the @ prefix",
			args: args{
				p: &pipeline.Pipeline{
					Schedule: "daily",
				},
			},
			want: noIssues,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsurePipelineScheduleIsValidCron(tt.args.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestEnsureOnlyAcceptedTaskTypesAreThere(t *testing.T) {
	t.Parallel()

	type args struct {
		p *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "task with empty type is flagged",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Type: "",
						},
					},
				},
			},
			want: []*Issue{
				{
					Task:        &pipeline.Asset{},
					Description: taskTypeMustExist,
				},
			},
		},
		{
			name: "task invalid type is flagged",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Type: "some.random.type",
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Type: "some.random.type",
					},
					Description: "Invalid task type 'some.random.type'",
				},
			},
		},
		{
			name: "task with valid type is not flagged",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Type: "bq.sql",
						},
					},
				},
			},
			want: noIssues,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsureOnlyAcceptedTaskTypesAreThere(tt.args.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestEnsureTaskNameIsUnique(t *testing.T) {
	t.Parallel()
	type args struct {
		p *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "empty name is skipped",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "",
						},
					},
				},
			},
			want:    noIssues,
			wantErr: false,
		},
		{
			name: "duplicates are reported",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "name1",
							DefinitionFile: pipeline.TaskDefinitionFile{
								Path: "path1",
							},
						},
						{
							Name: "name2",
							DefinitionFile: pipeline.TaskDefinitionFile{
								Path: "path2",
							},
						},
						{
							Name: "name1",
							DefinitionFile: pipeline.TaskDefinitionFile{
								Path: "path3",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Name: "name1",
						DefinitionFile: pipeline.TaskDefinitionFile{
							Path: "path1",
						},
					},
					Description: "Asset name 'name1' is not unique, please make sure all the task names are unique",
					Context:     []string{"path1", "path3"},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsureTaskNameIsUnique(tt.args.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestEnsurePipelineNameIsValid(t *testing.T) {
	t.Parallel()
	type args struct {
		p *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "empty pipeline name is reported",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "",
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineNameCannotBeEmpty,
					Context:     nil,
				},
			},
			wantErr: false,
		},
		{
			name: "spaces are not accepted",
			args: args{
				p: &pipeline.Pipeline{
					Name: "some test pipeline",
				},
			},
			want: []*Issue{
				{
					Description: pipelineNameMustBeAlphanumeric,
					Context:     nil,
				},
			},
			wantErr: false,
		},
		{
			name: "valid pipeline name passes",
			args: args{
				p: &pipeline.Pipeline{
					Name: "test",
				},
			},
			want:    []*Issue{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsurePipelineNameIsValid(tt.args.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestEnsurePipelineHasNoCycles(t *testing.T) {
	t.Parallel()
	type args struct {
		p *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "cycles are detected",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "task0",
						},
						{
							Name:      "task1",
							DependsOn: []string{"task2", "task0"},
						},
						{
							Name:      "task2",
							DependsOn: []string{"task3"},
						},
						{
							Name:      "task3",
							DependsOn: []string{"task1"},
						},
						{
							Name: "task4",
						},
						{
							Name:      "task5",
							DependsOn: []string{"task4", "task1"},
						},
						{
							Name:      "task6",
							DependsOn: []string{"task4", "task6"},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineContainsCycle,
					Context: []string{
						"Asset `task6` depends on itself",
					},
				},
				{
					Description: pipelineContainsCycle,
					Context: []string{
						"task3 ➜ task1",
						"task2 ➜ task3",
						"task1 ➜ task2",
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsurePipelineHasNoCycles(tt.args.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestEnsureAthenaSQLTypeTasksHasDatabaseAndS3FilePath(t *testing.T) {
	t.Parallel()
	type args struct {
		p *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "no athena.sql task type",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
						},
						{
							Name: "task2",
						},
					},
				},
			},
			want: noIssues,
		},
		{
			name: "all fields and values are correct, no issues",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
							Type: "athena.sql",
							Parameters: map[string]string{
								"database":     "database",
								"s3_file_path": "s3://",
							},
						},
					},
				},
			},
			want: noIssues,
		},
		{
			name: "database value is empty, caught",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
							Type: "athena.sql",
							Parameters: map[string]string{
								"database":     "",
								"s3_file_path": "s3://",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Name: "task1",
						Type: "athena.sql",
						Parameters: map[string]string{
							"database":     "",
							"s3_file_path": "s3://",
						},
					},
					Description: athenaSQLEmptyDatabaseField,
				},
			},
		},
		{
			name: "s3 file path value is wrong, caught",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
							Type: "athena.sql",
							Parameters: map[string]string{
								"database":     "database",
								"s3_file_path": "wrongs3://",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Name: "task1",
						Type: "athena.sql",
						Parameters: map[string]string{
							"database":     "database",
							"s3_file_path": "wrongs3://",
						},
					},
					Description: athenaSQLInvalidS3FilePath,
					Context:     []string{"Given `s3_file_path` is: wrongs3://"},
				},
			},
		},
		{
			name: "database and s3_file_path fields are wrong, caught",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
							Type: "athena.sql",
							Parameters: map[string]string{
								"wrongdb": "database",
								"wrongs3": "s3://",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Name: "task1",
						Type: "athena.sql",
						Parameters: map[string]string{
							"wrongdb": "database",
							"wrongs3": "s3://",
						},
					},
					Description: athenaSQLMissingDatabaseParameter,
				},
				{
					Task: &pipeline.Asset{
						Name: "task1",
						Type: "athena.sql",
						Parameters: map[string]string{
							"wrongdb": "database",
							"wrongs3": "s3://",
						},
					},
					Description: athenaSQLEmptyS3FilePath,
				},
			},
		},
		{
			name: "database field is wrong, caught",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
							Type: "athena.sql",
							Parameters: map[string]string{
								"wrongdb":      "database",
								"s3_file_path": "s3://",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Name: "task1",
						Type: "athena.sql",
						Parameters: map[string]string{
							"wrongdb":      "database",
							"s3_file_path": "s3://",
						},
					},
					Description: athenaSQLMissingDatabaseParameter,
				},
			},
		},
		{
			name: "s3_file_path field is wrong, caught",
			args: args{
				p: &pipeline.Pipeline{
					Assets: []*pipeline.Asset{
						{
							Name: "task1",
							Type: "athena.sql",
							Parameters: map[string]string{
								"database": "database",
								"wrongs3":  "s3://",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Name: "task1",
						Type: "athena.sql",
						Parameters: map[string]string{
							"database": "database",
							"wrongs3":  "s3://",
						},
					},
					Description: athenaSQLEmptyS3FilePath,
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := EnsureAthenaSQLTypeTasksHasDatabaseAndS3FilePath(tt.args.p)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEnsureSlackFieldInPipelineIsValid(t *testing.T) {
	t.Parallel()
	type args struct {
		p *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "no issues",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Name:       "name",
								Connection: "connect",
							},
						},
					},
				},
			},
			want: noIssues,
		},
		{
			name: "empty slack name field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Name:       "",
								Connection: "connect",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackFieldEmptyName,
				},
			},
		},
		{
			name: "empty slack connection field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Name:       "name",
								Connection: "",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackFieldEmptyConnection,
				},
			},
		},

		{
			name: "no slack name field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Connection: "connect",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackFieldEmptyName,
				},
			},
		},
		{
			name: "no slack connection field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Name: "name",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackFieldEmptyConnection,
				},
			},
		},

		{
			name: "empty slack name and connection field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Name:       "",
								Connection: "",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackFieldEmptyName,
				},
				{
					Description: pipelineSlackFieldEmptyConnection,
				},
			},
		},

		{
			name: "no slack name and connection field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackFieldEmptyName,
				},
				{
					Description: pipelineSlackFieldEmptyConnection,
				},
			},
		},

		{
			name: "non unique name field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Name:       "name",
								Connection: "connect1",
							},
							{
								Name:       "name",
								Connection: "connect",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackNameFieldNotUnique,
				},
			},
		},

		{
			name: "non unique connection field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Name:       "name1",
								Connection: "connect",
							},
							{
								Name:       "name",
								Connection: "connect",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackConnectionFieldNotUnique,
				},
			},
		},

		{
			name: "non unique connection and name field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Name:       "name",
								Connection: "connect",
							},
							{
								Name:       "name",
								Connection: "connect",
							},
							{
								Name:       "name1",
								Connection: "connect1",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackNameFieldNotUnique,
				},
				{
					Description: pipelineSlackConnectionFieldNotUnique,
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := EnsureSlackFieldInPipelineIsValid(tt.args.p)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equalf(t, tt.want, got, "EnsureSlackFieldInPipelineIsValid(%v)", tt.args.p)
		})
	}
}

func TestEnsureStartDateIsValid(t *testing.T) {
	t.Parallel()

	type args struct {
		p *pipeline.Pipeline
	}
	tests := []struct {
		name    string
		args    args
		want    []*Issue
		wantErr bool
	}{
		{
			name: "no start date",
			args: args{
				p: &pipeline.Pipeline{},
			},
			want: []*Issue{
				{
					Description: pipelineStartDateCannotBeEmpty,
				},
			},
		},
		{
			name: "malformed start date",
			args: args{
				p: &pipeline.Pipeline{
					StartDate: "2022/09/01",
				},
			},
			want: []*Issue{
				{
					Description: pipelineStartDateMustBeValidDate,
				},
			},
		},
		{
			name: "valid start date",
			args: args{
				p: &pipeline.Pipeline{
					StartDate: "2022-09-01",
				},
			},
			want: noIssues,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			tt := tt
			t.Parallel()
			got, err := EnsureStartDateIsValid(tt.args.p)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
