package lint

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/bruin-data/bruin/pkg/glossary"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CallFuncForEveryAsset(EnsureTaskNameIsValidForASingleAsset)(tt.args.pipeline)
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

	// this is done outside because windows and unix treat paths differently
	// which means we cannot simply put some-path/some-file.sh in the test cases, we need to dynamically join them.
	// e.g. Windows created `some-path\some-file.sh` while macOS creates `some-path/some-file.sh`
	filePath := filepath.Join("some-path", "some-file.sh")
	secondFilePath := filepath.Join("some-path", "some-other-file.sh")

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
					err := fs.MkdirAll(filePath, 0o644)
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
								Path: filePath,
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
							Path: filePath,
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
					file, err := fs.Create(filePath)
					require.NoError(t, err)
					err = fs.Chmod(filePath, 0o755)
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
								Path: filePath,
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
							Path: filePath,
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
							Path: filePath,
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
					file, err := fs.Create(filePath)
					require.NoError(t, err)
					err = fs.Chmod(filePath, os.FileMode(0o100))
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
								Path: filePath,
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
							Path: filePath,
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
					file, err := fs.Create(filePath)
					require.NoError(t, err)
					defer func() { require.NoError(t, file.Close()) }()

					err = fs.Chmod(filePath, 0o644)
					require.NoError(t, err)

					_, err = file.Write([]byte("some content"))
					require.NoError(t, err)

					file, err = fs.Create(secondFilePath)
					require.NoError(t, err)
					defer func() { require.NoError(t, file.Close()) }()

					err = fs.Chmod(secondFilePath, 0o644)
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
								Path: filePath,
							},
						},
						{
							DefinitionFile: pipeline.TaskDefinitionFile{
								Type: pipeline.YamlTask,
							},
							ExecutableFile: pipeline.ExecutableFile{
								Name: "some-other-file.sh",
								Path: secondFilePath,
							},
						},
					},
				},
			},
			want: noIssues,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewMemMapFs()
			if tt.args.setupFilesystem != nil {
				tt.args.setupFilesystem(t, fs)
			}

			checker := CallFuncForEveryAsset(EnsureExecutableFileIsValidForASingleAsset(fs))

			got, err := checker(&tt.args.pipeline)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
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
							Upstreams: []pipeline.Upstream{},
						},
						{
							Name: "task2",
							Upstreams: []pipeline.Upstream{
								{
									Type:  "asset",
									Value: "task1",
								},
								{
									Type:  "asset",
									Value: "task3",
								},
								{
									Type:  "asset",
									Value: "task5",
								},
							},
						},
						{
							Name: "task3",
							Upstreams: []pipeline.Upstream{
								{
									Type:  "asset",
									Value: "task1",
								},
								{
									Type:  "asset",
									Value: "task4",
								},
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Name: "task2",
						Upstreams: []pipeline.Upstream{
							{
								Type:  "asset",
								Value: "task1",
							},
							{
								Type:  "asset",
								Value: "task3",
							},
							{
								Type:  "asset",
								Value: "task5",
							},
						},
					},
					Description: "Dependency 'task5' does not exist",
				},
				{
					Task: &pipeline.Asset{
						Name: "task3",
						Upstreams: []pipeline.Upstream{
							{
								Type:  "asset",
								Value: "task1",
							},
							{
								Type:  "asset",
								Value: "task4",
							},
						},
					},
					Description: "Dependency 'task4' does not exist",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CallFuncForEveryAsset(EnsureDependencyExistsForASingleAsset)(tt.args.p)
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
					Description: "Invalid asset type 'some.random.type'",
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
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CallFuncForEveryAsset(EnsureTypeIsCorrectForASingleAsset)(tt.args.p)
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

	tests := []struct {
		name    string
		p       *pipeline.Pipeline
		want    []*Issue
		wantErr bool
	}{
		{
			name: "empty name is skipped",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "",
					},
				},
			},
			want:    noIssues,
			wantErr: false,
		},
		{
			name: "duplicates are reported",

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
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsureTaskNameIsUnique(tt.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestEnsureTaskNameIsUniqueForASingleAsset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		p       *pipeline.Pipeline
		asset   *pipeline.Asset
		want    []*Issue
		wantErr bool
	}{
		{
			name: "empty name is skipped",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "",
					},
				},
			},
			asset:   &pipeline.Asset{Name: ""},
			want:    noIssues,
			wantErr: false,
		},
		{
			name: "duplicates are reported",
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
			asset: &pipeline.Asset{
				Name: "name1",
				DefinitionFile: pipeline.TaskDefinitionFile{
					Path: "path3",
				},
			},
			want: []*Issue{
				{
					Task: &pipeline.Asset{
						Name: "name1",
						DefinitionFile: pipeline.TaskDefinitionFile{
							Path: "path3",
						},
					},
					Description: "Asset name 'name1' is not unique, please make sure all the task names are unique",
					Context:     []string{"path1", "path3"},
				},
			},
			wantErr: false,
		},
		{
			name: "no duplicates are found, all good",
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
						Name: "name3",
						DefinitionFile: pipeline.TaskDefinitionFile{
							Path: "path3",
						},
					},
				},
			},
			asset: &pipeline.Asset{
				Name: "name1",
				DefinitionFile: pipeline.TaskDefinitionFile{
					Path: "path1",
				},
			},
			want: noIssues,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsureTaskNameIsUniqueForASingleAsset(context.Background(), tt.p, tt.asset)
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
							Name: "task1",
							Upstreams: []pipeline.Upstream{
								{Type: "asset", Value: "task2"},
								{Type: "asset", Value: "task0"},
							},
						},
						{
							Name: "task2",
							Upstreams: []pipeline.Upstream{
								{Type: "asset", Value: "task3"},
							},
						},
						{
							Name: "task3",
							Upstreams: []pipeline.Upstream{
								{Type: "asset", Value: "task1"},
							},
						},
						{
							Name: "task4",
						},
						{
							Name: "task5",
							Upstreams: []pipeline.Upstream{
								{Type: "asset", Value: "task4"},
								{Type: "asset", Value: "task1"},
							},
						},
						{
							Name: "task6",
							Upstreams: []pipeline.Upstream{
								{Type: "asset", Value: "task4"},
								{Type: "asset", Value: "task6"},
							},
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
								Channel: "#data",
							},
						},
					},
				},
			},
			want: noIssues,
		},
		{
			name: "empty channel field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Channel: "",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackFieldEmptyChannel,
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
					Description: pipelineSlackFieldEmptyChannel,
				},
			},
		},

		{
			name: "non unique channel field with and without hash",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Channel: "#data",
							},
							{
								Channel: "data",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackChannelFieldNotUnique,
				},
			},
		},
		{
			name: "non unique channel field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						Slack: []pipeline.SlackNotification{
							{
								Channel: "#data",
							},
							{
								Channel: "#data",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineSlackChannelFieldNotUnique,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := EnsureSlackFieldInPipelineIsValid(tt.args.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equalf(t, tt.want, got, "EnsureSlackFieldInPipelineIsValid(%v)", tt.args.p)
		})
	}
}

func TestMSTeamsFieldInPipelineIsValid(t *testing.T) {
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
						MSTeams: []pipeline.MSTeamsNotification{
							{
								Connection: "some_conn",
							},
						},
					},
				},
			},
			want: noIssues,
		},
		{
			name: "empty channel field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						MSTeams: []pipeline.MSTeamsNotification{
							{
								Connection: "some_conn",
							},
							{
								Connection: "some_conn",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineMSTeamsConnectionFieldNotUnique,
				},
			},
		},
		{
			name: "no slack name and connection field",
			args: args{
				p: &pipeline.Pipeline{
					Notifications: pipeline.Notifications{
						MSTeams: []pipeline.MSTeamsNotification{
							{
								Connection: "",
							},
						},
					},
				},
			},
			want: []*Issue{
				{
					Description: pipelineMSTeamsConnectionFieldEmpty,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := EnsureMSTeamsFieldInPipelineIsValid(tt.args.p)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			assert.Equalf(t, tt.want, got, "EnsureMSTeamsFieldInPipelineIsValid(%v)", tt.args.p)
		})
	}
}

func TestEnsureMaterializationValuesAreValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		assets  []*pipeline.Asset
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "no materialization",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "view materialization has extra fields",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:           pipeline.MaterializationTypeView,
						Strategy:       "whatever",
						IncrementalKey: "whatever",
						ClusterBy:      []string{"whatever"},
						PartitionBy:    "whatever",
					},
				},
			},
			wantErr: assert.NoError,
			want: []string{
				materializationStrategyIsNotSupportedForViews,
				materializationIncrementalKeyNotSupportedForViews,
				materializationClusterByNotSupportedForViews,
				materializationPartitionByNotSupportedForViews,
			},
		},
		{
			name: "table materialization has create+replace, all good",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:     pipeline.MaterializationTypeTable,
						Strategy: pipeline.MaterializationStrategyCreateReplace,
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "table materialization has append, all good",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:     pipeline.MaterializationTypeTable,
						Strategy: pipeline.MaterializationStrategyAppend,
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "table materialization has incremental key but wrong strategy",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:           pipeline.MaterializationTypeTable,
						Strategy:       pipeline.MaterializationStrategyCreateReplace,
						IncrementalKey: "dt",
					},
				},
			},
			wantErr: assert.NoError,
			want: []string{
				"Incremental key is only supported with 'delete+insert' or 'time_interval' strategies.",
			},
		},
		{
			name: "table materialization has delete+insert but no incremental key",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:     pipeline.MaterializationTypeTable,
						Strategy: pipeline.MaterializationStrategyDeleteInsert,
					},
				},
			},
			wantErr: assert.NoError,
			want: []string{
				"Materialization strategy 'delete+insert' requires the 'incremental_key' field to be set",
			},
		},
		{
			name: "some random materialization strategy is used",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:     pipeline.MaterializationTypeTable,
						Strategy: "whatever",
					},
				},
			},
			wantErr: assert.NoError,
			want: []string{
				fmt.Sprintf(
					"Materialization strategy 'whatever' is not supported, available strategies are: %v",
					pipeline.AllAvailableMaterializationStrategies,
				),
			},
		},
		{
			name: "some random materialization type is used",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type: "whatever",
					},
				},
			},
			wantErr: assert.NoError,
			want: []string{
				fmt.Sprintf(
					"Materialization type 'whatever' is not supported, available types are: %v",
					[]pipeline.MaterializationType{
						pipeline.MaterializationTypeView,
						pipeline.MaterializationTypeTable,
					},
				),
			},
		},
		{
			name: "successful table incremental materialization",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:           pipeline.MaterializationTypeTable,
						Strategy:       pipeline.MaterializationStrategyDeleteInsert,
						IncrementalKey: "dt",
						ClusterBy:      []string{"dt"},
						PartitionBy:    "dt",
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "table materialization has merge but no columns",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:     pipeline.MaterializationTypeTable,
						Strategy: pipeline.MaterializationStrategyMerge,
					},
				},
			},
			wantErr: assert.NoError,
			want: []string{
				"Materialization strategy 'merge' requires the 'columns' field to be set with actual columns",
				"Materialization strategy 'merge' requires the 'primary_key' field to be set on at least one column",
			},
		},
		{
			name: "table materialization has merge but no columns",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:     pipeline.MaterializationTypeTable,
						Strategy: pipeline.MaterializationStrategyMerge,
					},
					Columns: []pipeline.Column{
						{Name: "dt"},
						{Name: "abc"},
					},
				},
			},
			wantErr: assert.NoError,
			want: []string{
				"Materialization strategy 'merge' requires the 'primary_key' field to be set on at least one column",
			},
		},
		{
			name: "table materialization has merge and it is successful",
			assets: []*pipeline.Asset{
				{
					Name: "task1",
					Materialization: pipeline.Materialization{
						Type:     pipeline.MaterializationTypeTable,
						Strategy: pipeline.MaterializationStrategyMerge,
					},
					Columns: []pipeline.Column{
						{Name: "dt", PrimaryKey: true},
						{Name: "abc"},
					},
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CallFuncForEveryAsset(EnsureMaterializationValuesAreValidForSingleAsset)(&pipeline.Pipeline{
				Assets: tt.assets,
			})

			if !tt.wantErr(t, err) {
				return
			}

			// I am doing this because I don't care if I get a nil or empty slice
			if tt.want != nil {
				gotMessages := make([]string, len(got))
				for i, issue := range got {
					gotMessages[i] = issue.Description
				}

				assert.Equal(t, tt.want, gotMessages)
			} else {
				assert.Equal(t, []*Issue{}, got)
			}
		})
	}
}

func TestEnsureSnowflakeSensorHasQueryParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		p       *pipeline.Pipeline
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "no query param",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeSnowflakeQuerySensor,
					},
				},
			},
			wantErr: assert.NoError,
			want:    []string{"Snowflake query sensor requires a `query` parameter"},
		},
		{
			name: "query param exists but empty",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeSnowflakeQuerySensor,
						Parameters: map[string]string{
							"query": "",
						},
					},
				},
			},
			wantErr: assert.NoError,
			want:    []string{"Snowflake query sensor requires a `query` parameter that is not empty"},
		},
		{
			name: "no issues",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeSnowflakeQuerySensor,
						Parameters: map[string]string{
							"query": "SELECT 1",
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CallFuncForEveryAsset(EnsureSnowflakeSensorHasQueryParameterForASingleAsset)(tt.p)
			if !tt.wantErr(t, err) {
				return
			}

			// I am doing this because I don't care if I get a nil or empty slice
			if tt.want != nil {
				gotMessages := make([]string, len(got))
				for i, issue := range got {
					gotMessages[i] = issue.Description
				}

				assert.Equal(t, tt.want, gotMessages)
			} else {
				assert.Equal(t, []*Issue{}, got)
			}
		})
	}
}

func TestEnsureBigqueryQuerySensorHasQueryParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		p       *pipeline.Pipeline
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "no query param",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeBigqueryQuerySensor,
					},
				},
			},
			wantErr: assert.NoError,
			want:    []string{"BigQuery query sensor requires a `query` parameter"},
		},
		{
			name: "query param exists but empty",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeBigqueryQuerySensor,
						Parameters: map[string]string{
							"query": "",
						},
					},
				},
			},
			wantErr: assert.NoError,
			want:    []string{"BigQuery query sensor requires a `query` parameter that is not empty"},
		},
		{
			name: "no issues",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeBigqueryQuerySensor,
						Parameters: map[string]string{
							"query": "SELECT 1",
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CallFuncForEveryAsset(EnsureBigQueryQuerySensorHasTableParameterForASingleAsset)(tt.p)
			if !tt.wantErr(t, err) {
				return
			}

			// I am doing this because I don't care if I get a nil or empty slice
			if tt.want != nil {
				gotMessages := make([]string, len(got))
				for i, issue := range got {
					gotMessages[i] = issue.Description
				}

				assert.Equal(t, tt.want, gotMessages)
			} else {
				assert.Equal(t, []*Issue{}, got)
			}
		})
	}
}

func TestEnsureBigQueryTableSensorHasTableParameter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		p       *pipeline.Pipeline
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "no table param",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeBigqueryTableSensor,
					},
				},
			},
			wantErr: assert.NoError,
			want:    []string{"BigQuery table sensor requires a `table` parameter"},
		},
		{
			name: "table param exists but empty",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeBigqueryTableSensor,
						Parameters: map[string]string{
							"table": "",
						},
					},
				},
			},
			wantErr: assert.NoError,
			want:    []string{"BigQuery table sensor `table` parameter must be either in the format `dataset.table` or `project.dataset.table`"},
		},
		{
			name: "table param exists with dataset.table, valid",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeBigqueryTableSensor,
						Parameters: map[string]string{
							"table": "a.b",
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
		{
			name: "no issues",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "task1",
						Type: pipeline.AssetTypeBigqueryTableSensor,
						Parameters: map[string]string{
							"table": "a.b.c",
						},
					},
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := CallFuncForEveryAsset(EnsureBigQueryTableSensorHasTableParameterForASingleAsset)(tt.p)
			if !tt.wantErr(t, err) {
				return
			}

			// I am doing this because I don't care if I get a nil or empty slice
			if tt.want != nil {
				gotMessages := make([]string, len(got))
				for i, issue := range got {
					gotMessages[i] = issue.Description
				}

				assert.Equal(t, tt.want, gotMessages)
			} else {
				assert.Equal(t, []*Issue{}, got)
			}
		})
	}
}

func TestEnsureIngestrAssetIsValidForASingleAsset(t *testing.T) {
	t.Parallel()

	expectedErr := "Ingestr assets require the following parameters: source_connection, source_table, destination"
	tests := []struct {
		name           string
		asset          *pipeline.Asset
		wantErrMessage string
		wantErr        assert.ErrorAssertionFunc
	}{
		{
			name: "empty asset",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeIngestr,
			},
			wantErrMessage: expectedErr,
			wantErr:        assert.NoError,
		},
		{
			name: "asset with some params missing",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeIngestr,
				Parameters: map[string]string{
					"source": "source",
				},
			},
			wantErrMessage: expectedErr,
			wantErr:        assert.NoError,
		},
		{
			name: "asset with all params there but has some update-on-merge columns",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeIngestr,
				Parameters: map[string]string{
					"source_connection":      "source_connection",
					"source_table":           "source_table",
					"destination":            "destination",
					"destination_connection": "destination_connection",
					"destination_table":      "destination_table",
				},
				Columns: []pipeline.Column{
					{Name: "dt", PrimaryKey: true},
					{Name: "abc", UpdateOnMerge: true},
				},
			},
			wantErrMessage: "Ingestr assets do not support the 'update_on_merge' field, the strategy used decide the update behavior",
			wantErr:        assert.NoError,
		},
		{
			name: "asset with all params there",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeIngestr,
				Parameters: map[string]string{
					"source_connection":      "source_connection",
					"source_table":           "source_table",
					"destination":            "destination",
					"destination_connection": "destination_connection",
					"destination_table":      "destination_table",
				},
			},
			wantErrMessage: "",
			wantErr:        assert.NoError,
		},
		{
			name: "ingestr asset with merge strategy but no primary key",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeIngestr,
				Parameters: map[string]string{
					"source_connection":    "conn1",
					"source_table":         "table1",
					"destination":          "dest1",
					"incremental_strategy": "merge",
				},
				Columns: []pipeline.Column{
					{Name: "col1"},
					{Name: "col2"},
				},
			},
			wantErrMessage: "Materialization strategy 'merge' requires the 'primary_key' field to be set on at least one column",
			wantErr:        assert.NoError,
		},
		{
			name: "valid ingestr asset with merge strategy and primary key",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeIngestr,
				Parameters: map[string]string{
					"source_connection":    "conn1",
					"source_table":         "table1",
					"destination":          "dest1",
					"incremental_strategy": "merge",
				},
				Columns: []pipeline.Column{
					{Name: "col1", PrimaryKey: true},
					{Name: "col2"},
				},
			},
			wantErrMessage: "",
			wantErr:        assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &pipeline.Pipeline{Assets: []*pipeline.Asset{tt.asset}}
			got, err := EnsureIngestrAssetIsValidForASingleAsset(context.Background(), p, tt.asset)
			if !tt.wantErr(t, err) {
				return
			}

			if tt.wantErrMessage != "" {
				assert.Len(t, got, 1)
				assert.Equal(t, tt.wantErrMessage, got[0].Description)
			} else {
				assert.Equal(t, []*Issue{}, got)
			}
		})
	}
}

func TestEnsurePipelineStartDateIsValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		p       *pipeline.Pipeline
		want    []*Issue
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "empty start date is skipped",
			p: &pipeline.Pipeline{
				StartDate: "",
			},
			want:    noIssues,
			wantErr: assert.NoError,
		},
		{
			name: "invalid start date is reported",
			p: &pipeline.Pipeline{
				StartDate: "20240101",
			},
			want:    []*Issue{{Description: "start_date must be in the format of YYYY-MM-DD in the pipeline definition, '20240101' given"}},
			wantErr: assert.NoError,
		},
		{
			name: "valid start date is not reported as error",
			p: &pipeline.Pipeline{
				StartDate: "2024-01-01",
			},
			want:    noIssues,
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EnsurePipelineStartDateIsValid(tt.p)
			tt.wantErr(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestGlossaryChecker_EnsureAssetEntitiesExistInGlossary(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asset   *pipeline.Asset
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "asset with no cols skipped",
			asset: &pipeline.Asset{
				Name: "asset1",
			},
			want:    []string{},
			wantErr: assert.NoError,
		},
		{
			name: "asset with no attribute reference skipped",
			asset: &pipeline.Asset{
				Name: "asset1",
				Columns: []pipeline.Column{
					{Name: "col1"},
					{Name: "col2"},
				},
			},
			want:    []string{},
			wantErr: assert.NoError,
		},
		{
			name: "asset with empty entity is reported",
			asset: &pipeline.Asset{
				Name: "asset1",
				Columns: []pipeline.Column{
					{Name: "col1"},
					{
						Name: "col2",
						EntityAttribute: &pipeline.EntityAttribute{
							Entity: "",
						},
					},
				},
			},
			want:    []string{"Entity name cannot be empty"},
			wantErr: assert.NoError,
		},
		{
			name: "asset with empty attribute is reported",
			asset: &pipeline.Asset{
				Name: "asset1",
				Columns: []pipeline.Column{
					{Name: "col1"},
					{
						Name: "col2",
						EntityAttribute: &pipeline.EntityAttribute{
							Entity:    "SomeEntity",
							Attribute: "",
						},
					},
				},
			},
			want:    []string{"Attribute name cannot be empty"},
			wantErr: assert.NoError,
		},
		{
			name: "asset with missing entity reference is reported",
			asset: &pipeline.Asset{
				Name: "asset1",
				Columns: []pipeline.Column{
					{Name: "col1"},
					{
						Name: "col2",
						EntityAttribute: &pipeline.EntityAttribute{
							Entity:    "SomeEntity",
							Attribute: "SomeAttribute",
						},
					},
				},
			},
			want:    []string{"Entity 'SomeEntity' does not exist in the glossary"},
			wantErr: assert.NoError,
		},
		{
			name: "asset with missing attribute reference is reported",
			asset: &pipeline.Asset{
				Name: "asset1",
				Columns: []pipeline.Column{
					{Name: "col1"},
					{
						Name: "col2",
						EntityAttribute: &pipeline.EntityAttribute{
							Entity:    "entity1",
							Attribute: "SomeAttribute",
						},
					},
				},
			},
			want:    []string{"Attribute 'SomeAttribute' does not exist in the entity 'entity1'"},
			wantErr: assert.NoError,
		},
		{
			name: "asset with accurate entity is not reported",
			asset: &pipeline.Asset{
				Name: "asset1",
				Columns: []pipeline.Column{
					{Name: "col1"},
					{
						Name: "col2",
						EntityAttribute: &pipeline.EntityAttribute{
							Entity:    "entity1",
							Attribute: "attr2",
						},
					},
				},
			},
			want:    []string{},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			checker := GlossaryChecker{
				foundGlossary: &glossary.Glossary{
					Entities: []*glossary.Entity{
						{
							Name: "entity1",
							Attributes: map[string]*glossary.Attribute{
								"attr1": {
									Name: "attr1",
								},
								"attr2": {
									Name: "attr2",
								},
							},
						},
					},
				},
			}

			got, err := checker.EnsureAssetEntitiesExistInGlossary(context.Background(), &pipeline.Pipeline{}, tt.asset)
			if !tt.wantErr(t, err) {
				return
			}

			gotMessages := make([]string, len(got))
			for i, issue := range got {
				gotMessages[i] = issue.Description
			}

			assert.Equal(t, tt.want, gotMessages)
		})
	}
}

type mockSQLParser struct {
	mock.Mock
}

func (m *mockSQLParser) UsedTables(sql, dialect string) ([]string, error) {
	args := m.Called(sql, dialect)
	return args.Get(0).([]string), args.Error(1)
}

type mockRenderer struct {
	mock.Mock
}

func (m *mockRenderer) Render(query string) (string, error) {
	args := m.Called(query)
	return args.Get(0).(string), args.Error(1)
}

func TestUsedTableValidatorRule_ValidateAsset(t *testing.T) {
	t.Parallel()

	pp := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name: "asset1",
			},
			{
				Name: "asset2",
			},
			{
				Name: "asset3",
			},
			{
				Name: "asset4",
			},
		},
	}

	tests := []struct {
		name    string
		setup   func(m *mockRenderer, p *mockSQLParser)
		asset   *pipeline.Asset
		want    []string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "irrelevant assets are ignored",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypePython,
			},
			want:    []string{},
			wantErr: assert.NoError,
		},
		{
			name: "assets that do not have materialization are ignored",
			asset: &pipeline.Asset{
				Type: pipeline.AssetTypeBigqueryQuery,
			},
			want:    []string{},
			wantErr: assert.NoError,
		},
		{
			name: "render fails",
			asset: &pipeline.Asset{
				Type:            pipeline.AssetTypeBigqueryQuery,
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
				ExecutableFile:  pipeline.ExecutableFile{Content: "SELECT 1"},
			},
			setup: func(m *mockRenderer, p *mockSQLParser) {
				m.On("Render", "SELECT 1").Return("", errors.New("something failed"))
			},
			want:    []string{"Failed to render the query before parsing the SQL"},
			wantErr: assert.NoError,
		},
		{
			name: "query parsing fails but ignored",
			asset: &pipeline.Asset{
				Type:            pipeline.AssetTypeBigqueryQuery,
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
				ExecutableFile:  pipeline.ExecutableFile{Content: "SELECT 1"},
			},
			setup: func(m *mockRenderer, p *mockSQLParser) {
				m.On("Render", "SELECT 1").Return("materialized select", nil)
				p.On("UsedTables", "materialized select", "bigquery").Return([]string{}, errors.New("something failed"))
			},
			want:    []string{},
			wantErr: assert.NoError,
		},
		{
			name: "query parsing succeeds but returns no tables",
			asset: &pipeline.Asset{
				Type:            pipeline.AssetTypeBigqueryQuery,
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
				ExecutableFile:  pipeline.ExecutableFile{Content: "SELECT 1"},
			},
			setup: func(m *mockRenderer, p *mockSQLParser) {
				m.On("Render", "SELECT 1").Return("materialized select", nil)
				p.On("UsedTables", "materialized select", "bigquery").Return([]string{}, nil)
			},
			want:    []string{},
			wantErr: assert.NoError,
		},
		{
			name: "query parsing succeeds, missing tables are caught",
			asset: &pipeline.Asset{
				Type:            pipeline.AssetTypeBigqueryQuery,
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
				ExecutableFile:  pipeline.ExecutableFile{Content: "SELECT 1"},
				Upstreams: []pipeline.Upstream{
					{
						Type:  "asset",
						Value: "asset1",
					},
					{
						Type:  "uri",
						Value: "some://other.upstream",
					},
					{
						Type:  "asset",
						Value: "asset2",
					},
				},
			},
			setup: func(m *mockRenderer, p *mockSQLParser) {
				m.On("Render", "SELECT 1").Return("materialized select", nil)
				p.On("UsedTables", "materialized select", "bigquery").Return([]string{
					"asset1", "asset2", "asset3", "asset4", "some.asset-that-doesnt-exist", "some.other.asset",
				}, nil)
			},
			want: []string{
				"Table 'asset3' is used in the query but not referenced in the 'depends' array.",
				"Table 'asset4' is used in the query but not referenced in the 'depends' array.",
			},
			wantErr: assert.NoError,
		},
		{
			name: "query parsing succeeds, all is good",
			asset: &pipeline.Asset{
				Type:            pipeline.AssetTypeBigqueryQuery,
				Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable},
				ExecutableFile:  pipeline.ExecutableFile{Content: "SELECT 1"},
				Upstreams: []pipeline.Upstream{
					{
						Type:  "asset",
						Value: "asset1",
					},
					{
						Type:  "uri",
						Value: "some://other.upstream",
					},
					{
						Type:  "asset",
						Value: "asset2",
					},
				},
			},
			setup: func(m *mockRenderer, p *mockSQLParser) {
				m.On("Render", "SELECT 1").Return("materialized select", nil)
				p.On("UsedTables", "materialized select", "bigquery").Return([]string{
					"asset1", "asset2", "some.asset-that-doesnt-exist", "some.other.asset",
				}, nil)
			},
			want:    []string{},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			renderer := new(mockRenderer)
			parser := new(mockSQLParser)
			if tt.setup != nil {
				tt.setup(renderer, parser)
			}

			u := UsedTableValidatorRule{renderer, parser}
			got, err := u.ValidateAsset(context.Background(), pp, tt.asset)
			if !tt.wantErr(t, err) {
				return
			}

			issueMessages := make([]string, len(got))
			for i, issue := range got {
				issueMessages[i] = issue.Description
			}

			sort.Strings(issueMessages)

			assert.Equal(t, tt.want, issueMessages)
		})
	}
}

func TestValidateDuplicateColumnNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asset   *pipeline.Asset
		want    []*Issue
		wantErr bool
	}{
		{
			name: "no duplicate column names",
			asset: &pipeline.Asset{
				Name: "asset1",
				Columns: []pipeline.Column{
					{Name: "col1"},
					{Name: "col2"},
					{Name: "col3"},
				},
			},
			want:    nil,
			wantErr: false,
		},
		{
			name: "duplicate column names",
			asset: &pipeline.Asset{
				Name: "asset1",
				Columns: []pipeline.Column{
					{Name: "col1"},
					{Name: "col2"},
					{Name: "Col1"},
				},
			},
			want: []*Issue{
				{
					Task:        &pipeline.Asset{Name: "asset1", Columns: []pipeline.Column{{Name: "col1"}, {Name: "col2"}, {Name: "Col1"}}},
					Description: "Duplicate column name 'Col1' found ",
				},
			},
			wantErr: false,
		},
		{
			name: "multiple duplicate column names",
			asset: &pipeline.Asset{
				Name: "asset1",
				Columns: []pipeline.Column{
					{Name: "col1"},
					{Name: "Col1"},
					{Name: "col2"},
					{Name: "COL2"},
				},
			},
			want: []*Issue{
				{
					Task:        &pipeline.Asset{Name: "asset1", Columns: []pipeline.Column{{Name: "col1"}, {Name: "Col1"}, {Name: "col2"}, {Name: "COL2"}}},
					Description: "Duplicate column name 'Col1' found ",
				},
				{
					Task:        &pipeline.Asset{Name: "asset1", Columns: []pipeline.Column{{Name: "col1"}, {Name: "Col1"}, {Name: "col2"}, {Name: "COL2"}}},
					Description: "Duplicate column name 'COL2' found ",
				},
			},
			wantErr: false,
		},
		{
			name: "no columns in asset",
			asset: &pipeline.Asset{
				Name: "asset1",
			},
			want:    nil,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ValidateDuplicateColumnNames(context.Background(), &pipeline.Pipeline{}, tt.asset)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestEnsureValidPythonAssetMaterialization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		p       *pipeline.Pipeline
		want    []*Issue
		wantErr bool
	}{
		{
			name: "valid python asset materialization",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "asset1",
						Type: pipeline.AssetTypePython,
						Materialization: pipeline.Materialization{
							Type: pipeline.MaterializationTypeTable,
						},
						Connection: "conn1",
					},
				},
			},
			want:    []*Issue{},
			wantErr: false,
		},
		{
			name: "invalid python asset materialization",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "asset1",
						Type: pipeline.AssetTypePython,
						Materialization: pipeline.Materialization{
							Type: pipeline.MaterializationTypeTable,
						},
					},
				},
			},
			want: []*Issue{
				{
					Task:        &pipeline.Asset{Name: "asset1", Type: pipeline.AssetTypePython, Materialization: pipeline.Materialization{Type: pipeline.MaterializationTypeTable}},
					Description: "A task with materialization must have a connection defined",
				},
			},
			wantErr: false,
		},
		{
			name: "valid python asset materialization with view",
			p: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						Name: "asset1",
						Type: pipeline.AssetTypePython,
						Materialization: pipeline.Materialization{
							Type: pipeline.MaterializationTypeView,
						},
					},
				},
			},
			want:    []*Issue{},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := ValidatePythonAssetMaterialization(context.Background(), tt.p, tt.p.Assets[0])
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWarnRegularYamlFiles_WarnRegularYamlFilesInRepo(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		p       *pipeline.Pipeline
		want    []*Issue
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "no regular yaml files",
			p: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: path.AbsPathForTests(t, "./testdata/regular-yaml-files/pipeline2/pipeline.yml"),
				},
			},
			want:    noIssues,
			wantErr: assert.NoError,
		},
		{
			name: "regular yaml files are caught",
			p: &pipeline.Pipeline{
				DefinitionFile: pipeline.DefinitionFile{
					Path: path.AbsPathForTests(t, "./testdata/regular-yaml-files/pipeline1/pipeline.yml"),
				},
			},
			want: []*Issue{
				{
					Description: "Regular YAML files are not treated as assets, please rename them to `.asset.yml` if you intended to create assets.",
					Context: []string{
						filepath.Join("assets", "file1.yml"),
					},
				},
			},
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &WarnRegularYamlFiles{
				fs: afero.NewOsFs(),
			}
			got, err := w.WarnRegularYamlFilesInRepo(tt.p)
			if !tt.wantErr(t, err, fmt.Sprintf("WarnRegularYamlFilesInRepo(%v)", tt.p)) {
				return
			}
			assert.Equalf(t, tt.want, got, "WarnRegularYamlFilesInRepo(%v)", tt.p)
		})
	}
}
