package path

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

const testPipelinePath = "testdata/walk/pipelines"

func TestGetPipelinePaths(t *testing.T) {
	t.Parallel()

	firstPipelineAbsolute, err := filepath.Abs("testdata/walk/pipelines/first-pipeline")
	require.NoError(t, err)

	secondPipelineAbsolute, err := filepath.Abs("testdata/walk/pipelines/second-pipeline")
	require.NoError(t, err)

	tests := []struct {
		name                   string
		root                   string
		pipelineDefinitionFile []string
		want                   []string
		wantErr                bool
	}{
		{
			name:                   "pipelines are found",
			root:                   testPipelinePath,
			pipelineDefinitionFile: []string{"pipeline.yml"},
			want:                   []string{firstPipelineAbsolute, secondPipelineAbsolute},
		},
		{
			name:                   "filepath errors are propagated",
			root:                   "some-random-directory-name-that-does-not-exist",
			pipelineDefinitionFile: []string{"pipeline.yml"},
			wantErr:                true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := GetPipelinePaths(tt.root, tt.pipelineDefinitionFile)

			if (err != nil) != tt.wantErr {
				t.Errorf("GetPipelinePaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestGetPipelineRootFromTask(t *testing.T) {
	t.Parallel()

	firstFilePath := filepath.Join("testdata", "walk", "task-to-pipeline", "first-pipeline")
	secondFilePath := filepath.Join("testdata", "walk", "task-to-pipeline", "second-pipeline")

	firstPipelineAbsolute, err := filepath.Abs(firstFilePath)
	require.NoError(t, err)

	secondPipelineAbsolute, err := filepath.Abs(secondFilePath)
	require.NoError(t, err)

	tests := []struct {
		name                   string
		taskPath               string
		pipelineDefinitionFile []string
		want                   string
		wantErr                bool
	}{
		{
			name:                   "pipeline is found from a deeply nested task",
			taskPath:               filepath.Join("testdata", "walk", "task-to-pipeline", "first-pipeline", "tasks", "helloworld", "task.yml"),
			pipelineDefinitionFile: []string{"pipeline.yml"},
			want:                   firstPipelineAbsolute,
		},
		{
			name:                   "pipeline is found from a shallow nested task",
			taskPath:               filepath.Join("testdata", "walk", "task-to-pipeline", "second-pipeline", "tasks", "task1.yml"),
			pipelineDefinitionFile: []string{"pipeline.yml"},
			want:                   secondPipelineAbsolute,
		},
		{
			name:                   "pipeline is found even if definition file name is different",
			taskPath:               filepath.Join("testdata", "walk", "task-to-pipeline", "second-pipeline", "tasks", "test2", "task.yml"),
			pipelineDefinitionFile: []string{"task1.yml"},
			want:                   filepath.Join(secondPipelineAbsolute, "tasks"),
		},
		{
			name:                   "an error is returned when the pipeline is not found",
			taskPath:               "testdata",
			pipelineDefinitionFile: []string{"pipeline.yml"},
			wantErr:                true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := GetPipelineRootFromTask(tt.taskPath, tt.pipelineDefinitionFile)

			if (err != nil) != tt.wantErr {
				t.Errorf("GetPipelinePaths() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			require.Equal(t, tt.want, got)
		})
	}
}

func TestGetAllFilesRecursive(t *testing.T) {
	t.Parallel()

	type args struct {
		root string
	}

	mp := func(path string) string {
		abs, err := filepath.Abs(testPipelinePath)
		if err != nil {
			t.Fatal()
		}
		return filepath.Join(abs, path)
	}

	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{
			name: "all files with the correct extension are found",
			args: args{
				root: testPipelinePath,
			},
			want: []string{
				mp("first-pipeline/pipeline.yml"),
				mp("first-pipeline/tasks/helloworld/task.yml"),
				mp("first-pipeline/tasks/test1/task.yml"),
				mp("first-pipeline/tasks/test2/task.yml"),
				mp("second-pipeline/pipeline.yml"),
				mp("second-pipeline/tasks/test1/task.yml"),
				mp("second-pipeline/tasks/test2/task.yml"),
			},
		},
		{
			name: "filepath errors are propagated",
			args: args{
				root: "some-random-directory-name-that-does-not-exist",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := GetAllFilesRecursive(tt.args.root, []string{".yml"})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.want, got)
		})
	}
}
