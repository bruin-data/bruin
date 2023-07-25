package path

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type parents struct {
	FirstParent  string `yaml:"parent1"`
	SecondParent string `yaml:"parent2"`
}

type family struct {
	Parents  parents  `yaml:"parents"`
	Siblings []string `yaml:"siblings"`
}

type exampleData struct {
	Name    string   `yaml:"name"`
	Middle  string   `yaml:"middle" validate:"required"`
	Surname string   `yaml:"surname"`
	Age     int      `yaml:"age" validate:"required,gte=28"`
	Height  float64  `yaml:"height"`
	Skills  []string `yaml:"skills"`
	Family  family   `yaml:"family"`
}

func Test_readYamlFileFromPath(t *testing.T) {
	t.Parallel()

	type args struct {
		path string
		out  interface{}
	}

	tests := []struct {
		name           string
		args           args
		expectedOutput *exampleData
		wantErr        bool
	}{
		{
			name: "read valid yaml file from path",
			args: args{
				path: "testdata/yamlreader/successful-validation.yml",
				out:  &exampleData{},
			},
			expectedOutput: &exampleData{
				Name:    "jane",
				Middle:  "james",
				Surname: "doe",
				Age:     30,
				Height:  1.65,
				Skills:  []string{"java", "python", "go"},
				Family: family{
					Parents: parents{
						FirstParent:  "mama",
						SecondParent: "papa",
					},
					Siblings: []string{"sister", "brother"},
				},
			},
		},
		{
			name: "read yaml file from path",
			args: args{
				path: "testdata/yamlreader/no-validation.yml",
				out:  &exampleData{},
			},
			wantErr: true,
		},
		{
			name: "file does not exist",
			args: args{
				path: "testdata/yamlreader/some-file-that-doesnt-exist",
			},
			wantErr: true,
		},
		{
			name: "invalid yaml file",
			args: args{
				path: "testdata/yamlreader/invalid.yml",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := ReadYaml(afero.NewOsFs(), tt.args.path, tt.args.out)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedOutput, tt.args.out)
			}
		})
	}
}

func TestExcludeItemsInDirectoryContainingFile(t *testing.T) {
	t.Parallel()

	type args struct {
		filePaths []string
		file      string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "all sub-items in directory for task.yml are removed",
			args: args{
				filePaths: []string{
					"pipeline/tasks/task1/task.yml",
					"pipeline/tasks/task1/runfile.sh",
					"pipeline/tasks/task1/some/nested/dir/code.py",
					"pipeline/tasks/task1/some/nested/dir/another/task.yml",
					"pipeline/tasks/task2/task1/code2.py",
					"pipeline/tasks/task3.py",
				},
				file: "task.yml",
			},
			want: []string{
				"pipeline/tasks/task1/task.yml",
				"pipeline/tasks/task2/task1/code2.py",
				"pipeline/tasks/task3.py",
			},
		},
		{
			name: "empty list is handled fine",
			args: args{
				filePaths: []string{},
				file:      "task.yml",
			},
			want: []string{},
		},
		{
			name: "only task.yml works fine",
			args: args{
				filePaths: []string{"path/to/task.yml"},
				file:      "task.yml",
			},
			want: []string{"path/to/task.yml"},
		},
		{
			name: "no task.yml is also okay",
			args: args{
				filePaths: []string{
					"pipeline/tasks/task1/runfile.sh",
					"pipeline/tasks/task1/some/nested/dir/code.py",
					"pipeline/tasks/task2/task1/code2.py",
					"pipeline/tasks/task3.py",
				},
				file: "task.yml",
			},
			want: []string{
				"pipeline/tasks/task1/runfile.sh",
				"pipeline/tasks/task1/some/nested/dir/code.py",
				"pipeline/tasks/task2/task1/code2.py",
				"pipeline/tasks/task3.py",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ExcludeSubItemsInDirectoryContainingFile(tt.args.filePaths, tt.args.file)
			require.Equal(t, tt.want, got)
		})
	}
}

func BenchmarkExcludeItemsInDirectoryContainingFile(b *testing.B) {
	filePaths := []string{
		"pipeline/tasks/task1/task.yml",
		"pipeline/tasks/task1/runfile.sh",
		"pipeline/tasks/task1/some/nested/dir/code.py",
		"pipeline/tasks/task1/some/nested/dir/another/task.yml",
		"pipeline/tasks/task2/task1/code2.py",
		"pipeline/tasks/task3.py",
	}
	file := "task.yml"

	for i := 0; i < b.N; i++ {
		ExcludeSubItemsInDirectoryContainingFile(filePaths, file)
	}
}

func TestDirExists(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	err := fs.MkdirAll("/path1/path2/path3", 0o644)
	assert.NoError(t, err, "failed to create the in-memory directory")

	err = fs.MkdirAll("/path1/path2/venv", 0o644)
	require.NoError(t, err, "failed to create the in-memory directory")

	tests := []struct {
		name      string
		searchDir string
		want      bool
	}{
		{
			name:      "directory doesn't exists",
			searchDir: "/path1/path2/path3/venv",
			want:      false,
		},
		{
			name:      "directory exists",
			searchDir: "/path1/path2/venv",
			want:      true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := DirExists(fs, tt.searchDir)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWriteYaml(t *testing.T) {
	t.Parallel()

	type args struct {
		path    string
		content interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "write yaml file",
			args: args{
				path: "path/to/file.yml",
				content: map[string]interface{}{
					"key": "value",
					"key2": map[string]interface{}{
						"key3": "value3",
					},
				},
			},
			want:    "key: value\nkey2:\n    key3: value3\n",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewMemMapFs()
			tt.wantErr(t, WriteYaml(fs, tt.args.path, tt.args.content))

			file, err := afero.ReadFile(fs, tt.args.path)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, string(file))
		})
	}
}
