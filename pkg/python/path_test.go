package python

import (
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestFindModulePath(t *testing.T) {
	t.Parallel()

	type args struct {
		repo       *git.Repo
		executable *pipeline.ExecutableFile
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{
			name: "the executable is in a different path",
			args: args{
				repo: &git.Repo{
					Path: "/Users/robin/Projects/my-pipeline",
				},
				executable: &pipeline.ExecutableFile{
					Path: "/Users/robin/Projects/other-project/pipeline1/tasks/my-module/script.py",
				},
			},
			wantErr: true,
		},
		{
			name: "can find the module path",
			args: args{
				repo: &git.Repo{
					Path: "/Users/robin/Projects/my-pipeline",
				},
				executable: &pipeline.ExecutableFile{
					Path: "/Users/robin/Projects/my-pipeline/pipeline1/tasks/my-module/script.py",
				},
			},
			want: "pipeline1.tasks.my-module.script",
		},
		{
			name: "can find the module path even with indirect directory references",
			args: args{
				repo: &git.Repo{
					Path: "/Users/robin/Projects/my-pipeline",
				},
				executable: &pipeline.ExecutableFile{
					Path: "/Users/robin/Projects/my-pipeline/../../Projects/my-pipeline/pipeline1/tasks/my-module/script.py",
				},
			},
			want: "pipeline1.tasks.my-module.script",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			finder := &ModulePathFinder{}
			got, err := finder.FindModulePath(tt.args.repo, tt.args.executable)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestFindRequirementsTxt(t *testing.T) {
	t.Parallel()

	abs := func(path string) string {
		absPath, err := filepath.Abs(path)
		assert.NoError(t, err)
		return absPath
	}

	repoPath := abs("./testdata/reqfinder")

	type args struct {
		repo       *git.Repo
		executable *pipeline.ExecutableFile
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			name: "the reqs file is next to the script",
			args: args{
				repo: &git.Repo{
					Path: repoPath,
				},
				executable: &pipeline.ExecutableFile{
					Path: abs("./testdata/reqfinder/dir1/dir2/dir3/main.py"),
				},
			},
			want:    abs("./testdata/reqfinder/dir1/dir2/dir3/requirements.txt"),
			wantErr: assert.NoError,
		},
		{
			name: "the reqs file is in the parent folder",
			args: args{
				repo: &git.Repo{
					Path: repoPath,
				},
				executable: &pipeline.ExecutableFile{
					Path: abs("./testdata/reqfinder/dir1/dir2/task2.py"),
				},
			},
			want:    abs("./testdata/reqfinder/dir1/requirements.txt"),
			wantErr: assert.NoError,
		},
		{
			name: "deeper nested files go up the tree as well",
			args: args{
				repo: &git.Repo{
					Path: repoPath,
				},
				executable: &pipeline.ExecutableFile{
					Path: abs("./testdata/reqfinder/dir1/dir22/dir11/dir11/main.py"),
				},
			},
			want:    abs("./testdata/reqfinder/dir1/requirements.txt"),
			wantErr: assert.NoError,
		},
		{
			name: "no requirements.txt file found",
			args: args{
				repo: &git.Repo{
					Path: repoPath,
				},
				executable: &pipeline.ExecutableFile{
					Path: abs("./testdata/reqfinder/main.py"),
				},
			},
			want: "",
			wantErr: func(t assert.TestingT, err error, msgAndArgs ...interface{}) bool {
				_, ok := err.(*NoRequirementsFoundError) //nolint:errorlint
				return ok
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			finder := &ModulePathFinder{}
			got, err := finder.FindRequirementsTxt(tt.args.repo, tt.args.executable)

			tt.wantErr(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
