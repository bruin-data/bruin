package git

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRepo(t *testing.T) {
	t.Parallel()

	absPath := func(path string) string {
		abs, err := filepath.Abs(path)
		assert.NoError(t, err)
		return abs
	}

	tests := []struct {
		name    string
		path    string
		want    *Repo
		wantErr bool
	}{
		{
			name:    "no repo exists",
			path:    "/",
			wantErr: true,
		},
		{
			name: "can find its own repo root",
			path: ".",
			want: &Repo{
				Path: absPath("../../."),
			},
		},
		{
			name: "can find its own repo root even if a file is given",
			path: "./root_test.go",
			want: &Repo{
				Path: absPath("../../."),
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			finder := &RepoFinder{}
			got, err := finder.Repo(tt.path)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
