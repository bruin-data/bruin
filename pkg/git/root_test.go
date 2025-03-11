package git

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRepo(t *testing.T) {
	t.Parallel()

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
				Path: path.AbsPathForTests(t, "../../."),
			},
		},
		{
			name: "can find its own repo root even if a file is given",
			path: "./root_test.go",
			want: &Repo{
				Path: path.AbsPathForTests(t, "../../."),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := FindRepoFromPath(tt.path)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func BenchmarkFindRepoFromPath(b *testing.B) {
	// Reset the timer to exclude setup time
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		_, err := FindRepoFromPath(".")
		if err != nil {
			b.Fatal(err)
		}
	}
}
