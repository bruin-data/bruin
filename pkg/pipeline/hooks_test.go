package pipeline

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWrapHooks_TrimsAndSkipsEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		hooks Hooks
		want  string
	}{
		{
			name:  "trims and skips empty",
			query: "  select 9  ",
			hooks: Hooks{
				Pre:  []Hook{{Query: ""}, {Query: "select 1;"}},
				Post: []Hook{{Query: "  select 2  "}},
			},
			want: "select 1;\nselect 9;\nselect 2;",
		},
		{
			name:  "no hooks returns original query",
			query: "select 1",
			hooks: Hooks{},
			want:  "select 1",
		},
		{
			name:  "only pre hooks",
			query: "select 2",
			hooks: Hooks{
				Pre: []Hook{{Query: "select 1"}},
			},
			want: "select 1;\nselect 2;",
		},
		{
			name:  "only post hooks",
			query: "select 2",
			hooks: Hooks{
				Post: []Hook{{Query: "select 3"}},
			},
			want: "select 2;\nselect 3;",
		},
		{
			name:  "empty main query",
			query: " ",
			hooks: Hooks{
				Pre:  []Hook{{Query: "select 1"}},
				Post: []Hook{{Query: "select 3"}},
			},
			want: "select 1;\nselect 3;",
		},
		{
			name:  "preserves semicolons",
			query: "select 2;",
			hooks: Hooks{
				Pre:  []Hook{{Query: "select 1;"}},
				Post: []Hook{{Query: "select 3;"}},
			},
			want: "select 1;\nselect 2;\nselect 3;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, WrapHooks(tt.query, tt.hooks))
		})
	}
}
