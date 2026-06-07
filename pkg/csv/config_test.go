package csv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "relative path",
			cfg: Config{
				Path: "data/users.csv",
			},
			want: "csv://data/users.csv",
		},
		{
			name: "absolute path",
			cfg: Config{
				Path: "/tmp/data/users.csv",
			},
			want: "csv:///tmp/data/users.csv",
		},
		{
			name: "trims path",
			cfg: Config{
				Path: "  data/users.csv  ",
			},
			want: "csv://data/users.csv",
		},
		{
			name: "encoding",
			cfg: Config{
				Path:     "/tmp/data/users.csv",
				Encoding: "windows-1252",
			},
			want: "csv:///tmp/data/users.csv?encoding=windows-1252",
		},
		{
			name: "layout",
			cfg: Config{
				Path:   "/tmp/exports",
				Layout: "{table_name}.{ext}",
			},
			want: "csv:///tmp/exports?layout=%7Btable_name%7D.%7Bext%7D",
		},
		{
			name: "trims query values",
			cfg: Config{
				Path:     "  data/users.csv  ",
				Encoding: "  latin1  ",
				Layout:   "  {table_name}.{ext}  ",
			},
			want: "csv://data/users.csv?encoding=latin1&layout=%7Btable_name%7D.%7Bext%7D",
		},
		{
			name: "empty path",
			cfg:  Config{},
			want: "csv://",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, tt.cfg.GetIngestrURI())
		})
	}
}
