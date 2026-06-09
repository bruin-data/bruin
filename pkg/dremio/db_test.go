package dremio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFolderPathForAsset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		asset string
		want  string
	}{
		{name: "folder.table", asset: "my_folder.my_table", want: "my_folder"},
		{name: "deeper paths are not supported (flat only)", asset: "my_source.my_folder.my_table", want: ""},
		{name: "bare table has no folder", asset: "my_table", want: ""},
		{name: "empty name", asset: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, folderPathForAsset(tt.asset))
		})
	}
}
