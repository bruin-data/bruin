//go:build !windows

package backfill

import (
	"os"
	"path/filepath"
)

// syncParent makes the directory entry created by an atomic rename durable.
func syncParent(path string) error {
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
