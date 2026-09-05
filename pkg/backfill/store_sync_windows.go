//go:build windows

package backfill

// Windows does not expose POSIX-style directory fsync through os.File.Sync.
func syncParent(string) error {
	return nil
}
