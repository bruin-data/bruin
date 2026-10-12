package user

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

// copyHomeFiles merges a legacy directory without replacing destination files.
// Files are published only after a successful copy, so failed copies can retry.
func copyHomeFiles(fs afero.Fs, source, destination string) error {
	if _, err := fs.Stat(source); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if fs.Name() == "OsFs" {
		var err error
		source, err = filepath.EvalSymlinks(source)
		if err != nil {
			return err
		}
		if resolved, err := filepath.EvalSymlinks(destination); err == nil {
			destination = resolved
		}
	}
	if source == destination {
		return nil
	}
	return afero.Walk(fs, source, func(name string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// BRUIN_HOME may be a subdirectory of the old home.
		if name == destination {
			return filepath.SkipDir
		}
		// Only the destination may mark its own migration complete.
		if name == filepath.Join(source, homeMigrationMarker) {
			return nil
		}
		rel, err := filepath.Rel(source, name)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, rel)
		existing, err := lstat(fs, target)
		if err == nil {
			if info.IsDir() && !existing.IsDir() {
				return filepath.SkipDir
			}
			if !info.IsDir() {
				return nil
			}
		} else if !os.IsNotExist(err) {
			return err
		}
		if info.IsDir() {
			// The old home may be read-only; new tool directories must be writable.
			return fs.MkdirAll(target, info.Mode().Perm()|0o700)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			links, ok := fs.(afero.LinkReader)
			if !ok {
				return fmt.Errorf("cannot read symlink %s", name)
			}
			link, err := links.ReadlinkIfPossible(name)
			if err != nil {
				return err
			}
			if filepath.IsAbs(link) {
				if rel, err := filepath.Rel(source, link); err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
					link = filepath.Join(destination, rel)
				}
			}
			err = fs.(afero.Linker).SymlinkIfPossible(link, target)
			if os.IsExist(err) {
				return nil
			}
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		return copyHomeFile(fs, name, target, info.Mode().Perm())
	})
}

func lstat(fs afero.Fs, name string) (os.FileInfo, error) {
	if fs, ok := fs.(afero.Lstater); ok {
		info, _, err := fs.LstatIfPossible(name)
		return info, err
	}
	return fs.Stat(name)
}

func copyHomeFile(fs afero.Fs, source, destination string, mode os.FileMode) error {
	in, err := fs.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := afero.TempFile(fs, filepath.Dir(destination), ".migrate-*")
	if err != nil {
		return err
	}
	defer func() { _ = fs.Remove(out.Name()) }()
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := fs.Chmod(out.Name(), mode); err != nil {
		return err
	}
	if fs.Name() == "OsFs" {
		// Publish the completed file atomically without replacing a destination
		// created by another process while we were copying. The temporary file
		// is in the same directory, so the hard link stays on one filesystem.
		err := os.Link(out.Name(), destination)
		if os.IsExist(err) {
			return nil
		}
		return err
	}
	// In-memory filesystems are protected by homeMigrationMu.
	return fs.Rename(out.Name(), destination)
}
