package user

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/gofrs/flock"
	"github.com/spf13/afero"
)

const homeMigrationLock = ".home-migration.lock"

// Keep the lockfile in place: unlinking it would let waiters lock different
// inodes. Memory filesystems are already protected by homeMigrationMu.
func lockHomeMigration(fs afero.Fs, home string) (func(), error) {
	if fs.Name() != "OsFs" {
		return func() {}, nil
	}
	lock := flock.New(filepath.Join(home, homeMigrationLock))
	if err := lock.Lock(); err != nil {
		return nil, fmt.Errorf("failed to lock bruin home migration: %w", err)
	}
	return func() { _ = lock.Unlock() }, nil
}

// copyHomeFiles merges a legacy directory without replacing destination files.
// Callers hold the home migration lock until all files and markers are ready.
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
		// Migration state and locks belong to the destination itself.
		if name == filepath.Join(source, homeMigrationMarker) || name == filepath.Join(source, homeMigrationLock) {
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
	temporary := out.Name()
	defer func() { _ = fs.Remove(temporary) }()
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := fs.Chmod(temporary, mode); err != nil {
		return err
	}
	if fs.Name() == "OsFs" {
		// Publish the completed file atomically without replacing a destination
		// created by another process while we were copying.
		err := os.Link(temporary, destination)
		if err == nil || os.IsExist(err) {
			return nil
		}
	}
	// Filesystems such as exFAT cannot create hardlinks. The shared home lock
	// serializes Bruin migrations, so recheck the destination and rename the
	// completed file. Never copy into the final path: interruption would leave
	// a partial tool that the next run mistakes for an existing installation.
	if _, err := lstat(fs, destination); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return fs.Rename(temporary, destination)
}
