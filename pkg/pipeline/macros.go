package pipeline

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/spf13/afero"
)

// LoadMacros reads SQL macro files without following symlinks. recursive controls
// whether subdirectories are loaded. Missing macro directories are optional.
// Filesystems that hide symlink metadata (such as caches) must not be used here.
func LoadMacros(filesystem afero.Fs, directory string, recursive bool) ([]Macro, error) {
	info, err := filesystem.Stat(directory)
	if os.IsNotExist(err) {
		return []Macro{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []Macro{}, nil
	}

	var source fs.FS
	switch filesystem.(type) {
	case *afero.OsFs:
		absolute, err := filepath.Abs(directory)
		if err != nil {
			return nil, err
		}
		// Check ancestors too: a regular macros directory can live beneath a
		// symlinked pipeline directory. Lstat on the final path alone misses it.
		for current := absolute; ; current = filepath.Dir(current) {
			info, err := os.Lstat(current)
			if err != nil {
				return nil, err
			}
			if info.Mode()&os.ModeSymlink != 0 {
				return nil, fmt.Errorf("symlinks are not allowed in macro paths: %s", current)
			}
			if filepath.Dir(current) == current {
				break
			}
		}
		root, err := os.OpenRoot(absolute)
		if err != nil {
			return nil, err
		}
		defer root.Close()
		// Root also prevents escapes if a file is replaced with a symlink
		// between directory enumeration and the read.
		source = root.FS()
	case *afero.MemMapFs:
		// MemMapFs has no symlinks and is used by in-memory pipeline builders.
		source = afero.NewIOFS(afero.NewBasePathFs(filesystem, directory))
	default:
		return nil, fmt.Errorf("macro loading requires an uncached OS or memory filesystem, got %T", filesystem)
	}

	macros := []Macro{}
	err = fs.WalkDir(source, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("symlinks are not allowed in macro paths: %s", name)
		}
		if entry.IsDir() {
			if name != "." && (!recursive || slices.Contains(path.SkipDirs, entry.Name())) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".sql") {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("macro must be a regular file: %s", name)
		}
		content, err := fs.ReadFile(source, name)
		if err != nil {
			return fmt.Errorf("failed to read macro file %s: %w", name, err)
		}
		macros = append(macros, Macro(content))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return macros, nil
}
