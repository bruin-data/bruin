package ssis

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
)

// Check both identity and location before writing anything: a file can only be
// skipped when it declares the expected asset, and overwrite grants permission
// to replace generated paths, not to delete same-named assets elsewhere.
func validateDestination(fs afero.Fs, opts ImportOptions, assets []*pipeline.Asset) error {
	directory := filepath.Join(opts.PipelinePath, "assets")
	byPath, byName := map[string]string{}, map[string]string{}
	for _, asset := range assets {
		path := filepath.Clean(asset.ExecutableFile.Path)
		byPath[path] = asset.Name
		byName[asset.Name] = path
	}
	return afero.Walk(fs, directory, func(path string, info os.FileInfo, err error) error {
		if path == directory && errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("destination symlink %s must be removed before importing", path)
		}
		expected := byPath[path]
		if info.IsDir() {
			if expected != "" {
				return fmt.Errorf("asset output path %s is a directory", path)
			}
			return nil
		}
		if expected != "" && opts.Overwrite {
			return nil
		}
		var asset *pipeline.Asset
		if strings.HasSuffix(path, "asset.yml") || strings.HasSuffix(path, "asset.yaml") || strings.HasSuffix(path, "task.yml") || strings.HasSuffix(path, "task.yaml") {
			data, readErr := afero.ReadFile(fs, path)
			if readErr != nil {
				return readErr
			}
			asset, err = pipeline.ConvertYamlToTask(data)
		} else {
			asset, err = pipeline.CreateTaskFromFileComments(fs)(path)
		}
		if err != nil {
			return fmt.Errorf("validate existing asset %s: %w", path, err)
		}
		name := ""
		if asset != nil {
			name = asset.Name
			if name == "" {
				asset.DefinitionFile.Path = path
				name, err = asset.GetNameIfItWasSetFromItsPath(&pipeline.Pipeline{DefinitionFile: pipeline.DefinitionFile{Path: filepath.Join(opts.PipelinePath, "pipeline.yml")}})
				if err != nil {
					return err
				}
			}
		}
		if expected != "" && name != expected {
			return fmt.Errorf("existing file %s declares asset %q, expected %q; use --overwrite to replace it", path, name, expected)
		}
		if target, exists := byName[name]; exists && target != path {
			return fmt.Errorf("existing asset %q at %s conflicts with generated path %s", name, path, target)
		}
		return nil
	})
}

// Never open an existing destination for truncation: it may have become a
// symlink or hard link since preflight. Exclusive creation protects new files;
// rename replaces an overwritten directory entry without following that link.
func writeRootFile(root *os.Root, path string, content []byte, overwrite bool) error {
	writePath := path
	if overwrite {
		writePath = filepath.Join(filepath.Dir(path), ".bruin-import-"+rand.Text())
		defer func() { _ = root.Remove(writePath) }()
	}
	file, err := root.OpenFile(writePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, err = file.Write(content)
	if err := errors.Join(err, file.Close()); err != nil {
		return err
	}
	if overwrite {
		return root.Rename(writePath, path)
	}
	return nil
}
