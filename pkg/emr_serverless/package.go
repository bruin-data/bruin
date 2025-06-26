package emr_serverless //nolint

import (
	"archive/zip"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"slices"
)

var bruinExcludes = []string{
	"README.md",
	".bruin.yml",
	"pipeline.yml",
	"pipeline.yaml",
}
var venvExcludes = []*regexp.Regexp{
	regexp.MustCompile(`[/\\].venv[/\\]`),
	regexp.MustCompile(`[/\\]venv[/\\]`),
}

func exclude(path string) bool {
	fileName := filepath.Base(path)
	if slices.Contains(bruinExcludes, fileName) {
		return true
	}

	for _, re := range venvExcludes {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

// zip.AddFS() modified with support for filesystem prefix
// and some spark specific adjustments.
func packageContextWithPrefix(zw *zip.Writer, context fs.FS, prefix string) error {
	// todo(turtledev): exclude assets from the packaged zip.
	// this shouldn't create any issues in most cases, but
	// adding this will fool-proof our implementation
	return fs.WalkDir(context, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fullPath := path.Join(prefix, name)
		if exclude(fullPath) {
			return nil
		}
		if d.IsDir() {
			// spark will refuse to treat a directory as a package
			// if it doesn't contain __init__.py
			zw.CreateHeader(&zip.FileHeader{ //nolint
				Name: path.Join(fullPath, "__init__.py"),
			})
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		h, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		h.Name = fullPath
		h.Method = zip.Deflate
		fw, err := zw.CreateHeader(h)
		if err != nil {
			return err
		}
		f, err := context.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(fw, f)
		return err
	})
}
