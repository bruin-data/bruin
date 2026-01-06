// Package spark provides shared utilities for Spark-based execution environments
// such as EMR Serverless and Dataproc Serverless.
package spark

import (
	"archive/zip"
	"io"
	"io/fs"
	"path"
	"path/filepath"
	"regexp"
	"slices"
)

// BruinExcludes contains files that should be excluded from Spark context packages.
var BruinExcludes = []string{
	"README.md",
	".bruin.yml",
	"pipeline.yml",
	"pipeline.yaml",
}

// DirExcludes contains regex patterns for directories that should be excluded from Spark context packages.
var DirExcludes = []*regexp.Regexp{
	regexp.MustCompile(`(^|[/\\])\.venv([/\\]|$)`),
	regexp.MustCompile(`(^|[/\\])venv([/\\]|$)`),
	regexp.MustCompile(`^logs([/\\]|$)`),
	regexp.MustCompile(`^\.git([/\\]|$)`),
}

// Exclude returns true if the given path should be excluded from packaging.
func Exclude(path string) bool {
	fileName := filepath.Base(path)
	if slices.Contains(BruinExcludes, fileName) {
		return true
	}

	for _, re := range DirExcludes {
		if re.MatchString(path) {
			return true
		}
	}
	return false
}

// PackageContext creates a zip archive from the given filesystem, suitable for Spark execution.
// It's a modified version of zip.AddFS() with:
//   - Exclusion of Bruin configuration files and virtual environments
//   - Automatic creation of __init__.py files in directories for Python package support
//
// Spark requires directories to contain __init__.py to be treated as packages.
func PackageContext(zw *zip.Writer, context fs.FS) error {
	return fs.WalkDir(context, ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if Exclude(name) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			// spark will refuse to treat a directory as a package
			// if it doesn't contain __init__.py
			zw.CreateHeader(&zip.FileHeader{ //nolint
				Name: path.Join(name, "__init__.py"),
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
		h.Name = name
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
