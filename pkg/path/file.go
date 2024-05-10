package path

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

type YamlParseError struct {
	msg string
}

func (e *YamlParseError) Error() string {
	return e.msg
}

func ReadYaml(fs afero.Fs, path string, out interface{}) error {
	buf, err := afero.ReadFile(fs, path)
	if err != nil {
		return errors.Wrapf(err, "failed to read file %s", path)
	}

	return ConvertYamlToObject(buf, out)
}

func WriteYaml(fs afero.Fs, path string, content interface{}) error {
	buf, err := yaml.Marshal(content)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal object to yaml")
	}

	err = afero.WriteFile(fs, path, buf, 0o644)
	if err != nil {
		return errors.Wrapf(err, "failed to write YAML file to %s", path)
	}

	return nil
}

func ConvertYamlToObject(buf []byte, out interface{}) error {
	err := yaml.Unmarshal(buf, out)
	if err != nil {
		return &YamlParseError{msg: err.Error()}
	}

	validate := validator.New()

	err = validate.Struct(out)
	if err != nil {
		return err
	}

	return nil
}

// ExcludeSubItemsInDirectoryContainingFile cleans up the list to remove sub-paths that are in the same directory as
// the file. The primary usage of this is to remove the sub-paths for the directory that contains `task.yml`.
func ExcludeSubItemsInDirectoryContainingFile(filePaths []string, file string) []string {
	result := make([]string, 0, len(filePaths))
	var targetsToRemove []string

	for _, path := range filePaths {
		if strings.HasSuffix(path, file) {
			targetsToRemove = append(targetsToRemove, filepath.Dir(path))
		}
	}

	for _, path := range filePaths {
		shouldBeIncluded := true
		for _, target := range targetsToRemove {
			if strings.HasPrefix(path, target) && path != filepath.Join(target, file) {
				shouldBeIncluded = false
				break
			}
		}

		if shouldBeIncluded {
			result = append(result, path)
		}
	}

	return result
}

func DirExists(fs afero.Fs, searchDir string) bool {
	res, err := afero.DirExists(fs, searchDir)
	return err == nil && res
}

func FileExists(fs afero.Fs, searchFile string) bool {
	res, err := afero.Exists(fs, searchFile)
	return err == nil && res
}

func AbsPathForTests(t *testing.T, path string) string {
	t.Helper()

	pathFields := strings.Split(path, "/")
	absolutePath, err := filepath.Abs(filepath.Join(pathFields...))
	if err != nil {
		t.Fatalf("failed to get absolute path for %s: %v", path, err)
	}

	return absolutePath
}
