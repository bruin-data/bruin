package semantic

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/bruin-data/bruin/schemas"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

func parseFileFS(fs afero.Fs, path string) (*Model, error) {
	data, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, fmt.Errorf("reading semantic model: %w", err)
	}
	if err := schemas.ValidateYAML(schemas.SemanticModelV1ID, data); err != nil {
		return nil, err
	}

	var model Model
	if err := yaml.Unmarshal(data, &model); err != nil {
		return nil, fmt.Errorf("parsing semantic model YAML: %w", err)
	}
	if model.Schema == "" {
		model.Schema = schemas.SemanticModelV1ID
	}

	return &model, nil
}

// LoadFile loads and validates a semantic model from a YAML file.
func LoadFile(path string) (*Model, error) {
	return LoadFileFS(afero.NewOsFs(), path)
}

// LoadFileFS loads and validates a semantic model from a YAML file using the given filesystem.
func LoadFileFS(fs afero.Fs, path string) (*Model, error) {
	model, err := parseFileFS(fs, path)
	if err != nil {
		return nil, err
	}

	if _, err := NewEngine(model); err != nil {
		return nil, err
	}

	return model, nil
}

// LoadDir loads every *.yml/*.yaml semantic model from a directory.
func LoadDir(dir string) (map[string]*Model, error) {
	return LoadDirFS(afero.NewOsFs(), dir)
}

// LoadDirFS loads every *.yml/*.yaml semantic model from a directory using the given filesystem.
func LoadDirFS(fs afero.Fs, dir string) (map[string]*Model, error) {
	if dir == "" {
		return map[string]*Model{}, nil
	}

	entries, err := afero.ReadDir(fs, dir)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]*Model{}, nil
		}
		return nil, fmt.Errorf("reading semantic directory: %w", err)
	}

	models := make(map[string]*Model)
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") || !isYAMLFile(entry.Name()) {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		model, err := LoadFileFS(fs, path)
		if err != nil {
			return nil, fmt.Errorf("loading %s: %w", entry.Name(), err)
		}
		if _, exists := models[model.Name]; exists {
			return nil, fmt.Errorf("duplicate semantic model name %q", model.Name)
		}
		models[model.Name] = model
	}

	return models, nil
}

// LoadDirPartial loads valid semantic models while keeping track of invalid
// named models so callers can fail only dashboards that reference them.
func LoadDirPartial(dir string) (map[string]*Model, map[string]error, error) {
	return LoadDirPartialFS(afero.NewOsFs(), dir)
}

// LoadDirPartialFS loads valid semantic models using the given filesystem while keeping track of invalid
// named models so callers can fail only dashboards that reference them.
func LoadDirPartialFS(fs afero.Fs, dir string) (map[string]*Model, map[string]error, error) {
	if dir == "" {
		return map[string]*Model{}, map[string]error{}, nil
	}

	entries, err := afero.ReadDir(fs, dir)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]*Model{}, map[string]error{}, nil
		}
		return nil, nil, fmt.Errorf("reading semantic directory: %w", err)
	}

	models := make(map[string]*Model)
	invalid := make(map[string]error)
	for _, entry := range entries {
		if entry.IsDir() || strings.HasPrefix(entry.Name(), ".") || !isYAMLFile(entry.Name()) {
			continue
		}

		path := filepath.Join(dir, entry.Name())
		model, err := parseFileFS(fs, path)
		if err != nil {
			continue
		}
		if model.Name == "" {
			continue
		}

		if _, exists := models[model.Name]; exists {
			delete(models, model.Name)
			invalid[model.Name] = fmt.Errorf("duplicate semantic model name %q", model.Name)
			continue
		}
		if _, exists := invalid[model.Name]; exists {
			invalid[model.Name] = fmt.Errorf("duplicate semantic model name %q", model.Name)
			continue
		}

		if _, err := NewEngine(model); err != nil {
			invalid[model.Name] = fmt.Errorf("loading %s: %w", entry.Name(), err)
			continue
		}

		models[model.Name] = model
	}

	return models, invalid, nil
}

func Names(models map[string]*Model) []string {
	names := make([]string, 0, len(models))
	for name := range models {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func isYAMLFile(name string) bool {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".yml", ".yaml":
		return true
	default:
		return false
	}
}
