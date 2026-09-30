// Package ssis imports a conservative subset of SSIS and SQL Server Agent exports.
package ssis

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
)

type ImportOptions struct {
	SourcePath   string
	PipelinePath string
	Connection   string
	Overwrite    bool
}

type ImportResult struct {
	SQLAssets       int
	PythonAssets    int
	SkippedAssets   int
	PipelineCreated bool
}

type task struct {
	id       string
	name     string
	code     string
	python   bool
	upstream []string
}

type workflow struct {
	name  string
	tasks []task
}

var unsafeName = regexp.MustCompile(`[^a-z0-9_]+`)

func safeName(s string) string {
	s = strings.Trim(unsafeName.ReplaceAllString(strings.ToLower(s), "_"), "_")
	if s == "" {
		return "imported"
	}
	return s
}

// Import validates every input before writing. It never executes source commands
// or copies connection-manager credentials into the destination.
func Import(ctx context.Context, fs afero.Fs, opts ImportOptions) (*ImportResult, error) {
	if opts.SourcePath == "" || opts.PipelinePath == "" {
		return nil, fmt.Errorf("source and pipeline paths are required")
	}
	var files []string
	err := afero.Walk(fs, opts.SourcePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && (strings.EqualFold(filepath.Ext(path), ".dtsx") || strings.EqualFold(filepath.Ext(path), ".json")) {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no .dtsx or SQL Server Agent .json exports found")
	}
	var assets []*pipeline.Asset
	names := map[string]bool{}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := afero.ReadFile(fs, file)
		if err != nil {
			return nil, err
		}
		var workflows []workflow
		if strings.EqualFold(filepath.Ext(file), ".dtsx") {
			var w workflow
			w, err = parsePackage(fs, file, data)
			workflows = []workflow{w}
		} else {
			workflows, err = parseJobs(fs, file, data)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", file, err)
		}
		for _, w := range workflows {
			if err := validateGraph(w); err != nil {
				return nil, fmt.Errorf("%s: %w", file, err)
			}
			byID := map[string]string{}
			for _, t := range w.tasks {
				name := safeName(w.name) + "." + safeName(t.name)
				if names[name] {
					return nil, fmt.Errorf("duplicate generated asset name %q; rename the source jobs or tasks", name)
				}
				names[name] = true
				byID[t.id] = name
			}
			for _, t := range w.tasks {
				if strings.TrimSpace(t.code) == "" || strings.Contains(t.code, "@bruin") {
					return nil, fmt.Errorf("task %q has empty code or an existing Bruin header", t.name)
				}
				assetType, ext := pipeline.AssetType("ms.sql"), ".sql"
				connection := opts.Connection
				if t.python {
					assetType, ext, connection = pipeline.AssetType("python"), ".py", ""
				}
				a := &pipeline.Asset{
					Name: byID[t.id], Type: assetType, Connection: connection,
					ExecutableFile: pipeline.ExecutableFile{
						Path: filepath.Join(opts.PipelinePath, "assets", byID[t.id]+ext), Content: t.code,
					},
				}
				for _, id := range t.upstream {
					a.Upstreams = append(a.Upstreams, pipeline.Upstream{Type: "asset", Value: byID[id]})
				}
				assets = append(assets, a)
			}
		}
	}
	if len(assets) == 0 {
		return nil, fmt.Errorf("no supported tasks found")
	}
	if err := fs.MkdirAll(filepath.Join(opts.PipelinePath, "assets"), 0o755); err != nil {
		return nil, err
	}
	result := &ImportResult{}
	pipelineFile := filepath.Join(opts.PipelinePath, "pipeline.yml")
	exists, err := afero.Exists(fs, pipelineFile)
	if err != nil {
		return nil, err
	}
	if !exists {
		p := &pipeline.Pipeline{Name: safeName(filepath.Base(filepath.Clean(opts.PipelinePath))), DefinitionFile: pipeline.DefinitionFile{Path: pipelineFile}}
		if err := p.Persist(fs); err != nil {
			return nil, err
		}
		result.PipelineCreated = true
	}
	for _, asset := range assets {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		exists, err := afero.Exists(fs, asset.ExecutableFile.Path)
		if err != nil {
			return nil, err
		}
		if exists && !opts.Overwrite {
			result.SkippedAssets++
			continue
		}
		if err := asset.Persist(fs); err != nil {
			return nil, err
		}
		if asset.Type == "python" {
			result.PythonAssets++
		} else {
			result.SQLAssets++
		}
	}
	return result, nil
}

func validateGraph(w workflow) error {
	byID := map[string]task{}
	for _, t := range w.tasks {
		if _, exists := byID[t.id]; exists || t.id == "" {
			return fmt.Errorf("workflow %q has duplicate or missing task IDs", w.name)
		}
		byID[t.id] = t
	}
	state := map[string]int{}
	var visit func(string) error
	visit = func(id string) error {
		t, exists := byID[id]
		if !exists {
			return fmt.Errorf("unresolved precedence reference %q", id)
		}
		if state[id] == 1 {
			return fmt.Errorf("workflow %q contains a cycle", w.name)
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		for _, upstream := range t.upstream {
			if err := visit(upstream); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	for _, t := range w.tasks {
		if err := visit(t.id); err != nil {
			return err
		}
	}
	return nil
}

// Only a single relative script path is accepted, not an arbitrary shell command.
// The export and its scripts must be staged together before import.
func pythonScript(fs afero.Fs, source, executable, arguments string) (string, error) {
	exe := strings.ToLower(filepath.Base(strings.ReplaceAll(executable, `\`, "/")))
	if exe != "python" && exe != "python.exe" && exe != "python3" && exe != "python3.exe" {
		return "", fmt.Errorf("only Python Execute Process tasks are supported")
	}
	path := strings.TrimSpace(arguments)
	if len(path) >= 2 && path[0] == '"' && path[len(path)-1] == '"' {
		path = path[1 : len(path)-1]
	}
	path = strings.ReplaceAll(path, `\`, "/")
	if !filepath.IsLocal(path) || strings.ContainsAny(path, ":\"\r\n") || !strings.HasSuffix(path, ".py") {
		return "", fmt.Errorf("Python arguments must be a relative .py path without script arguments")
	}
	data, err := afero.ReadFile(fs, filepath.Join(filepath.Dir(source), path))
	if err != nil {
		return "", fmt.Errorf("read Python script: %w", err)
	}
	return string(data), nil
}
