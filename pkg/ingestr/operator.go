package ingestr

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

type ingestrRunner interface {
	RunIngestr(ctx context.Context, args, extraPackages []string, repo *git.Repo) error
}

type BasicOperator struct {
	conn          config.ConnectionGetter
	runner        ingestrRunner
	finder        repoFinder
	jinjaRenderer jinja.RendererInterface
}

type SeedOperator struct {
	conn          config.ConnectionGetter
	runner        ingestrRunner
	finder        repoFinder
	jinjaRenderer jinja.RendererInterface
}

type pipelineConnection interface {
	GetIngestrURI() (string, error)
}

func NewBasicOperator(conn config.ConnectionGetter, j jinja.RendererInterface) (*BasicOperator, error) {
	uvRunner := &python.UvPythonRunner{
		UvInstaller: &python.UvChecker{},
		Cmd:         &python.CommandRunner{},
	}

	return &BasicOperator{conn: conn, runner: uvRunner, finder: &git.RepoFinder{}, jinjaRenderer: j}, nil
}

func (o *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	var extraPackages []string

	asset := ti.GetAsset()

	// Render interval modifier templates if they exist
	if asset.IntervalModifiers.Start.Template != "" {
		renderedStart, err := asset.IntervalModifiers.Start.ResolveTemplateToNew(o.jinjaRenderer)
		if err != nil {
			return fmt.Errorf("failed to render start interval modifier template: %w", err)
		}
		asset.IntervalModifiers.Start = renderedStart
	}

	if asset.IntervalModifiers.End.Template != "" {
		renderedEnd, err := asset.IntervalModifiers.End.ResolveTemplateToNew(o.jinjaRenderer)
		if err != nil {
			return fmt.Errorf("failed to render end interval modifier template: %w", err)
		}
		asset.IntervalModifiers.End = renderedEnd
	}

	// Source connection
	sourceConnectionName, ok := asset.Parameters["source_connection"]
	if !ok {
		return errors.New("source connection not configured")
	}

	sourceConnection := o.conn.GetConnection(sourceConnectionName)
	if sourceConnection == nil {
		return errors.Errorf("source connection %s not found", sourceConnectionName)
	}

	sourceURI, err := sourceConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return fmt.Errorf("could not get the source uri: %w", err)
	}

	if sourceURI == "" {
		return errors.New("source uri is empty, which means the source connection is not configured correctly")
	}

	// some connection types can be shared among sources, therefore inferring source URI from the connection type is not
	// always feasible. In the case of GSheets, we have to reuse the same GCP credentials, but change the prefix with gsheets://
	if asset.Parameters["source"] == "gsheets" {
		sourceURI = strings.ReplaceAll(sourceURI, "bigquery://", "gsheets://")
	}

	sourceTable, ok := asset.Parameters["source_table"]
	if !ok {
		return errors.New("source table not configured")
	}

	fileType, ok := asset.Parameters["file_type"]
	if ok {
		sourceTable = sourceTable + "#" + fileType
	}

	destConnectionName, err := ti.GetPipeline().GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	destConnection := o.conn.GetConnection(destConnectionName)
	if destConnection == nil {
		return errors.Errorf("destination connection %s not found", destConnectionName)
	}

	destURI, err := destConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return errors.Wrap(err, "could not get the destination uri")
	}

	if destURI == "" {
		return errors.New("destination uri is empty, which means the connection is not configured correctly")
	}

	destTable := asset.Name

	extraPackages = python.AddExtraPackages(destURI, sourceURI, extraPackages)

	cmdArgs, err := python.ConsolidatedParameters(ctx, asset, []string{
		"ingest",
		"--source-uri",
		sourceURI,
		"--source-table",
		sourceTable,
		"--dest-uri",
		destURI,
		"--dest-table",
		destTable,
		"--yes",
		"--progress",
		"log",
	}, &python.ColumnHintOptions{
		NormalizeColumnNames:   false,
		EnforceSchemaByDefault: false,
	})
	if err != nil {
		return err
	}

	path := asset.ExecutableFile.Path
	repo, err := o.finder.Repo(path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Ingestr")
	}

	if strings.HasPrefix(destURI, "duckdb://") {
		duck.LockDatabase(destURI)
		defer duck.UnlockDatabase(destURI)
	}

	if strings.HasPrefix(sourceURI, "duckdb://") && sourceURI != destURI {
		duck.LockDatabase(sourceURI)
		defer duck.UnlockDatabase(sourceURI)
	}

	return o.runner.RunIngestr(ctx, cmdArgs, extraPackages, repo)
}

func NewSeedOperator(conn config.ConnectionGetter, j jinja.RendererInterface) (*SeedOperator, error) {
	uvRunner := &python.UvPythonRunner{
		UvInstaller: &python.UvChecker{},
		Cmd:         &python.CommandRunner{},
	}

	return &SeedOperator{
		conn:          conn,
		runner:        uvRunner,
		finder:        &git.RepoFinder{},
		jinjaRenderer: j,
	}, nil
}

func (o *SeedOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	var extraPackages []string
	// Source connection

	asset := ti.GetAsset()

	// Render interval modifier templates if they exist
	if asset.IntervalModifiers.Start.Template != "" {
		renderedStart, err := asset.IntervalModifiers.Start.ResolveTemplateToNew(o.jinjaRenderer)
		if err != nil {
			return fmt.Errorf("failed to render start interval modifier template: %w", err)
		}
		asset.IntervalModifiers.Start = renderedStart
	}

	if asset.IntervalModifiers.End.Template != "" {
		renderedEnd, err := asset.IntervalModifiers.End.ResolveTemplateToNew(o.jinjaRenderer)
		if err != nil {
			return fmt.Errorf("failed to render end interval modifier template: %w", err)
		}
		asset.IntervalModifiers.End = renderedEnd
	}

	sourceConnectionPath, ok := asset.Parameters["path"]
	if !ok {
		return errors.New("source connection not configured")
	}

	var sourceURI string
	if strings.HasPrefix(sourceConnectionPath, "http://") || strings.HasPrefix(sourceConnectionPath, "https://") {
		sourceURI = sourceConnectionPath
	} else {
		sourceURI = "csv://" + filepath.Join(filepath.Dir(asset.ExecutableFile.Path), sourceConnectionPath)
	}

	destConnectionName, err := ti.GetPipeline().GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	destConnection := o.conn.GetConnection(destConnectionName)
	if destConnection == nil {
		return errors.Errorf("destination connection %s not found", destConnectionName)
	}

	destURI, err := destConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return errors.Wrap(err, "could not get the destination uri")
	}
	if destURI == "" {
		return errors.New("destination uri is empty, which means the destination connection is not configured correctly")
	}

	destTable := asset.Name

	extraPackages = python.AddExtraPackages(destURI, sourceURI, extraPackages)

	cmdArgs, err := python.ConsolidatedParameters(ctx, asset, []string{
		"ingest",
		"--source-uri",
		sourceURI,
		"--source-table",
		"seed.raw",
		"--dest-uri",
		destURI,
		"--dest-table",
		destTable,
		"--yes",
		"--progress",
		"log",
	}, &python.ColumnHintOptions{
		NormalizeColumnNames:   true,
		EnforceSchemaByDefault: true,
	})
	if err != nil {
		return err
	}

	path := asset.ExecutableFile.Path
	repo, err := o.finder.Repo(path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Ingestr")
	}

	return o.runner.RunIngestr(ctx, cmdArgs, extraPackages, repo)
}
