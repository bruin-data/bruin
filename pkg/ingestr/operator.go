package ingestr

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/connection"
	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type connectionFetcher interface {
	GetConnection(name string) (interface{}, error)
}

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

type ingestrRunner interface {
	RunIngestr(ctx context.Context, args, extraPackages []string, repo *git.Repo) error
}

type renderer interface {
	Render(query string) (string, error)
	RenderAsset(asset *pipeline.Asset) (*pipeline.Asset, error)
}

type BasicOperator struct {
	conn          connectionFetcher
	runner        ingestrRunner
	finder        repoFinder
	jinjaRenderer renderer
}

type SeedOperator struct {
	conn     connectionFetcher
	runner   ingestrRunner
	finder   repoFinder
	renderer renderer
}

type pipelineConnection interface {
	GetIngestrURI() (string, error)
}

func NewBasicOperator(conn *connection.Manager, j renderer) (*BasicOperator, error) {
	uvRunner := &python.UvPythonRunner{
		UvInstaller: &python.UvChecker{},
		Cmd:         &python.CommandRunner{},
	}

	return &BasicOperator{conn: conn, runner: uvRunner, finder: &git.RepoFinder{}, jinjaRenderer: j}, nil
}

func (o *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	var extraPackages []string

	// Source connection
	sourceConnectionName, ok := asset.Parameters["source_connection"]
	if !ok {
		return errors.New("source connection not configured")
	}

	sourceConnection, err := o.conn.GetConnection(sourceConnectionName)
	if err != nil {
		return errors.Wrapf(err, "source connection %s not found", sourceConnectionName)
	}

	sourceURI, err := sourceConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return fmt.Errorf("could not get the source uri: %w", err)
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

	destConnectionName, err := ti.GetPipeline().GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	destConnection, err := o.conn.GetConnection(destConnectionName)
	if err != nil {
		return fmt.Errorf("destination connection %s not found", destConnectionName)
	}

	destURI, err := destConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return errors.New("could not get the source uri")
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

func NewSeedOperator(conn *connection.Manager, renderer renderer) (*SeedOperator, error) {
	uvRunner := &python.UvPythonRunner{
		UvInstaller: &python.UvChecker{},
		Cmd:         &python.CommandRunner{},
	}

	return &SeedOperator{
		conn:     conn,
		runner:   uvRunner,
		finder:   &git.RepoFinder{},
		renderer: renderer,
	}, nil
}

func (o *SeedOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	var extraPackages []string
	// Source connection

	asset, err := o.renderer.RenderAsset(ti.GetAsset())
	if err != nil {
		return fmt.Errorf("failed to render asset: %w", err)
	}

	sourceConnectionPath, ok := asset.Parameters["path"]
	if !ok {
		return errors.New("source connection not configured")
	}

	sourceURI := "csv://" + filepath.Join(filepath.Dir(asset.ExecutableFile.Path), sourceConnectionPath)

	destConnectionName, err := ti.GetPipeline().GetConnectionNameForAsset(asset)
	if err != nil {
		return err
	}

	destConnection, err := o.conn.GetConnection(destConnectionName)
	if err != nil {
		return fmt.Errorf("destination connection %s not found", destConnectionName)
	}

	destURI, err := destConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return errors.New("could not get the source uri")
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
	})
	if err != nil {
		return err
	}

	columns := columnHints(asset.Columns)
	if columns != "" {
		cmdArgs = append(cmdArgs, "--columns", columns)
	}

	path := asset.ExecutableFile.Path
	repo, err := o.finder.Repo(path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Ingestr")
	}

	return o.runner.RunIngestr(ctx, cmdArgs, extraPackages, repo)
}
