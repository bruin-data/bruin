package ingestr

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

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

type BasicOperator struct {
	conn     connectionFetcher
	uvRunner *python.UvPythonRunner
	finder   repoFinder
}

type pipelineConnection interface {
	GetIngestrURI() (string, error)
}

func NewBasicOperator(conn *connection.Manager) (*BasicOperator, error) {
	uvRunner := &python.UvPythonRunner{
		UvInstaller: &python.UvChecker{},
		Cmd:         &python.CommandRunner{},
	}

	return &BasicOperator{conn: conn, uvRunner: uvRunner, finder: &git.RepoFinder{}}, nil
}

func (o *BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	// Source connection
	sourceConnectionName, ok := ti.GetAsset().Parameters["source_connection"]
	if !ok {
		return errors.New("source connection not configured")
	}

	sourceConnection, err := o.conn.GetConnection(sourceConnectionName)
	if err != nil {
		return errors.Wrapf(err, "source connection %s not found", sourceConnectionName)
	}

	sourceURI, err := sourceConnection.(pipelineConnection).GetIngestrURI()
	if err != nil {
		return errors.New("could not get the source uri")
	}

	// some connection types can be shared among sources, therefore inferring source URI from the connection type is not
	// always feasible. In the case of GSheets, we have to reuse the same GCP credentials, but change the prefix with gsheets://
	if ti.GetAsset().Parameters["source"] == "gsheets" {
		sourceURI = strings.ReplaceAll(sourceURI, "bigquery://", "gsheets://")
	}

	sourceTable, ok := ti.GetAsset().Parameters["source_table"]
	if !ok {
		return errors.New("source table not configured")
	}

	destConnectionName, err := ti.GetPipeline().GetConnectionNameForAsset(ti.GetAsset())
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

	destTable := ti.GetAsset().Name

	cmdArgs := []string{
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
	}

	incrementalStrategy, ok := ti.GetAsset().Parameters["incremental_strategy"]
	if ok {
		cmdArgs = append(cmdArgs, "--incremental-strategy", incrementalStrategy)
	}

	incrementalKey, ok := ti.GetAsset().Parameters["incremental_key"]
	if ok {
		cmdArgs = append(cmdArgs, "--incremental-key", incrementalKey)
	}

	primaryKeys := ti.GetAsset().ColumnNamesWithPrimaryKey()
	if len(primaryKeys) > 0 {
		for _, pk := range primaryKeys {
			cmdArgs = append(cmdArgs, "--primary-key", pk)
		}
	}

	loaderFileFormat, ok := ti.GetAsset().Parameters["loader_file_format"]
	if ok {
		cmdArgs = append(cmdArgs, "--loader-file-format", loaderFileFormat)
	}

	sqlBackend, ok := ti.GetAsset().Parameters["sql_backend"]
	if ok {
		cmdArgs = append(cmdArgs, "--sql-backend", sqlBackend)
	}

	injectIntervals, ok := ti.GetAsset().Parameters["inject_intervals"]
	if ok {
		boolInject, err := strconv.ParseBool(injectIntervals)
		if err != nil {
			return errors.Wrap(err, "failed to parse inject_intervals")
		}

		if boolInject {
			startDateString := ctx.Value(pipeline.RunConfigStartDate).(time.Time).Format(time.RFC3339)
			endDateString := ctx.Value(pipeline.RunConfigEndDate).(time.Time).Format(time.RFC3339)

			cmdArgs = append(cmdArgs, "--interval-start", startDateString, "--interval-end", endDateString)
		}
	}

	fullRefresh := ctx.Value(pipeline.RunConfigFullRefresh)
	if fullRefresh != nil && fullRefresh.(bool) {
		cmdArgs = append(cmdArgs, "--full-refresh")
	}

	if strings.HasPrefix(destURI, "duckdb://") {
		duck.LockDatabase(destURI)
		defer duck.UnlockDatabase(destURI)
	}

	path := ti.GetAsset().ExecutableFile.Path
	repo, err := o.finder.Repo(path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Ingestr")
	}

	return o.uvRunner.RunIngestr(ctx, cmdArgs, repo)
}
