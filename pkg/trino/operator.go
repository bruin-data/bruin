package trino

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
)

const CharacterLimit = 10000

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
	LogIfFullRefreshAndDDL(writer interface{}, asset *pipeline.Asset) error
}

type BasicOperator struct {
	connection   config.ConnectionGetter
	extractor    query.QueryExtractor
	materializer materializer
}

func NewBasicOperator(conn config.ConnectionGetter, extractor query.QueryExtractor, materializer materializer) *BasicOperator {
	return &BasicOperator{
		connection:   conn,
		extractor:    extractor,
		materializer: materializer,
	}
}

func (o BasicOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o BasicOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	extractor, err := o.extractor.CloneForAsset(ctx, p, t)
	if err != nil {
		return errors.Wrapf(err, "failed to clone extractor for asset %s", t.Name)
	}

	queries, err := extractor.ExtractQueriesFromString(t.ExecutableFile.Content)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from the task file")
	}

	if len(queries) == 0 {
		return nil
	}

	if len(queries) > 1 && t.Materialization.Type != pipeline.MaterializationTypeNone {
		return errors.New("cannot enable materialization for tasks with multiple queries")
	}

	q := queries[0]
	materialized, err := o.materializer.Render(t, q.String())
	if err != nil {
		return err
	}
	writer := ctx.Value(executor.KeyPrinter)
	err = o.materializer.LogIfFullRefreshAndDDL(writer, t)
	if err != nil {
		return err
	}
	connName, err := p.GetConnectionNameForAsset(t)
	if err != nil {
		return err
	}

	conn, ok := o.connection.GetConnection(connName).(*Client)
	if !ok {
		return errors.Errorf("'%s' either does not exist or is not a trino connection", connName)
	}

	// Extract multiple queries from the materialized string
	materializedQueries, err := extractor.ExtractQueriesFromString(materialized)
	if err != nil {
		return errors.Wrap(err, "cannot extract queries from materialized string")
	}

	// Execute each query separately
	for _, queryObj := range materializedQueries {
		// Print SQL query in verbose mode
		if verbose := ctx.Value(executor.KeyVerbose); verbose != nil && verbose.(bool) {
			if w, ok := writer.(io.Writer); ok {
				queryPreview := strings.TrimSpace(queryObj.Query)
				if len(queryPreview) > CharacterLimit {
					queryPreview = queryPreview[:CharacterLimit] + "\n... (truncated)"
				}
				fmt.Fprintf(w, "Executing SQL query:\n%s\n\n", queryPreview)
			}
		}

		err = conn.RunQueryWithoutResult(ctx, queryObj)
		if err != nil {
			return err
		}
	}

	return nil
}
