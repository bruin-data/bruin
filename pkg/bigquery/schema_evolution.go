package bigquery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"google.golang.org/api/googleapi"
)

// bigQuerySchemaInspector abstracts BigQuery table metadata and dry-run schema inspection
// so schema evolution works against both live *Client connections and unit-test fakes.
type bigQuerySchemaInspector interface {
	TableMetadataForEvolution(ctx context.Context, tableName string) (*bigquery.TableMetadata, bool, error)
	QuerySchemaForEvolution(ctx context.Context, queryObj *query.Query) (bigquery.Schema, error)
}

func (d *Client) TableMetadataForEvolution(ctx context.Context, tableName string) (*bigquery.TableMetadata, bool, error) {
	cat, sch, tbl, n := schemaevolution.SplitTableIdentifier(bigQuerySQLDialect, tableName, "")
	normalized := tableName
	switch n {
	case 2:
		normalized = sch + "." + tbl
	case 3:
		normalized = cat + "." + sch + "." + tbl
	}

	tableRef, err := d.getTableRef(ctx, normalized)
	if err != nil {
		return nil, false, err
	}
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		var apiErr *googleapi.Error
		if errors.As(err, &apiErr) && apiErr.Code == 404 {
			return nil, false, nil
		}
		return nil, false, err
	}
	return meta, true, nil
}

func (d *Client) QuerySchemaForEvolution(ctx context.Context, queryObj *query.Query) (bigquery.Schema, error) {
	stats, err := d.QueryDryRun(ctx, queryObj)
	if err != nil {
		return nil, err
	}
	return stats.Schema, nil
}

// errQuerySchemaUnavailable means a dry run of the asset query behind its pre-hooks returned no
// schema, which happens when the query reads temp tables that only exist once the pre-hooks run.
var errQuerySchemaUnavailable = errors.New("query schema is not available from a dry run")

// ValidateSchemaEvolution validates a schema-evolution-enabled BigQuery SQL asset during `bruin validate`
// by dry-running the SELECT query and verifying that any diff against the live table is backwards-compatible,
// avoiding false-positive DML dry-run errors before `bruin run` applies the planned ALTER TABLE.
// The asset's hooks are expected to be rendered already; hoister orders their DECLAREs the way a run does.
func (d *Client) ValidateSchemaEvolution(ctx context.Context, asset *pipeline.Asset, rawQuery string, hoister pipeline.DeclareHoister) error {
	renderAsset := d.PrepareAssetForRender(asset)
	if renderAsset.Materialization.IsIceberg() {
		return errors.New("schema_evolution is not yet supported for BigQuery Iceberg tables")
	}

	cleaned := schemaevolution.StripTrailingSemicolonsAndComments(bigQuerySQLDialect, rawQuery)
	if _, err := NewMaterializer(false).Render(renderAsset, cleaned); err != nil {
		return err
	}

	engine := &bigQueryEvolutionEngine{
		BigQueryDialect: newBigQueryDialect(),
		inspector:       d,
	}
	// Pre-hooks share script scope with the query (a DECLARE, for example), so the query schema
	// is inspected behind them.
	if len(renderAsset.Hooks.Pre) > 0 {
		engine.preHookSQL = pipeline.WrapHooks("", pipeline.Hooks{Pre: renderAsset.Hooks.Pre}, hoister, renderAsset.Type)
	}

	plan, err := schemaevolution.Validate(ctx, engine, renderAsset, asset.Name, cleaned)
	schemaKnown := true
	switch {
	case errors.Is(err, errQuerySchemaUnavailable):
		// The schema can only be checked once the pre-hooks have run, which `bruin run` does.
		schemaKnown = false
	case err != nil:
		return err
	}

	hooks := renderAsset.Hooks
	if schemaKnown && (!plan.TableExists || !plan.IsEmpty()) {
		// Post-hooks run after the table is created or altered, so a dry run against the current
		// table would reject valid references to the new columns. They are checked at run time.
		hooks.Post = nil
	}
	if hooks.IsZero() {
		return nil
	}
	script := pipeline.WrapHooks(cleaned, hooks, hoister, renderAsset.Type)
	_, err = d.QueryDryRun(ctx, &query.Query{Query: script})
	return err
}

type bigQueryEvolutionEngine struct {
	BigQueryDialect
	inspector bigQuerySchemaInspector
	// preHookSQL, when set, is dry-run ahead of the query so the query can use what the pre-hooks
	// declare. Only validation sets it; runs execute pre-hooks in a session instead.
	preHookSQL string
}

func (e *bigQueryEvolutionEngine) ExistingColumns(ctx context.Context, tableName string) ([]schemaevolution.Column, bool, error) {
	meta, exists, err := e.inspector.TableMetadataForEvolution(ctx, tableName)
	if err != nil || !exists || meta == nil {
		return nil, false, err
	}

	pkSet := make(map[string]bool)
	if meta.TableConstraints != nil && meta.TableConstraints.PrimaryKey != nil {
		for _, pk := range meta.TableConstraints.PrimaryKey.Columns {
			pkSet[e.ColumnKey(pk)] = true
		}
	}

	cols := make([]schemaevolution.Column, 0, len(meta.Schema))
	for _, field := range meta.Schema {
		if field == nil || field.Name == "" {
			continue
		}
		cols = append(cols, schemaevolution.Column{
			Name:       field.Name,
			Type:       bigQueryFieldSchemaToSQLType(field),
			NotNull:    field.Required && !field.Repeated,
			PrimaryKey: pkSet[e.ColumnKey(field.Name)],
		})
	}
	return cols, true, nil
}

func (e *bigQueryEvolutionEngine) QueryColumns(ctx context.Context, queryStr string) ([]schemaevolution.Column, error) {
	trimmed := schemaevolution.StripTrailingSemicolonsAndComments(bigQuerySQLDialect, queryStr)
	if trimmed == "" {
		return nil, nil
	}

	inspected := trimmed
	if e.preHookSQL != "" {
		inspected = e.preHookSQL + "\n" + trimmed + ";"
	}
	schema, err := e.inspector.QuerySchemaForEvolution(ctx, &query.Query{Query: inspected})
	if err != nil {
		return nil, err
	}
	if e.preHookSQL != "" && len(schema) == 0 {
		return nil, errQuerySchemaUnavailable
	}

	cols := make([]schemaevolution.Column, 0, len(schema))
	for _, field := range schema {
		if field == nil || field.Name == "" {
			continue
		}
		cols = append(cols, schemaevolution.Column{
			Name: field.Name,
			Type: bigQueryFieldSchemaToSQLType(field),
		})
	}
	schemaevolution.ApplyUntypedSelectLiterals(bigQueryTypeFormatConfig, trimmed, cols)
	return cols, nil
}

// bigQuerySessionRunner is implemented by connections that can run jobs in a BigQuery session.
type bigQuerySessionRunner interface {
	StartSession(ctx context.Context, q *query.Query) (string, error)
	AbortSession(ctx context.Context, sessionID string)
}

// runPreHooksInSession runs the asset's pre-hooks as the first job of a new BigQuery session.
// It returns a context that keeps later jobs in that session, the asset without its pre-hooks
// (they must not run a second time with the main query), and a function that ends the session.
// Without pre-hooks it changes nothing.
func (o BasicOperator) runPreHooksInSession(
	ctx context.Context,
	p *pipeline.Pipeline,
	asset *pipeline.Asset,
	renderAsset *pipeline.Asset,
	conn DB,
) (context.Context, *pipeline.Asset, func(), error) {
	noop := func() {}
	preHooks := pipeline.Hooks{Pre: renderAsset.Hooks.Pre}
	var preHookSQL string
	if wrapper, ok := o.materializer.(hookWrapper); ok {
		preHookSQL = strings.TrimSpace(wrapper.WrapHooks(&pipeline.Asset{Type: renderAsset.Type, Hooks: preHooks}, ""))
	}
	if preHookSQL == "" {
		return ctx, renderAsset, noop, nil
	}

	runner, ok := conn.(bigQuerySessionRunner)
	if !ok {
		return nil, nil, nil, errors.New("schema_evolution with pre-hooks requires a BigQuery connection that supports sessions")
	}

	hookQuery := &query.Query{Query: preHookSQL}
	if o.devEnv != nil {
		var err error
		hookQuery, err = o.devEnv.Modify(ctx, p, asset, hookQuery)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	annotatedHook, err := ansisql.AddAnnotationComment(ctx, hookQuery, asset.Name, "pre_hook", p.Name)
	if err != nil {
		return nil, nil, nil, err
	}
	sessionID, err := runner.StartSession(query.WithQueryType(ctx, "pre_hook"), annotatedHook)
	if err != nil {
		return nil, nil, nil, err
	}

	withoutPreHooks := *renderAsset
	withoutPreHooks.Hooks.Pre = nil
	endSession := func() { runner.AbortSession(ctx, sessionID) }
	return withSessionID(ctx, sessionID), &withoutPreHooks, endSession, nil
}

func (o BasicOperator) prepareAndApplySchemaEvolution(
	ctx context.Context,
	p *pipeline.Pipeline,
	asset *pipeline.Asset,
	renderAsset *pipeline.Asset,
	conn DB,
	rawQuery string,
	fullRefresh bool,
	writer io.Writer,
) (*schemaevolution.Plan, error) {
	if renderAsset.Materialization.IsIceberg() {
		return nil, errors.New("schema_evolution is not yet supported for BigQuery Iceberg tables")
	}

	inspector, ok := conn.(bigQuerySchemaInspector)
	if !ok {
		return nil, errors.New("bigquery connection does not support schema inspection for schema_evolution")
	}

	inspectQuery := rawQuery
	if o.devEnv != nil && !schemaevolution.UsesDeclaredSchemaOnly(renderAsset.Materialization.Strategy) {
		modified, err := o.devEnv.Modify(ctx, p, asset, &query.Query{Query: inspectQuery})
		if err != nil {
			return nil, err
		}
		inspectQuery = modified.Query
	}

	dialect := newBigQueryDialect()
	engine := &bigQueryEvolutionEngine{
		BigQueryDialect: dialect,
		inspector:       inspector,
	}

	plan, err := schemaevolution.BuildPlan(ctx, engine, renderAsset, asset.Name, inspectQuery, fullRefresh)
	if err != nil {
		return nil, err
	}

	if !plan.IsEmpty() {
		alterSQL := strings.Join(dialect.RenderStatements(asset.Name, plan), ";\n") + ";"
		annotatedAlter, err := ansisql.AddAnnotationComment(ctx, &query.Query{Query: alterSQL}, asset.Name, "schema_evolution", p.Name)
		if err != nil {
			return nil, err
		}
		if err := conn.RunQueryWithoutResult(query.WithQueryType(ctx, "schema_evolution"), annotatedAlter); err != nil {
			return nil, fmt.Errorf("schema evolution DDL failed for %q: %w", asset.Name, err)
		}
		schemaevolution.LogPlan(writer, asset.Name, plan)
	}

	return plan, nil
}
