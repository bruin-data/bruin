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

// ValidateSchemaEvolution validates a schema-evolution-enabled BigQuery SQL asset during `bruin validate`
// by dry-running the SELECT query and verifying that any diff against the live table is backwards-compatible,
// avoiding false-positive DML dry-run errors before `bruin run` applies the planned ALTER TABLE.
func (d *Client) ValidateSchemaEvolution(ctx context.Context, asset *pipeline.Asset, rawQuery string) error {
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
	return schemaevolution.Validate(ctx, engine, renderAsset, asset.Name, cleaned)
}

type bigQueryEvolutionEngine struct {
	BigQueryDialect
	inspector bigQuerySchemaInspector
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

	schema, err := e.inspector.QuerySchemaForEvolution(ctx, &query.Query{Query: trimmed})
	if err != nil {
		return nil, err
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

func (o BasicOperator) prepareAndApplySchemaEvolution(
	ctx context.Context,
	p *pipeline.Pipeline,
	asset *pipeline.Asset,
	renderAsset *pipeline.Asset,
	conn DB,
	rawQuery string,
	fullRefresh bool,
	writer io.Writer,
) (*pipeline.Asset, *schemaevolution.Plan, error) {
	if renderAsset.Materialization.IsIceberg() {
		return nil, nil, errors.New("schema_evolution is not yet supported for BigQuery Iceberg tables")
	}

	inspector, ok := conn.(bigQuerySchemaInspector)
	if !ok {
		return nil, nil, errors.New("bigquery connection does not support schema inspection for schema_evolution")
	}

	// Execute any pre-hooks once before inspecting table/query schemas so staging setups are visible,
	// and strip pre-hooks from renderAsset so they are not executed a second time with the main query.
	effectiveRenderAsset := renderAsset
	if len(renderAsset.Hooks.Pre) > 0 {
		for _, hook := range renderAsset.Hooks.Pre {
			hq := strings.TrimSpace(hook.Query)
			if hq == "" {
				continue
			}
			hookQuery := &query.Query{Query: hq}
			if o.devEnv != nil {
				var err error
				hookQuery, err = o.devEnv.Modify(ctx, p, asset, hookQuery)
				if err != nil {
					return nil, nil, err
				}
			}
			annotatedHook, err := ansisql.AddAnnotationComment(ctx, hookQuery, asset.Name, "pre_hook", p.Name)
			if err != nil {
				return nil, nil, err
			}
			if err := conn.RunQueryWithoutResult(query.WithQueryType(ctx, "pre_hook"), annotatedHook); err != nil {
				return nil, nil, err
			}
		}
		copied := *renderAsset
		copied.Hooks.Pre = nil
		effectiveRenderAsset = &copied
	}

	inspectQuery := schemaevolution.StripTrailingSemicolonsAndComments(bigQuerySQLDialect, rawQuery)
	if o.devEnv != nil && !schemaevolution.UsesDeclaredSchemaOnly(effectiveRenderAsset.Materialization.Strategy) {
		modified, err := o.devEnv.Modify(ctx, p, asset, &query.Query{Query: inspectQuery})
		if err != nil {
			return nil, nil, err
		}
		inspectQuery = modified.Query
	}

	dialect := newBigQueryDialect()
	engine := &bigQueryEvolutionEngine{
		BigQueryDialect: dialect,
		inspector:       inspector,
	}

	plan, err := schemaevolution.BuildPlan(ctx, engine, effectiveRenderAsset, asset.Name, inspectQuery, fullRefresh)
	if err != nil {
		return nil, nil, err
	}

	if !plan.IsEmpty() {
		alterSQL := strings.Join(dialect.RenderStatements(asset.Name, plan), ";\n") + ";"
		annotatedAlter, err := ansisql.AddAnnotationComment(ctx, &query.Query{Query: alterSQL}, asset.Name, "schema_evolution", p.Name)
		if err != nil {
			return nil, nil, err
		}
		if err := conn.RunQueryWithoutResult(query.WithQueryType(ctx, "schema_evolution"), annotatedAlter); err != nil {
			return nil, nil, fmt.Errorf("schema evolution DDL failed for %q: %w", asset.Name, err)
		}
		schemaevolution.LogPlan(writer, asset.Name, plan)
	}

	return effectiveRenderAsset, plan, nil
}
