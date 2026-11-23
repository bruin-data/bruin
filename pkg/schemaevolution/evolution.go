package schemaevolution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// Column describes a table or query output column using the target engine's native SQL type
// together with standard SQL column metadata used for compatibility checks and DDL rendering.
type Column struct {
	Name          string
	Type          string
	NotNull       bool
	Default       string
	Collation     string
	PrimaryKey    bool
	Unique        bool
	HasForeignKey bool
	UntypedNull   bool
	// UnreportedTypeParams marks a type read from query metadata that never reports type
	// parameters (a BigQuery dry run reports STRING for a STRING(10) value), so a bare STRING,
	// BYTES, NUMERIC, or BIGNUMERIC keeps the length, precision, and scale of the existing column.
	UnreportedTypeParams bool
}

// ActionType identifies a single backwards-compatible schema evolution operation.
type ActionType string

const (
	ActionAddColumn       ActionType = "add_column"
	ActionAlterColumnType ActionType = "alter_column_type"
)

// Action describes one backwards-compatible schema change to apply to an existing table.
type Action struct {
	Type       ActionType
	ColumnName string
	FromType   string
	ToType     string
	// Column carries the full target column metadata (raw DDL type, default, collation, nullability).
	Column Column
}

// Plan holds the ordered schema evolution actions to apply along with the inspected
// table existence and incoming column list for engines that rewrite positional INSERTs.
type Plan struct {
	TableExists     bool
	Actions         []Action
	IncomingColumns []Column
}

// IsEmpty reports whether the plan contains any schema changes.
func (p *Plan) IsEmpty() bool {
	return p == nil || len(p.Actions) == 0
}

// TypeResolution describes the outcome of comparing an existing column type against an incoming one.
type TypeResolution int

const (
	// TypeChangeNone means the existing column type already matches or safely absorbs the incoming type.
	TypeChangeNone TypeResolution = iota
	// TypeChangeWiden means the existing column can be safely widened in place to a new type.
	TypeChangeWiden
	// TypeChangeIncompatible means the type change is not backwards-compatible.
	TypeChangeIncompatible
)

// IncompatibleColumnError describes a single column whose incoming schema cannot be
// reconciled with the existing table schema in a backwards-compatible way.
type IncompatibleColumnError struct {
	ColumnName   string
	CurrentType  string
	IncomingType string
	Reason       string
}

func (e IncompatibleColumnError) String() string {
	if e.Reason != "" {
		if e.CurrentType != "" && e.IncomingType != "" {
			return fmt.Sprintf("column %q (%s -> %s): %s", e.ColumnName, e.CurrentType, e.IncomingType, e.Reason)
		}
		return fmt.Sprintf("column %q: %s", e.ColumnName, e.Reason)
	}
	return fmt.Sprintf("column %q (%s -> %s)", e.ColumnName, e.CurrentType, e.IncomingType)
}

// IncompatibleSchemaError is returned when one or more columns have incompatible schema changes.
type IncompatibleSchemaError struct {
	TableName          string
	Changes            []IncompatibleColumnError
	SuggestFullRefresh bool
}

func (e *IncompatibleSchemaError) Error() string {
	parts := make([]string, len(e.Changes))
	for i, c := range e.Changes {
		parts[i] = c.String()
	}
	hint := "only backwards-compatible additions and type widenings are supported"
	if e.SuggestFullRefresh {
		hint += "; use --full-refresh to rebuild the table"
	}
	return fmt.Sprintf(
		"schema evolution failed for table %q: incompatible change(s): %s (%s)",
		e.TableName,
		strings.Join(parts, "; "),
		hint,
	)
}

// Dialect encapsulates warehouse-specific identifier matching, type normalization,
// and backwards-compatible type change rules.
type Dialect interface {
	ColumnKey(name string) string
	NormalizeType(sqlType string) string
	ValidateNewColumn(col Column) (Column, string, bool)
	ResolveTypeChange(existing, incoming Column) (TypeResolution, string, string)
}

// MissingColumnChecker is an optional interface implemented by dialects or materialization
// modes (such as table-rebuilding SCD2 strategies) where an existing table column omitted
// from incoming columns would be dropped by the write statement and must therefore be rejected.
type MissingColumnChecker interface {
	CheckMissingColumn(existing Column) (reason string, blocked bool)
}

// PlanReconciler is an optional interface implemented by engines that perform lazy session-level
// verification or plan refinement after pure type diffing (such as SCD2 UNION ALL verification
// or catalog index/FK/CHECK constraint checks when altering column types).
type PlanReconciler interface {
	ReconcilePlan(ctx context.Context, asset *pipeline.Asset, tableName string, existing, incoming []Column, plan *Plan) ([]IncompatibleColumnError, error)
}

// DeclaredColumnResolver is an optional interface implemented by engines that customize
// declared-column resolution (for example, DuckDB Data Vault metadata or live CAST canonicalization).
// Engines that do not implement it automatically use ColumnsFromAsset normalized via Dialect.NormalizeType.
type DeclaredColumnResolver interface {
	DeclaredColumns(ctx context.Context, asset *pipeline.Asset) ([]Column, error)
}

// Engine inspects live table and query schemas for a specific data warehouse.
type Engine interface {
	Dialect
	ExistingColumns(ctx context.Context, tableName string) ([]Column, bool, error)
	QueryColumns(ctx context.Context, queryStr string) ([]Column, error)
}

// ShouldEvolve reports whether schema evolution should run for asset given fullRefresh.
func ShouldEvolve(asset *pipeline.Asset, fullRefresh bool) bool {
	if asset == nil || !asset.SchemaEvolution.IsEnabled() {
		return false
	}
	if fullRefresh && asset.Materialization.Strategy != pipeline.MaterializationStrategyDDL {
		return false
	}
	return asset.Materialization.SupportsInPlaceEvolution()
}

// UsesDeclaredColumnsOnly reports whether strategy writes strictly the columns declared
// in asset.Columns (when non-empty), ignoring undeclared query columns.
func UsesDeclaredColumnsOnly(strategy pipeline.MaterializationStrategy) bool {
	switch strategy {
	case pipeline.MaterializationStrategyMerge,
		pipeline.MaterializationStrategySCD2ByColumn,
		pipeline.MaterializationStrategySCD2ByTime:
		return true
	default:
		return false
	}
}

// UsesDeclaredSchemaOnly reports whether strategy derives its table schema strictly from
// asset.Columns metadata without inspecting the user's SQL query.
func UsesDeclaredSchemaOnly(strategy pipeline.MaterializationStrategy) bool {
	return strategy == pipeline.MaterializationStrategyDDL || strategy.IsDataVault()
}

// ColumnsFromAsset converts an asset's declared columns into schema evolution Columns.
func ColumnsFromAsset(asset *pipeline.Asset) []Column {
	if asset == nil || len(asset.Columns) == 0 {
		return nil
	}
	cols := make([]Column, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		name := strings.TrimSpace(col.Name)
		if name == "" {
			continue
		}
		cols = append(cols, Column{
			Name:          name,
			Type:          strings.TrimSpace(col.SQLType()),
			NotNull:       col.Nullable.Value != nil && !*col.Nullable.Value,
			Default:       strings.TrimSpace(col.Default),
			Collation:     strings.TrimSpace(col.Collation),
			PrimaryKey:    col.PrimaryKey,
			HasForeignKey: col.ForeignKey != nil && strings.TrimSpace(col.ForeignKey.Table) != "" && strings.TrimSpace(col.ForeignKey.Column) != "",
		})
	}
	return cols
}

// Diff compares existing table columns against incoming columns using dialect and
// produces a backwards-compatible Plan. Existing columns absent from incoming are
// never dropped.
func Diff(dialect Dialect, tableName string, existing, incoming []Column) (*Plan, error) {
	existingByName := make(map[string]Column, len(existing))
	for _, col := range existing {
		key := dialect.ColumnKey(col.Name)
		if key == "" {
			continue
		}
		existingByName[key] = col
	}

	plan := &Plan{}
	var incompatible []IncompatibleColumnError
	seenIncoming := make(map[string]bool, len(incoming))

	for _, col := range incoming {
		key := dialect.ColumnKey(col.Name)
		if key == "" || seenIncoming[key] {
			continue
		}
		seenIncoming[key] = true

		existingCol, exists := existingByName[key]
		if !exists {
			targetCol, reason, ok := dialect.ValidateNewColumn(col)
			if !ok {
				incompatible = append(incompatible, IncompatibleColumnError{
					ColumnName:   col.Name,
					IncomingType: col.Type,
					Reason:       reason,
				})
				continue
			}
			plan.Actions = append(plan.Actions, Action{
				Type:       ActionAddColumn,
				ColumnName: targetCol.Name,
				ToType:     targetCol.Type,
				Column:     targetCol,
			})
			continue
		}

		res, targetType, reason := dialect.ResolveTypeChange(existingCol, col)
		switch res {
		case TypeChangeNone:
			continue
		case TypeChangeWiden:
			alteredCol := existingCol
			alteredCol.Type = targetType
			plan.Actions = append(plan.Actions, Action{
				Type:       ActionAlterColumnType,
				ColumnName: existingCol.Name,
				FromType:   dialect.NormalizeType(existingCol.Type),
				ToType:     targetType,
				Column:     alteredCol,
			})
		case TypeChangeIncompatible:
			incompatible = append(incompatible, IncompatibleColumnError{
				ColumnName:   existingCol.Name,
				CurrentType:  dialect.NormalizeType(existingCol.Type),
				IncomingType: dialect.NormalizeType(col.Type),
				Reason:       reason,
			})
		}
	}

	if checker, ok := dialect.(MissingColumnChecker); ok {
		for _, existingCol := range existing {
			key := dialect.ColumnKey(existingCol.Name)
			if key == "" || seenIncoming[key] {
				continue
			}
			if reason, blocked := checker.CheckMissingColumn(existingCol); blocked {
				incompatible = append(incompatible, IncompatibleColumnError{
					ColumnName:  existingCol.Name,
					CurrentType: dialect.NormalizeType(existingCol.Type),
					Reason:      reason,
				})
			}
		}
	}

	if len(incompatible) > 0 {
		return nil, &IncompatibleSchemaError{
			TableName:          tableName,
			Changes:            incompatible,
			SuggestFullRefresh: true,
		}
	}

	return plan, nil
}

// BuildPlan inspects tableName and userQuery, runs pure type diffing and any engine-specific
// PlanReconciler checks, and returns a backwards-compatible evolution Plan without applying it.
func BuildPlan(ctx context.Context, engine Engine, asset *pipeline.Asset, tableName, userQuery string, fullRefresh bool) (*Plan, error) {
	if !ShouldEvolve(asset, fullRefresh) {
		return &Plan{}, nil
	}

	existing, exists, err := engine.ExistingColumns(ctx, tableName)
	if err != nil {
		return nil, fmt.Errorf("schema evolution: failed to inspect existing table %q: %w", tableName, err)
	}
	if !exists {
		return &Plan{}, nil
	}

	incoming, err := resolveIncomingColumns(ctx, engine, asset, tableName, userQuery)
	if err != nil {
		return nil, err
	}

	plan, err := Diff(engine, tableName, existing, incoming)
	if err != nil {
		var incompatErr *IncompatibleSchemaError
		if errors.As(err, &incompatErr) {
			incompatErr.SuggestFullRefresh = canSuggestFullRefresh(asset)
		}
		return nil, err
	}

	if reconciler, ok := engine.(PlanReconciler); ok {
		incompatible, err := reconciler.ReconcilePlan(ctx, asset, tableName, existing, incoming, plan)
		if err != nil {
			return nil, err
		}
		if len(incompatible) > 0 {
			return nil, &IncompatibleSchemaError{
				TableName:          tableName,
				Changes:            incompatible,
				SuggestFullRefresh: canSuggestFullRefresh(asset),
			}
		}
	}

	plan.TableExists = true
	plan.IncomingColumns = incoming
	return plan, nil
}

// Validate checks a schema-evolution-enabled asset during `bruin validate` without mutating the
// target warehouse: it verifies that any schema diff against an existing table is backwards-compatible,
// and still inspects userQuery via engine.QueryColumns when the target table does not exist yet.
// It returns the plan a run would apply.
func Validate(ctx context.Context, engine Engine, asset *pipeline.Asset, tableName, userQuery string) (*Plan, error) {
	plan, err := BuildPlan(ctx, engine, asset, tableName, userQuery, false)
	if err != nil {
		return nil, err
	}
	if !plan.TableExists && asset != nil && !UsesDeclaredSchemaOnly(asset.Materialization.Strategy) && strings.TrimSpace(userQuery) != "" {
		if _, err := engine.QueryColumns(ctx, userQuery); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

func canSuggestFullRefresh(asset *pipeline.Asset) bool {
	if asset == nil {
		return true
	}
	if asset.Materialization.Strategy == pipeline.MaterializationStrategyDDL ||
		asset.Materialization.IsSCD2() ||
		asset.Materialization.Strategy.IsDataVault() {
		return false
	}
	if asset.RefreshRestricted != nil && *asset.RefreshRestricted {
		return false
	}
	return true
}

func resolveIncomingColumns(ctx context.Context, engine Engine, asset *pipeline.Asset, tableName, userQuery string) ([]Column, error) {
	strategy := asset.Materialization.Strategy
	if UsesDeclaredSchemaOnly(strategy) {
		if resolver, ok := engine.(DeclaredColumnResolver); ok {
			declared, err := resolver.DeclaredColumns(ctx, asset)
			if err != nil {
				return nil, fmt.Errorf("schema evolution: failed to resolve declared columns for %q: %w", tableName, err)
			}
			return declared, nil
		}
		cols := ColumnsFromAsset(asset)
		for i := range cols {
			if cols[i].Type != "" {
				cols[i].Type = engine.NormalizeType(cols[i].Type)
			}
		}
		return cols, nil
	}

	queryCols, err := engine.QueryColumns(ctx, userQuery)
	if err != nil {
		return nil, fmt.Errorf("schema evolution: failed to inspect query schema for %q: %w", tableName, err)
	}

	if !UsesDeclaredColumnsOnly(strategy) || asset == nil || len(asset.Columns) == 0 {
		return queryCols, nil
	}

	queryByName := make(map[string]Column, len(queryCols))
	for _, qc := range queryCols {
		if key := engine.ColumnKey(qc.Name); key != "" {
			queryByName[key] = qc
		}
	}

	// The materializations for these strategies reference every declared column on the query
	// output, so a declared column the query does not return would fail the write.
	filtered := make([]Column, 0, len(asset.Columns))
	var missing []string
	for _, col := range asset.Columns {
		name := strings.TrimSpace(col.Name)
		key := engine.ColumnKey(name)
		if key == "" {
			continue
		}
		qc, ok := queryByName[key]
		if !ok {
			missing = append(missing, name)
			continue
		}
		filtered = append(filtered, Column{
			Name:                 name,
			Type:                 qc.Type,
			UntypedNull:          qc.UntypedNull,
			UnreportedTypeParams: qc.UnreportedTypeParams,
		})
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("schema evolution: the query for %q does not return declared column(s) %s, which the %s strategy requires", tableName, strings.Join(missing, ", "), strategy)
	}
	return filtered, nil
}

// LogPlan writes a concise summary of applied schema evolution actions to writer.
func LogPlan(writer io.Writer, tableName string, plan *Plan) {
	if writer == nil || plan.IsEmpty() {
		return
	}

	summaries := make([]string, 0, len(plan.Actions))
	for _, action := range plan.Actions {
		switch action.Type {
		case ActionAddColumn:
			summaries = append(summaries, fmt.Sprintf("adding column %q (%s)", action.ColumnName, action.ToType))
		case ActionAlterColumnType:
			summaries = append(summaries, fmt.Sprintf("widening column %q (%s -> %s)", action.ColumnName, action.FromType, action.ToType))
		}
	}
	_, _ = fmt.Fprintf(writer, "Schema evolution on %s: %s\n", tableName, strings.Join(summaries, ", "))
}
