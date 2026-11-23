package duck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/bruin-data/bruin/pkg/ansisql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/schemaevolution"
	"github.com/bruin-data/bruin/pkg/sqlengine"
)

var errNoPinnedSession = errors.New("duckdb connection does not support pinned sessions")

// duckDBSession runs multiple statements on a single pinned DuckDB connection.
type duckDBSession interface {
	Select(ctx context.Context, queryStr string) ([][]interface{}, error)
	Exec(ctx context.Context, sqlStr string) error
}

type duckDBSessionRunner interface {
	RunInSession(ctx context.Context, hintSQL string, fn func(ctx context.Context, s duckDBSession) error) error
}

type lakehouseFormatProvider interface {
	LakehouseFormat() config.LakehouseFormat
}

type clientFallbackSession struct {
	client DuckDBClient
}

func (s *clientFallbackSession) Select(ctx context.Context, queryStr string) ([][]interface{}, error) {
	return s.client.Select(ctx, &query.Query{Query: queryStr})
}

func (s *clientFallbackSession) Exec(ctx context.Context, sqlStr string) error {
	return s.client.RunQueryWithoutResult(ctx, &query.Query{Query: sqlStr})
}

// duckDBEvolutionRequest bundles the inputs for an atomic DuckDB schema-evolution asset run.
type duckDBEvolutionRequest struct {
	Conn         DuckDBClient
	Hooks        pipeline.Hooks
	Asset        *pipeline.Asset
	InspectQuery string
	MainSQL      string
	FullRefresh  bool
	Writer       io.Writer
}

type duckDBEvolutionEngine struct {
	DuckDBDialect
	session duckDBSession
}

func runInDuckDBSession(
	ctx context.Context,
	conn DuckDBClient,
	requirePinned bool,
	hintSQL string,
	fn func(ctx context.Context, s duckDBSession) error,
) error {
	if runner, ok := conn.(duckDBSessionRunner); ok {
		err := runner.RunInSession(ctx, hintSQL, fn)
		if !errors.Is(err, errNoPinnedSession) {
			return err
		}
	}
	if requirePinned {
		return errors.New("schema evolution with pre-hooks requires a session-capable DuckDB connection")
	}
	return fn(ctx, &clientFallbackSession{client: conn})
}

// executeWithDuckDBSchemaEvolution runs the entire asset lifecycle (pre-hooks, schema
// inspection, atomic ALTER TABLE + write, and post-hooks) on a single pinned DuckDB session
// so pre-hooks execute exactly once and schema inspection sees the exact session state.
func executeWithDuckDBSchemaEvolution(
	ctx context.Context,
	req duckDBEvolutionRequest,
) (*schemaevolution.Plan, string, error) {
	tableName := ""
	rebuildsViaCTAS := false
	isDataVault := false
	if req.Asset != nil {
		tableName = req.Asset.Name
		rebuildsViaCTAS = req.Asset.Materialization.IsSCD2()
		isDataVault = req.Asset.Materialization.Strategy.IsDataVault()
	}

	duckLake := false
	if provider, ok := req.Conn.(lakehouseFormatProvider); ok {
		switch provider.LakehouseFormat() {
		case config.LakehouseFormatIceberg:
			return nil, "", errors.New("schema_evolution is not yet supported for DuckDB Iceberg lakehouse connections")
		case config.LakehouseFormatDuckLake:
			duckLake = true
		}
	}
	dialect := newDuckDBDialect(duckLake, rebuildsViaCTAS, isDataVault)

	preHookSQLs := nonEmptyHookSQLs(req.Hooks.Pre)
	postHookSQLs := nonEmptyHookSQLs(req.Hooks.Post)

	hintParts := make([]string, 0, len(preHookSQLs)+1+len(postHookSQLs))
	hintParts = append(hintParts, preHookSQLs...)
	hintParts = append(hintParts, req.MainSQL)
	hintParts = append(hintParts, postHookSQLs...)
	hintSQL := strings.Join(hintParts, ";\n")

	var (
		plan         *schemaevolution.Plan
		executedMain string
	)

	err := runInDuckDBSession(ctx, req.Conn, len(preHookSQLs) > 0, hintSQL, func(ctx context.Context, session duckDBSession) error {
		for _, hq := range preHookSQLs {
			ansisql.LogQueryIfVerbose(ctx, req.Writer, hq)
			if err := session.Exec(ctx, hq); err != nil {
				return err
			}
		}

		engine := &duckDBEvolutionEngine{
			DuckDBDialect: dialect,
			session:       session,
		}

		var err error
		plan, err = schemaevolution.BuildPlan(ctx, engine, req.Asset, tableName, req.InspectQuery, req.FullRefresh)
		if err != nil {
			return err
		}

		finalMain := req.MainSQL
		if !plan.IsEmpty() {
			alterStmts := dialect.RenderStatements(tableName, plan)
			finalMain = schemaevolution.SpliceStatementsInTransaction(duckDBSQLDialect, finalMain, alterStmts)
		}
		executedMain = finalMain

		ansisql.LogQueryIfVerbose(ctx, req.Writer, finalMain)
		if err := session.Exec(ctx, finalMain); err != nil {
			return err
		}

		for _, hq := range postHookSQLs {
			ansisql.LogQueryIfVerbose(ctx, req.Writer, hq)
			if err := session.Exec(ctx, hq); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return nil, "", err
	}
	return plan, executedMain, nil
}

func nonEmptyHookSQLs(hooks []pipeline.Hook) []string {
	out := make([]string, 0, len(hooks))
	for _, h := range hooks {
		q := strings.TrimSpace(h.Query)
		if q != "" {
			out = append(out, q)
		}
	}
	return out
}

// ValidateSchemaEvolution validates a schema-evolution-enabled DuckDB SQL asset during `bruin validate`
// without mutating the target database. Hooks are validated separately by the hook dry-run rule.
func (c *Client) ValidateSchemaEvolution(ctx context.Context, asset *pipeline.Asset, rawQuery string, _ pipeline.DeclareHoister) error {
	if q, ok := any(c).(interface{ HasQuack() bool }); ok && q.HasQuack() {
		return errors.New("connection uses quack: true, which does not support duckdb.sql assets yet")
	}
	duckLake := false
	if provider, ok := any(c).(lakehouseFormatProvider); ok {
		switch provider.LakehouseFormat() {
		case config.LakehouseFormatIceberg:
			return errors.New("schema_evolution is not yet supported for DuckDB Iceberg lakehouse connections")
		case config.LakehouseFormatDuckLake:
			duckLake = true
		default:
		}
	}

	cleaned := schemaevolution.StripTrailingSemicolonsAndComments(duckDBSQLDialect, rawQuery)
	if _, err := NewMaterializer(false).Render(asset, cleaned); err != nil {
		return err
	}

	dialect := newDuckDBDialect(duckLake, asset.Materialization.IsSCD2(), asset.Materialization.Strategy.IsDataVault())
	return runInDuckDBSession(ctx, c, false, cleaned, func(ctx context.Context, session duckDBSession) error {
		engine := &duckDBEvolutionEngine{
			DuckDBDialect: dialect,
			session:       session,
		}
		_, err := schemaevolution.Validate(ctx, engine, asset, asset.Name, cleaned)
		return err
	})
}

func (e *duckDBEvolutionEngine) ExistingColumns(ctx context.Context, tableName string) ([]schemaevolution.Column, bool, error) {
	rows, err := e.session.Select(ctx, "DESCRIBE "+tableName)
	if err != nil {
		if isDuckDBTableNotFoundError(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return parseDescribeRows(rows), true, nil
}

// ReconcilePlan implements schemaevolution.PlanReconciler, running SCD2 UNION ALL verification
// when rebuilding via CTAS and lazy catalog constraint checks only when altering column types.
func (e *duckDBEvolutionEngine) ReconcilePlan(
	ctx context.Context,
	asset *pipeline.Asset,
	tableName string,
	existing, incoming []schemaevolution.Column,
	plan *schemaevolution.Plan,
) ([]schemaevolution.IncompatibleColumnError, error) {
	if e.RebuildsViaCTAS {
		if incompatible := reconcileDuckDBSCD2Plan(ctx, e.session, e.DuckDBDialect, asset, existing, incoming, plan); len(incompatible) > 0 {
			return incompatible, nil
		}
	}
	return checkDuckDBAlterConstraints(ctx, e.session, tableName, plan), nil
}

type scd2DiffPair struct {
	existing      schemaevolution.Column
	incoming      schemaevolution.Column
	cur           schemaevolution.ParsedType
	postFmt       string
	incFmt        string
	postAlterType string
	widened       bool
}

// reconcileDuckDBSCD2Plan verifies that kept and widened column types survive DuckDB SCD2's
// `CREATE OR REPLACE TABLE ... UNION ALL` rebuild intact using a single batched DESCRIBE probe
// across differing columns (with per-column fallback only when UNION ALL itself fails).
func reconcileDuckDBSCD2Plan(
	ctx context.Context,
	session duckDBSession,
	dialect DuckDBDialect,
	asset *pipeline.Asset,
	existing, incoming []schemaevolution.Column,
	plan *schemaevolution.Plan,
) []schemaevolution.IncompatibleColumnError {
	pkSet := make(map[string]bool)
	comparesNonPK := false
	if asset != nil {
		comparesNonPK = asset.Materialization.Strategy == pipeline.MaterializationStrategySCD2ByColumn
		for _, col := range asset.Columns {
			if col.PrimaryKey {
				pkSet[dialect.ColumnKey(col.Name)] = true
			}
		}
	}

	existingByKey := make(map[string]schemaevolution.Column, len(existing))
	for _, col := range existing {
		if k := dialect.ColumnKey(col.Name); k != "" {
			existingByKey[k] = col
		}
	}
	alteredTypeByKey := make(map[string]string, len(plan.Actions))
	for _, a := range plan.Actions {
		if a.Type == schemaevolution.ActionAlterColumnType {
			alteredTypeByKey[dialect.ColumnKey(a.ColumnName)] = a.ToType
		}
	}

	var (
		pairs        []scd2DiffPair
		incompatible []schemaevolution.IncompatibleColumnError
		seen         = make(map[string]bool, len(incoming))
	)

	for _, incCol := range incoming {
		key := dialect.ColumnKey(incCol.Name)
		if key == "" || seen[key] || incCol.UntypedNull {
			continue
		}
		seen[key] = true
		existingCol, ok := existingByKey[key]
		if !ok {
			continue
		}

		postAlterType, widened := alteredTypeByKey[key]
		if !widened {
			postAlterType = existingCol.Type
		}

		cur := dialect.ParseType(existingCol.Type)
		post := dialect.ParseType(postAlterType)
		inc := dialect.ParseType(incCol.Type)
		postFmt := dialect.FormatType(post)
		incFmt := dialect.FormatType(inc)

		if postFmt == incFmt || incFmt == "" || inc.IsNull() {
			continue
		}
		if cur.DType == sqlengine.DT_VARCHAR && inc.DType != sqlengine.DT_VARCHAR && inc.DType != sqlengine.DT_ENUM {
			if comparesNonPK || pkSet[key] || inc.HasNestedNull() {
				incompatible = append(incompatible, schemaevolution.IncompatibleColumnError{
					ColumnName:   existingCol.Name,
					CurrentType:  dialect.FormatType(cur),
					IncomingType: incFmt,
					Reason: fmt.Sprintf(
						"SCD2 compares and UNION ALLs existing VARCHAR values against %s; explicitly CAST the column to VARCHAR in the query",
						incFmt,
					),
				})
				continue
			}
		}
		if inc.HasNestedNull() {
			continue
		}

		pairs = append(pairs, scd2DiffPair{
			existing:      existingCol,
			incoming:      incCol,
			cur:           cur,
			postFmt:       postFmt,
			incFmt:        incFmt,
			postAlterType: postAlterType,
			widened:       widened,
		})
	}

	if len(pairs) == 0 {
		return incompatible
	}

	unionTypes := resolveDuckDBUnionTypes(ctx, session, pairs)
	for i, pair := range pairs {
		unionType := unionTypes[i]
		if unionType == "" {
			incompatible = append(incompatible, schemaevolution.IncompatibleColumnError{
				ColumnName:   pair.existing.Name,
				CurrentType:  dialect.FormatType(pair.cur),
				IncomingType: pair.incFmt,
				Reason: fmt.Sprintf(
					"SCD2 rebuilds the table via UNION ALL, which cannot combine %s and %s; explicitly CAST the column to %s in the query",
					pair.postFmt, pair.incFmt, pair.postFmt,
				),
			})
			continue
		}
		unionFmt := dialect.NormalizeType(unionType)
		if unionFmt == pair.postFmt {
			continue
		}
		if !pair.widened && pair.cur.DType != sqlengine.DT_VARCHAR {
			widenRes, widenTarget, _ := dialect.ResolveTypeChange(pair.existing, schemaevolution.Column{
				Name: pair.incoming.Name,
				Type: unionType,
			})
			if widenRes == schemaevolution.TypeChangeWiden && dialect.NormalizeType(widenTarget) == unionFmt {
				alteredCol := pair.existing
				alteredCol.Type = widenTarget
				plan.Actions = append(plan.Actions, schemaevolution.Action{
					Type:       schemaevolution.ActionAlterColumnType,
					ColumnName: pair.existing.Name,
					FromType:   dialect.FormatType(pair.cur),
					ToType:     widenTarget,
					Column:     alteredCol,
				})
				continue
			}
		}
		incompatible = append(incompatible, schemaevolution.IncompatibleColumnError{
			ColumnName:   pair.existing.Name,
			CurrentType:  dialect.FormatType(pair.cur),
			IncomingType: pair.incFmt,
			Reason: fmt.Sprintf(
				"SCD2 rebuilds the table via UNION ALL, which would change %s to %s; explicitly CAST the column to %s in the query",
				pair.postFmt, unionFmt, pair.postFmt,
			),
		})
	}

	return incompatible
}

func resolveDuckDBUnionTypes(ctx context.Context, session duckDBSession, pairs []scd2DiffPair) []string {
	leftExprs := make([]string, len(pairs))
	rightExprs := make([]string, len(pairs))
	for i, p := range pairs {
		leftExprs[i] = fmt.Sprintf("CAST(NULL AS %s) AS __c%d", p.postAlterType, i)
		rightExprs[i] = fmt.Sprintf("CAST(NULL AS %s) AS __c%d", p.incoming.Type, i)
	}
	batchSQL := fmt.Sprintf("DESCRIBE SELECT %s UNION ALL SELECT %s", strings.Join(leftExprs, ", "), strings.Join(rightExprs, ", "))
	out := make([]string, len(pairs))
	if rows, err := session.Select(ctx, batchSQL); err == nil && len(rows) == len(pairs) {
		for i, row := range rows {
			if len(row) >= 2 {
				out[i], _ = row[1].(string)
			}
		}
		return out
	}

	for i, p := range pairs {
		singleSQL := fmt.Sprintf("DESCRIBE SELECT CAST(NULL AS %s) AS x UNION ALL SELECT CAST(NULL AS %s)", p.postAlterType, p.incoming.Type)
		if rows, err := session.Select(ctx, singleSQL); err == nil && len(rows) > 0 && len(rows[0]) >= 2 {
			out[i], _ = rows[0][1].(string)
		}
	}
	return out
}

// checkDuckDBAlterConstraints lazily inspects DuckDB catalog metadata ONLY when plan
// contains at least one ActionAlterColumnType.
func checkDuckDBAlterConstraints(
	ctx context.Context,
	session duckDBSession,
	tableName string,
	plan *schemaevolution.Plan,
) []schemaevolution.IncompatibleColumnError {
	if plan.IsEmpty() {
		return nil
	}
	var alterActions []schemaevolution.Action
	for _, a := range plan.Actions {
		if a.Type == schemaevolution.ActionAlterColumnType {
			alterActions = append(alterActions, a)
		}
	}
	if len(alterActions) == 0 {
		return nil
	}

	catalog, schema, table := resolveCatalogSchemaTable(ctx, session, tableName)
	dbExpr := "current_database()"
	if catalog != "" {
		dbExpr = quoteSQLStringLiteral(catalog)
	}
	escapedSchema := escapeSQLStringLiteral(schema)
	escapedTable := escapeSQLStringLiteral(table)

	blockAll := func(reason string) []schemaevolution.IncompatibleColumnError {
		changes := make([]schemaevolution.IncompatibleColumnError, len(alterActions))
		for i, a := range alterActions {
			changes[i] = schemaevolution.IncompatibleColumnError{
				ColumnName:   a.ColumnName,
				CurrentType:  a.FromType,
				IncomingType: a.ToType,
				Reason:       reason,
			}
		}
		return changes
	}

	indexSQL := fmt.Sprintf(
		"SELECT COUNT(*) FROM duckdb_indexes() WHERE lower(database_name) = lower(%s) AND lower(schema_name) = lower('%s') AND lower(table_name) = lower('%s')",
		dbExpr, escapedSchema, escapedTable,
	)
	if rows, err := session.Select(ctx, indexSQL); err == nil && len(rows) > 0 && len(rows[0]) > 0 {
		if count, ok := asInt64(rows[0][0]); ok && count > 0 {
			return blockAll(fmt.Sprintf("cannot change column type on table %q because an index depends on it in DuckDB", tableName))
		}
	}

	refFKSQL := fmt.Sprintf(
		`SELECT COUNT(*)
		 FROM information_schema.referential_constraints rc
		 JOIN information_schema.table_constraints tc
		   ON rc.unique_constraint_catalog = tc.constraint_catalog
		  AND rc.unique_constraint_schema = tc.constraint_schema
		  AND rc.unique_constraint_name = tc.constraint_name
		 WHERE lower(tc.table_catalog) = lower(%s)
		   AND lower(tc.table_schema) = lower('%s')
		   AND lower(tc.table_name) = lower('%s')`,
		dbExpr, escapedSchema, escapedTable,
	)
	if rows, err := session.Select(ctx, refFKSQL); err == nil && len(rows) > 0 && len(rows[0]) > 0 {
		if count, ok := asInt64(rows[0][0]); ok && count > 0 {
			return blockAll(fmt.Sprintf("cannot change column type on table %q because a FOREIGN KEY constraint references it in DuckDB", tableName))
		}
	}

	constraintSQL := fmt.Sprintf(
		"SELECT DISTINCT constraint_type, unnest(constraint_column_names) FROM duckdb_constraints() WHERE constraint_type IN ('FOREIGN KEY', 'CHECK') AND lower(database_name) = lower(%s) AND lower(schema_name) = lower('%s') AND lower(table_name) = lower('%s')",
		dbExpr, escapedSchema, escapedTable,
	)
	fkCols := make(map[string]bool)
	checkCols := make(map[string]bool)
	if rows, err := session.Select(ctx, constraintSQL); err == nil {
		for _, row := range rows {
			if len(row) < 2 {
				continue
			}
			cType, _ := row[0].(string)
			cCol, _ := row[1].(string)
			if cCol == "" {
				continue
			}
			switch cType {
			case "FOREIGN KEY":
				fkCols[strings.ToLower(cCol)] = true
			case "CHECK":
				checkCols[strings.ToLower(cCol)] = true
			}
		}
	}

	var incompatible []schemaevolution.IncompatibleColumnError
	for _, a := range alterActions {
		lowerCol := strings.ToLower(a.ColumnName)
		var reason string
		switch {
		case fkCols[lowerCol]:
			reason = "cannot change type of a column with a FOREIGN KEY constraint in DuckDB"
		case checkCols[lowerCol]:
			reason = "cannot change type of a column with a CHECK constraint in DuckDB"
		}
		if reason != "" {
			incompatible = append(incompatible, schemaevolution.IncompatibleColumnError{
				ColumnName:   a.ColumnName,
				CurrentType:  a.FromType,
				IncomingType: a.ToType,
				Reason:       reason,
			})
		}
	}
	return incompatible
}

// resolveCatalogSchemaTable resolves tableName to (catalog, schema, table).
// For two-part names (a.b), DuckDB resolves schema.table in the current database first,
// and falls back to catalog.main.table when no schema.table exists in the current database.
func resolveCatalogSchemaTable(ctx context.Context, session duckDBSession, tableName string) (string, string, string) {
	catalog, schema, table, partCount := schemaevolution.SplitTableIdentifier(duckDBSQLDialect, tableName, "main")
	if catalog != "" || partCount != 2 {
		return catalog, schema, table
	}
	checkSQL := fmt.Sprintf(
		"SELECT COUNT(*) FROM duckdb_tables() WHERE lower(database_name) = lower(current_database()) AND lower(schema_name) = lower('%s') AND lower(table_name) = lower('%s')",
		escapeSQLStringLiteral(schema),
		escapeSQLStringLiteral(table),
	)
	if rows, err := session.Select(ctx, checkSQL); err == nil && len(rows) > 0 && len(rows[0]) > 0 {
		if count, ok := asInt64(rows[0][0]); ok && count == 0 {
			return schema, "main", table
		}
	}
	return catalog, schema, table
}

func asInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case int32:
		return int64(n), true
	case int:
		return int64(n), true
	case uint64:
		return int64(n), true //nolint:gosec
	default:
		return 0, false
	}
}

func (e *duckDBEvolutionEngine) QueryColumns(ctx context.Context, queryStr string) ([]schemaevolution.Column, error) {
	trimmed := strings.TrimSpace(queryStr)
	if trimmed == "" {
		return nil, nil
	}
	if schemaevolution.IsBareValuesQuery(duckDBSQLDialect, trimmed) {
		return nil, errors.New("bare VALUES queries are not supported with schema_evolution because INSERT BY NAME requires named SELECT columns; wrap the query in SELECT ... AS <column_name>")
	}
	rows, err := e.session.Select(ctx, "DESCRIBE\n"+trimmed)
	if err != nil {
		return nil, err
	}
	cols := parseDescribeRows(rows)
	hints := schemaevolution.ApplyUntypedSelectLiterals(duckDBTypeFormatConfig, trimmed, cols)

	var candidates []int
	for i := range cols {
		if cols[i].UntypedNull {
			continue
		}
		key := e.ColumnKey(cols[i].Name)
		if hints.EmptyArrays[key] > 0 || hints.ConcreteScalars[key] {
			continue
		}
		if strings.Contains(strings.ToUpper(cols[i].Type), "INTEGER") {
			candidates = append(candidates, i)
		}
	}
	if len(candidates) > 0 {
		e.detectUntypedNullColumns(ctx, trimmed, cols, candidates)
	}

	return cols, nil
}

// detectUntypedNullColumns identifies untyped NULL / empty-list columns (and nested untyped
// NULLs inside STRUCT/MAP/UNION) among candidates in a single zero-scan query:
// SELECT typeof(first("c1")), ... FROM (<subquery>) WHERE FALSE.
func (e *duckDBEvolutionEngine) detectUntypedNullColumns(ctx context.Context, queryStr string, cols []schemaevolution.Column, candidates []int) {
	subquery := schemaevolution.StripTrailingSemicolonsAndComments(duckDBSQLDialect, queryStr)
	if subquery == "" {
		return
	}

	exprs := make([]string, len(candidates))
	for i, colIdx := range candidates {
		exprs[i] = fmt.Sprintf("typeof(first(%s))", schemaevolution.QuoteDoubleQuotedIdentifier(cols[colIdx].Name))
	}
	probeSQL := fmt.Sprintf(
		"SELECT %s FROM (\n%s\n) AS __bruin_se_probe WHERE FALSE",
		strings.Join(exprs, ", "),
		subquery,
	)

	rows, err := e.session.Select(ctx, probeSQL)
	if err != nil {
		logVerboseNote(ctx, fmt.Sprintf("Schema evolution untyped-NULL probe skipped: %v", err))
		return
	}
	if len(rows) == 0 {
		return
	}
	row := rows[0]
	for i, colIdx := range candidates {
		if i >= len(row) {
			break
		}
		typeStr, ok := row[i].(string)
		if !ok {
			continue
		}
		if isDuckDBUntypedNullTypeof(typeStr) {
			cols[colIdx].UntypedNull = true
			continue
		}
		if typeStr != cols[colIdx].Type && strings.Contains(typeStr, `"NULL"`) && e.ParseType(typeStr).HasNestedNull() {
			cols[colIdx].Type = typeStr
		}
	}
}

func isDuckDBUntypedNullTypeof(typeStr string) bool {
	trimmed := strings.TrimSpace(typeStr)
	return trimmed == `"NULL"` || strings.EqualFold(trimmed, "SQLNULL")
}

func logVerboseNote(ctx context.Context, msg string) {
	verbose, ok := ctx.Value(executor.KeyVerbose).(bool)
	if !ok || !verbose {
		return
	}
	if w, ok := ctx.Value(executor.KeyPrinter).(io.Writer); ok && w != nil {
		_, _ = fmt.Fprintln(w, msg)
	}
}

// DeclaredColumns extracts and canonicalizes full declared table schemas for strategies
// that derive their table schema strictly from asset.Columns (ddl and datavault_*).
func (e *duckDBEvolutionEngine) DeclaredColumns(ctx context.Context, asset *pipeline.Asset) ([]schemaevolution.Column, error) {
	if asset == nil || len(asset.Columns) == 0 {
		return nil, nil
	}

	strategy := asset.Materialization.Strategy
	switch {
	case strategy == pipeline.MaterializationStrategyDDL:
		return e.resolveDeclaredColumnTypes(ctx, schemaevolution.ColumnsFromAsset(asset))

	case strategy.IsDataVault():
		cols, err := duckDBDataVaultDeclaredColumns(asset)
		if err != nil {
			return nil, err
		}
		return e.resolveDeclaredColumnTypes(ctx, cols)

	default:
		return nil, fmt.Errorf("unexpected DeclaredColumns call for strategy %q", strategy)
	}
}

func duckDBDataVaultDeclaredColumns(asset *pipeline.Asset) ([]schemaevolution.Column, error) {
	var (
		primaryKeys      []string
		mandatoryColumns []*pipeline.Column
	)

	switch asset.Materialization.Strategy { //nolint:exhaustive
	case pipeline.MaterializationStrategyDataVaultHub:
		hashKey, businessKeys, loadDatetime, recordSource, err := resolveDataVaultHubColumns(asset)
		if err != nil {
			return nil, err
		}
		primaryKeys = []string{hashKey.Name}
		mandatoryColumns = append([]*pipeline.Column{hashKey, loadDatetime, recordSource}, businessKeys...)

	case pipeline.MaterializationStrategyDataVaultLink:
		linkHashKey, relatedHashKeys, loadDatetime, recordSource, err := resolveDataVaultLinkColumns(asset)
		if err != nil {
			return nil, err
		}
		primaryKeys = []string{linkHashKey.Name}
		mandatoryColumns = append([]*pipeline.Column{linkHashKey, loadDatetime, recordSource}, relatedHashKeys...)

	case pipeline.MaterializationStrategyDataVaultSatellite:
		parentHashKey, hashDiff, loadDatetime, recordSource, err := resolveDataVaultSatelliteColumns(asset)
		if err != nil {
			return nil, err
		}
		primaryKeys = dataVaultSatellitePrimaryKeys(asset, parentHashKey, loadDatetime)
		mandatoryColumns = []*pipeline.Column{parentHashKey, hashDiff, loadDatetime, recordSource}

	default:
		return nil, fmt.Errorf("unsupported Data Vault strategy %q", asset.Materialization.Strategy)
	}

	pkSet := make(map[string]bool, len(primaryKeys))
	for _, pk := range primaryKeys {
		pkSet[strings.ToLower(strings.TrimSpace(pk))] = true
	}
	mandatorySet := make(map[string]bool, len(mandatoryColumns))
	for _, col := range mandatoryColumns {
		if col != nil {
			mandatorySet[strings.ToLower(strings.TrimSpace(col.Name))] = true
		}
	}

	cols := make([]schemaevolution.Column, 0, len(asset.Columns))
	for _, col := range asset.Columns {
		name := strings.TrimSpace(col.Name)
		if name == "" {
			continue
		}
		lowerName := strings.ToLower(name)
		cols = append(cols, schemaevolution.Column{
			Name:       name,
			Type:       strings.TrimSpace(col.Type),
			NotNull:    mandatorySet[lowerName] || !col.Nullable.Bool(),
			PrimaryKey: pkSet[lowerName],
		})
	}
	return cols, nil
}

// resolveDeclaredColumnTypes asks DuckDB to canonicalize declared SQL types (such as DOUBLE PRECISION,
// TIMESTAMP WITHOUT TIME ZONE, STRUCT(a INT), or MAP(VARCHAR, INT)) so comparisons match DESCRIBE output.
func (e *duckDBEvolutionEngine) resolveDeclaredColumnTypes(ctx context.Context, cols []schemaevolution.Column) ([]schemaevolution.Column, error) {
	if len(cols) == 0 {
		return cols, nil
	}

	selectExprs := make([]string, 0, len(cols))
	indices := make([]int, 0, len(cols))
	for i, col := range cols {
		if strings.TrimSpace(col.Type) == "" {
			continue
		}
		selectExprs = append(selectExprs, fmt.Sprintf("CAST(NULL AS %s) AS __c%d", col.Type, i))
		indices = append(indices, i)
	}
	if len(selectExprs) == 0 {
		return cols, nil
	}

	rows, err := e.session.Select(ctx, "DESCRIBE SELECT "+strings.Join(selectExprs, ", "))
	if err != nil {
		return nil, err
	}

	resolved := make([]schemaevolution.Column, len(cols))
	copy(resolved, cols)
	for rowIdx, row := range rows {
		if rowIdx >= len(indices) || len(row) < 2 {
			continue
		}
		if colType, ok := row[1].(string); ok && strings.TrimSpace(colType) != "" {
			resolved[indices[rowIdx]].Type = colType
		}
	}
	return resolved, nil
}

func parseDescribeRows(rows [][]interface{}) []schemaevolution.Column {
	cols := make([]schemaevolution.Column, 0, len(rows))
	for _, row := range rows {
		if len(row) < 2 {
			continue
		}
		name, ok1 := row[0].(string)
		colType, ok2 := row[1].(string)
		if !ok1 || !ok2 {
			continue
		}
		col := schemaevolution.Column{
			Name: name,
			Type: colType,
		}
		if len(row) >= 4 {
			if keyFlag, ok := row[3].(string); ok {
				switch strings.ToUpper(strings.TrimSpace(keyFlag)) {
				case "PRI":
					col.PrimaryKey = true
				case "UNI":
					col.Unique = true
				}
			}
		}
		cols = append(cols, col)
	}
	return cols
}

func isDuckDBTableNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "table with name") && strings.Contains(msg, "does not exist")
}
