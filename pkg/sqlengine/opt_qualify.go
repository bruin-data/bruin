package sqlengine

// Port of sqlglot/optimizer/qualify.py.

// QualifyOptions mirrors the keyword arguments of sqlglot.optimizer.qualify.qualify.
//
// Go booleans cannot express "not passed", so:
//   - DefaultQualifyOptions() returns the Python defaults for every option.
//   - Defaults: true forces the options whose Python default is True and that callers normally
//     leave alone (ExpandAliasRefs, ExpandStars, QualifyColumns, QuoteIdentifiers) to true.
//     ValidateQualifyColumns and Identify are always taken as given (pass true to get the
//     Python default), and options whose Python default is False/None are taken as given.
//
// lineage.go relies on the latter: Qualify(e, QualifyOptions{..., ValidateQualifyColumns: false,
// Identify: false, Defaults: true}) mirrors qualify(e, dialect=..., schema=...,
// validate_qualify_columns=False, identify=False).
type QualifyOptions struct {
	Dialect *Dialect
	// Db and Catalog are the default database/catalog names ("" means None).
	Db      string
	Catalog string
	Schema  *MappingSchema

	ExpandAliasRefs bool
	ExpandStars     bool
	// InferSchema mirrors `infer_schema: bool | None` (TriNone = None).
	InferSchema               Tri
	IsolateTables             bool
	QualifyColumns            bool
	AllowPartialQualification bool
	ValidateQualifyColumns    bool
	QuoteIdentifiers          bool
	Identify                  bool
	CanonicalizeTableAliases  bool
	// OnQualify is called after a table has been qualified (may be nil).
	OnQualify func(table *Expr)
	// SQL is the original SQL string for error highlighting ("" means None).
	SQL string

	// Defaults applies the Python defaults described above.
	Defaults bool
}

// DefaultQualifyOptions returns the Python default values of qualify's keyword arguments.
func DefaultQualifyOptions() QualifyOptions {
	return QualifyOptions{
		ExpandAliasRefs:           true,
		ExpandStars:               true,
		InferSchema:               TriNone,
		IsolateTables:             false,
		QualifyColumns:            true,
		AllowPartialQualification: false,
		ValidateQualifyColumns:    true,
		QuoteIdentifiers:          true,
		Identify:                  true,
		CanonicalizeTableAliases:  false,
	}
}

// Qualify mirrors sqlglot.optimizer.qualify.qualify: rewrite the AST to have normalized and
// qualified tables and columns. This step is necessary for all further SQLGlot optimizations.
//
// Errors are raised as panics (*OptimizeError, *SchemaError, *ValueError ...), like the Python exceptions.
func Qualify(expression *Expr, o QualifyOptions) *Expr {
	if o.Defaults {
		o.ExpandAliasRefs = true
		o.ExpandStars = true
		o.QualifyColumns = true
		o.QuoteIdentifiers = true
	}

	schema := o.Schema
	if schema == nil {
		schema = NewMappingSchema(nil, nil, o.Dialect, true, nil)
	}
	dialect := o.Dialect
	if dialect == nil {
		dialect = prototype("")
	}

	expression = NormalizeIdentifiersFull(expression, dialect, true)
	expression = QualifyTables(expression, o.Db, o.Catalog, o.OnQualify, dialect, o.CanonicalizeTableAliases)

	if o.IsolateTables {
		expression = IsolateTableSelects(expression, schema, nil)
	}

	if o.QualifyColumns {
		expression = QualifyColumns(
			expression,
			schema,
			o.ExpandAliasRefs,
			o.ExpandStars,
			o.InferSchema,
			o.AllowPartialQualification,
			nil,
		)
	}

	if o.QuoteIdentifiers {
		expression = QuoteIdentifiers(expression, dialect, o.Identify)
	}

	if o.ValidateQualifyColumns {
		ValidateQualifyColumns(expression, o.SQL)
	}

	return expression
}
