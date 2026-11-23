package sqlengine

// QualifyOptions configures Qualify.
//
// Go booleans cannot express "not passed", so:
//   - DefaultQualifyOptions() returns the default for every option.
//   - Defaults: true forces the options that default to true and that callers normally leave
//     alone (ExpandAliasRefs, ExpandStars, QualifyColumns, QuoteIdentifiers) to true.
//     ValidateQualifyColumns and Identify are always taken as given (pass true to get the
//     default), and options that default to false or unset are taken as given.
//
// lineage.go relies on the latter: Qualify(e, QualifyOptions{..., ValidateQualifyColumns: false,
// Identify: false, Defaults: true}) qualifies with the defaults except that columns are not
// validated and only identifiers that need quoting are quoted.
type QualifyOptions struct {
	Dialect *Dialect
	// Db and Catalog are the default database/catalog names ("" means unset).
	Db      string
	Catalog string
	Schema  *MappingSchema

	ExpandAliasRefs bool
	ExpandStars     bool
	// InferSchema controls schema inference; TriNone infers it only when the schema is empty.
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
	// SQL is the original SQL string for error highlighting ("" means unavailable).
	SQL string

	Defaults bool
}

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

// Qualify rewrites the AST to have normalized and qualified tables and columns. The other
// optimizer rules depend on this step.
//
// Errors are raised as panics (*OptimizeError, *SchemaError, *ValueError ...).
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
