package sqlengine

// Port of sqlglot/dialects/bigquery.py (class BigQuery), the class bodies of
// sqlglot/parsers/bigquery.py (BigQueryParser) and sqlglot/generators/bigquery.py
// (BigQueryGenerator). Data-only class attributes are generated (zz_*_settings.go); this file
// wires the callable tables and method overrides. Parser code lives in d_bigquery_parser.go and
// generator code in d_bigquery_generator.go.

func init() { registerCustomizer("bigquery", customizeBigQuery) }

func customizeBigQuery(d *Dialect) {
	// Dialect class method overrides
	d.hooks.normalizeIdentifier = bigqueryNormalizeIdentifier

	customizeBigQueryParser(d)
	customizeBigQueryGenerator(d)
}

// bigqueryNormalizeIdentifier mirrors BigQuery.normalize_identifier.
func bigqueryNormalizeIdentifier(d *Dialect, expression *Expr) *Expr {
	if expression.IsA(KIdentifier) && d.NormalizationStrategy == NormCaseInsensitive {
		parent := expression.Parent()
		for parent.IsA(KDot) {
			parent = parent.Parent()
		}

		// In BigQuery, CTEs are case-insensitive, but UDF and table names are case-sensitive
		// by default. The following check uses a heuristic to detect tables based on whether
		// they are qualified. This should generally be correct, because tables in BigQuery
		// must be qualified with at least a dataset, unless @@dataset_id is set.
		caseSensitive := parent.IsA(KUserDefinedFunction) ||
			(parent.IsA(KTable) &&
				parent.DbName() != "" &&
				(truthy(parent.MetaGet("quoted_table")) || !truthy(parent.MetaGet("maybe_column")))) ||
			truthy(expression.MetaGet("is_table"))
		if !caseSensitive {
			expression.Set("this", pyLower(expression.ThisS()))
		}

		return expression
	}

	return d.baseNormalizeIdentifier(expression)
}
