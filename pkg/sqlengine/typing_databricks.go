package sqlengine

// Port of sqlglot/typing/databricks.py.

func typingDatabricksMetadata() ExprMetadataType {
	m := typingMetadata("spark").copy()

	m.returns(
		DT_DOUBLE,
		KRegrAvgx,
		KRegrAvgy,
		KRegrIntercept,
		KRegrR2,
		KRegrSlope,
		KRegrSxx,
		KRegrSxy,
		KRegrSyy,
		KRint,
	)
	m.returns(
		DT_INT,
		KRegexpCount,
		KRegexpInstr,
	)
	m.returns(DT_VARCHAR, KRegexpSubstr)
	m.returns(DT_BIGINT, KRegrCount)
	m.returns(DT_BOOLEAN, KSearch)
	m.returns(DT_VARCHAR, KTrim)
	m.annotators("typing/databricks.py:34", func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<STRING>", MustDialect("databricks"), false, true))
	}, KRegexpExtractAll)

	return m
}
