package sqlengine

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
	m.returns(DT_TIMESTAMP, KTimestampTrunc)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<STRING>", MustDialect("databricks"), false, true))
	}, KRegexpExtractAll)

	return m
}
