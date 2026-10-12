package sqlengine

// Port of sqlglot/typing/clickhouse.py.

func typingClickHouseMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.returns(
		DT_UBIGINT,
		KCountIf,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("FixedString(16)", MustDialect("clickhouse"), false, true))
	}, KMD5Digest)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("Float64", MustDialect("clickhouse"), false, true))
	}, KCorr)

	return m
}
