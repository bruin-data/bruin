package sqlengine

// Port of sqlglot/typing/tsql.py.

func typingTSQLMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.returns(
		DT_FLOAT,
		KAcos,
		KAsin,
		KAtan,
		KAtan2,
		KCos,
		KCot,
		KSin,
		KTan,
	)
	m.returns(
		DT_VARCHAR,
		KSoundex,
		KStuff,
	)
	m.annotators(
		"typing/tsql.py:29", func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KDegrees,
		KRadians,
	)
	m.returns(DT_NVARCHAR, KCurrentTimezone)
	m.returns(DT_DATETIME, KCurrentTimestamp)

	return m
}
