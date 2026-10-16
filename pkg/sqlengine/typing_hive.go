package sqlengine

// Port of sqlglot/typing/hive.py.

func typingHiveMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.returns(
		DT_BINARY,
		KEncode,
		KUnhex,
	)
	m.returns(
		DT_DOUBLE,
		KCorr,
		KMonthsBetween,
	)
	m.returns(
		DT_VARCHAR,
		KAddMonths,
		KCurrentDatabase,
		KCurrentUser,
		KCurrentSchema,
		KHex,
		KJSONExtractScalar,
		KJSONFormat,
		KNextDay,
		KRegexpExtract,
		KRegexpReplace,
		KReplace,
		KSoundex,
	)
	m.returns(
		DT_BIGINT,
		KFactorial,
		KIntDiv,
		KStrToUnix,
	)
	m.returns(
		DT_INT,
		KDenseRank,
		KMonth,
		KNtile,
		KRank,
		KRowNumber,
		KSecond,
		KMinute,
	)
	m.returns(DT_DOUBLE, KPercentileDisc)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KArrayDistinct,
		KArrayExcept,
		KFirst,
		KLast,
		KReverse,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "quantile") }, KApproxQuantile)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") }, KWithinGroup)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "expressions") }, KArrayIntersect)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.annotateByArgsFull(e, true, false, "this", "expressions")
	}, KCoalesce)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.annotateByArgsFull(e, true, false, "true", "false")
	}, KIf)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "quantile") }, KQuantile)
	m.returns(DataTypeBuild("ARRAY<STRING>", nil, false, true), KRegexpSplit)
	m.returns(DataTypeBuild("MAP<STRING, STRING>", nil, false, true), KStrToMap)

	return m
}
