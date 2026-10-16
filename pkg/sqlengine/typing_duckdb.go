package sqlengine

// Port of sqlglot/typing/duckdb.py.

func typingDuckDBMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.returns(
		DT_BIGINT,
		KBitLength,
		KDateDiff,
		KDay,
		KDayOfMonth,
		KDayOfWeek,
		KDayOfWeekIso,
		KDayOfYear,
		KExtract,
		KHour,
		KLength,
		KMinute,
		KMonth,
		KQuarter,
		KSecond,
		KWeek,
		KYear,
	)
	m.returns(
		DT_INT128,
		KCountIf,
		KFactorial,
	)
	m.returns(
		DT_DOUBLE,
		KAtan2,
		KJarowinklerSimilarity,
		KTimeToUnix,
	)
	m.returns(
		DT_VARCHAR,
		KFormat,
		KReverse,
		KDecode,
	)
	m.returns(
		DT_VARBINARY,
		KEncode,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "expression") }, KDateBin)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") }, KPercentileDisc)
	m.returns(DT_TIMESTAMP, KLocaltimestamp)
	m.returns(DT_INTERVAL, KToDays)
	m.returns(DT_TIME, KTimeFromParts)

	return m
}
