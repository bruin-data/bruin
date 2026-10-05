package sqlengine

// Port of sqlglot/typing/spark.py.

func typingSparkMetadata() ExprMetadataType {
	m := typingMetadata("spark2").copy()

	m.returns(
		DT_DOUBLE,
		KSec,
	)
	m.returns(
		DT_INT,
		KArraySize,
	)
	m.returns(
		DT_VARCHAR,
		KCollation,
		KCurrentTimezone,
		KRandstr,
	)
	m.annotators(
		"typing/spark.py:30", func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KArrayCompact,
		KArrayInsert,
		KBitwiseAndAgg,
		KBitwiseOrAgg,
		KBitwiseXorAgg,
		KOverlay,
	)
	m.returns(DT_BIGINT, KBitmapCount)
	m.returns(DT_TIMESTAMPNTZ, KLocaltimestamp)
	m.returns(DT_BINARY, KToBinary)
	m.returns(DT_DATE, KDateFromUnixDate)
	// 2-arg `date_add(startDate, numDays)` / `date_sub` are routed to
	// TsOrDsAdd by Hive/Spark parsers; both return DATE per the Spark
	// and Databricks contracts.
	m.returns(DT_DATE, KTsOrDsAdd)

	return m
}
