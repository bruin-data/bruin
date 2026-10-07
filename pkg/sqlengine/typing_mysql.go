package sqlengine

// Port of sqlglot/typing/mysql.py.

func typingMySQLAnnotateReverse(a *TypeAnnotator, expression *Expr) *Expr {
	if expression.This().IsTypeOf(DT_BINARY, DT_VARBINARY, DT_UNKNOWN) {
		a.annotateByArgs(expression, "this")
	} else {
		a.setType(expression, DT_VARCHAR)
	}

	return expression
}

func typingMySQLAnnotateTruncate(a *TypeAnnotator, expression *Expr) *Expr {
	if annIsTypeSet(expression.This(), DataType_TEXT_TYPES) {
		return a.setType(expression, DT_DOUBLE)
	}

	return a.annotateByArgs(expression, "this")
}

func typingMySQLMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.returns(
		DT_DOUBLE,
		KAtan2,
	)
	m.returns(
		DT_DATETIME,
		KCurrentTimestamp,
		KLocaltime,
		KLocaltimestamp,
	)
	m.returns(
		DT_VARCHAR,
		KElt,
		KHex,
		KNumberToStr, // format()
		KReplace,
		KStuff, // insert function
	)
	m.returns(
		DT_INT,
		KMonth,
		KSecond,
		KWeek,
		KMinute,
	)
	m.returns(
		DT_TIME,
		KTimeFromParts,
	)
	m.annotators(
		"typing/mysql.py:70", func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KPad,
		KLeft,
		KRight,
	)
	m.annotators("typing/mysql.py:12", func(a *TypeAnnotator, e *Expr) { typingMySQLAnnotateReverse(a, e) }, KReverse)
	m.annotators("typing/mysql.py:21", func(a *TypeAnnotator, e *Expr) { typingMySQLAnnotateTruncate(a, e) }, KTrunc)

	return m
}
