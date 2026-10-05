package sqlengine

// Port of sqlglot/typing/spark2.py.

// typingSpark2AnnotateBySimilarArgs is the type inference for CONCAT-family expressions
// (CONCAT, LPAD, RPAD).
//
//   - All-BINARY → BINARY (the binary overload).
//   - Otherwise, if any arg has a known, non-array, non-binary type → STRING.
//     Spark coerces scalars (dates, ints, etc.) to string when mixed with a
//     string-resolving arg. The binary exclusion preserves the binary+unknown
//     case as UNKNOWN: Spark can't disambiguate the string vs. binary overload
//     there.
//   - Else → UNKNOWN. Covers all-unknown, binary+unknown, and anything
//     involving arrays (array handling is intentionally out of scope here).
func typingSpark2AnnotateBySimilarArgs(a *TypeAnnotator, expression *Expr, argKeys ...string) *Expr {
	var argExprs []*Expr
	for _, key := range argKeys {
		argExprs = append(argExprs, annEnsureList(expression.Arg(key))...)
	}

	allBinary := len(argExprs) > 0
	for _, e := range argExprs {
		if !e.IsTypeOf(DT_BINARY) {
			allBinary = false
			break
		}
	}

	var result any
	if allBinary {
		result = DT_BINARY
	} else {
		anyKnown := false
		for _, e := range argExprs {
			if e.Type() != nil && !e.IsTypeOf(DT_UNKNOWN, DT_ARRAY, DT_BINARY) {
				anyKnown = true
				break
			}
		}
		if anyKnown {
			result = DT_TEXT
		} else {
			result = DT_UNKNOWN
		}
	}

	a.setType(expression, result)
	return expression
}

func typingSpark2Metadata() ExprMetadataType {
	m := typingMetadata("hive").copy()

	m.returns(
		DT_DOUBLE,
		KAtan2,
		KRandn,
	)
	m.returns(
		DT_VARCHAR,
		KFormat,
		KRight,
	)
	m.annotators(
		"typing/spark2.py:63", func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KArrayFilter,
		KSubstring,
	)
	m.returns(DT_DATE, KAddMonths)
	m.annotators("typing/spark2.py:71", func(a *TypeAnnotator, e *Expr) {
		a.annotateByArgsFull(e, false, e.argKeyAttr("quantile", "is_type").IsTypeOf(DT_ARRAY), "this")
	}, KApproxQuantile)
	m.returns(DT_TIMESTAMP, KAtTimeZone)
	m.annotators("typing/spark2.py:76", func(a *TypeAnnotator, e *Expr) {
		typingSpark2AnnotateBySimilarArgs(a, e, "expressions")
	}, KConcat)
	m.returns(DT_DATE, KNextDay)
	m.annotators("typing/spark2.py:79", func(a *TypeAnnotator, e *Expr) {
		typingSpark2AnnotateBySimilarArgs(a, e, "this", "fill_pattern")
	}, KPad)

	return m
}
