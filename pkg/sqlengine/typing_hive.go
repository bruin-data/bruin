package sqlengine

// typingHiveAnnotateFloorCeil types FLOOR and CEIL as BIGINT, or as UNKNOWN when they round to a
// number of decimals or their argument is a DECIMAL, of unknown type or a numeric literal: those
// results need a precision inference that is not modeled.
func typingHiveAnnotateFloorCeil(a *TypeAnnotator, e *Expr) {
	this := e.This()
	if e.ArgE("decimals") != nil || this.IsTypeOf(DT_DECIMAL, DT_UNKNOWN) || this.Unnest().IsNumber() {
		a.setType(e, DT_UNKNOWN)
		return
	}
	a.setType(e, DT_BIGINT)
}

func typingHiveMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.annotators(typingHiveAnnotateFloorCeil, KCeil, KFloor)

	m.returns(
		DT_BINARY,
		KEncode,
		KUnhex,
	)
	m.returns(
		DT_DOUBLE,
		KCorr,
		KMonthsBetween,
		KSign,
	)
	m.returns(
		DT_VARCHAR,
		KAddMonths,
		KCurrentDatabase,
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
		KArraySize,
		KDenseRank,
		KMonth,
		KNtile,
		KRank,
		KRowNumber,
		KSecond,
		KMinute,
	)
	m.returns(DT_DOUBLE, KPercentileDisc)
	m.returns(DT_BIGINT, KGrouping)
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
