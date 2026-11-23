package sqlengine

var typingDuckDBDatetimeWithoutTZ = []any{
	DT_DATE,
	DT_TIMESTAMP,
	DT_TIMESTAMPNTZ,
	DT_TIMESTAMP_S,
	DT_TIMESTAMP_MS,
	DT_TIMESTAMP_NS,
}

func typingDuckDBAnnotateAtTimeZone(a *TypeAnnotator, e *Expr) {
	this := e.This()

	switch {
	case this.IsTypeOf(typingDuckDBDatetimeWithoutTZ...):
		a.setType(e, DT_TIMESTAMPTZ)
	case this.IsTypeOf(DT_TIMESTAMPTZ):
		a.setType(e, DT_TIMESTAMP)
	case this.IsTypeOf(DT_TIMETZ):
		a.setType(e, DT_TIMETZ)
	default:
		a.setType(e, DT_UNKNOWN)
	}
}

func typingDuckDBAnnotateDateTrunc(a *TypeAnnotator, e *Expr) {
	if e.This().IsTypeOf(DT_DATE, DT_TIMESTAMP_S, DT_TIMESTAMP_MS, DT_TIMESTAMP_NS) {
		a.setType(e, DT_TIMESTAMP)
		return
	}
	a.setType(e, e.This().Type())
}

func typingDuckDBAnnotateFloorCeil(a *TypeAnnotator, e *Expr) {
	this := e.This()
	switch {
	case this.IsTypeOf(DT_DECIMAL):
		a.setType(e, DT_UNKNOWN)
	case this.Type() != nil && annDTypeIn(this.Type().Arg("this"), DataType_INTEGER_TYPES.Union(DataType_TEXT_TYPES)):
		a.setType(e, DT_DOUBLE)
	default:
		a.setType(e, this.Type())
	}
}

func typingDuckDBAnnotateAddSub(a *TypeAnnotator, e *Expr) {
	a.annotateBinary(e)
	left, right := e.Left(), e.Right()

	if e.IsA(KAdd) && left.IsTypeOf(DT_INTERVAL) {
		left, right = right, left
	}

	if right.IsTypeOf(DT_INTERVAL) {
		if left.IsTypeOf(typingDuckDBDatetimeWithoutTZ...) {
			a.setType(e, DT_TIMESTAMP)
		} else if left.IsTypeOf(DT_TIME, DT_TIMETZ, DT_TIMESTAMPTZ) {
			a.setType(e, left.Type())
		}
	}
}

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
		KCurrentRole,
		KFormat,
		KReverse,
		KDecode,
	)
	m.returns(
		DT_VARBINARY,
		KEncode,
		KUnhex,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.annotateBinary(e)
		a.setType(e, DT_BOOLEAN)
	}, KArrayOverlaps, KRegexpFullMatch)
	m.annotators(typingDuckDBAnnotateFloorCeil, KCeil, KFloor)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KBitwiseAndAgg,
		KBitwiseOrAgg,
		KBitwiseXorAgg,
		KPercentileDisc,
		KMode,
	)
	// The top-n form MIN/MAX(x, n) returns a list of x's type.
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.annotateByArgsFull(e, false, e.ArgE("count") != nil, "this", "expressions")
	}, KMax, KMin)
	m.annotators(typingDuckDBAnnotateAddSub, KAdd, KSub)
	m.annotators(typingDuckDBAnnotateAtTimeZone, KAtTimeZone)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "expression") }, KDateBin)
	m.annotators(typingDuckDBAnnotateDateTrunc, KDateTrunc, KTimestampTrunc)
	m.returns(DT_TIMESTAMP, KLocaltimestamp)
	m.returns(DT_INTERVAL, KToDays)
	m.returns(DT_TIME, KTimeFromParts)
	m.returns(DT_TIMESTAMP, KTimestampFromParts)

	return m
}
