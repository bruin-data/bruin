package sqlengine

// typingPostgresAnnotateMathFunction types CEIL, FLOOR and SIGN: DOUBLE for FLOAT, integer and
// text arguments, the argument's type otherwise.
func typingPostgresAnnotateMathFunction(a *TypeAnnotator, expression *Expr) {
	this := expression.This()
	if this.IsTypeOf(DT_FLOAT) || annIsTypeSet(this, DataType_INTEGER_TYPES) || annIsTypeSet(this, DataType_TEXT_TYPES) {
		a.setType(expression, DT_DOUBLE)
		return
	}
	a.setType(expression, annT(this.Type()))
}

// typingPostgresAnnotateAddSub types date and time arithmetic with an interval operand: a DATE
// shifted by an interval is a TIMESTAMP and any other temporal operand keeps its type.
func typingPostgresAnnotateAddSub(a *TypeAnnotator, expression *Expr) {
	a.annotateBinary(expression)
	left, right := expression.Left(), expression.Right()

	if expression.IsA(KAdd) && left.IsTypeOf(DT_INTERVAL) {
		left, right = right, left
	}

	if right.IsTypeOf(DT_INTERVAL) {
		if left.IsTypeOf(DT_DATE) {
			a.setType(expression, DT_TIMESTAMP)
		} else if annIsTypeSet(left, DataType_TEMPORAL_TYPES) {
			a.setType(expression, annT(left.Type()))
		}
	}
}

func typingPostgresMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	// https://www.postgresql.org/docs/current/functions-window.html
	// NTILE returns integer; other ranking functions return bigint (base default).
	m.returns(DT_INT, KNtile, KWidthBucket)
	m.returns(
		DT_TEXT,
		KEncode,
		KLeft,
		KRight,
		KOverlay,
		KReverse,
		KPad,
		KFormat,
		KHex,
		KSplitPart,
		KNormalize,
		KRegexpReplace,
		KReplace,
	)
	m.returns(DT_VARBINARY, KConvertToCharset, KDecode)
	m.returns(DT_TIME, KTimeFromParts)
	m.returns(DT_TIMESTAMP, KTimestampFromParts, KLocaltimestamp)
	m.returns(DT_DECIMAL, KToNumber)
	m.annotators(typingPostgresAnnotateMathFunction, KCeil, KFloor, KSign)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KBitwiseAndAgg,
		KBitwiseOrAgg,
		KBitwiseXorAgg,
	)
	m.annotators(typingPostgresAnnotateAddSub, KAdd, KSub)

	return m
}
