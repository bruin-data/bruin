package sqlengine

// typingTSQLAnnotateMathFunction types a DECIMAL argument of CEILING or FLOOR as UNKNOWN, since
// its precision is not modeled.
func typingTSQLAnnotateMathFunction(a *TypeAnnotator, e *Expr) {
	this := e.This()
	switch {
	case this.IsTypeOf(DT_UTINYINT, DT_SMALLINT):
		a.setType(e, DT_INT)
	case this.IsTypeOf(typingTSQLDoubleArgTypes...):
		a.setType(e, DT_DOUBLE)
	case this.IsTypeOf(DT_SMALLMONEY):
		a.setType(e, DT_MONEY)
	case this.IsTypeOf(DT_DECIMAL) && !e.IsA(KSign):
		a.setType(e, DT_UNKNOWN)
	default:
		typingAnnotateByNumericArg(a, e)
	}
}

var typingTSQLDoubleArgTypes = func() []any {
	var out []any
	for _, t := range append(append([]DType{DT_BIT}, DataType_FLOAT_TYPES.Items()...), DataType_TEXT_TYPES.Items()...) {
		out = append(out, t)
	}
	return out
}()

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
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KDegrees,
		KRadians,
	)
	m.annotators(typingTSQLAnnotateMathFunction, KCeil, KFloor, KSign)
	m.returns(DT_NVARCHAR, KCurrentTimezone)
	m.returns(DT_DATETIME, KCurrentTimestamp)

	return m
}
