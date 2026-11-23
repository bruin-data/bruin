package sqlengine

var typingBigQueryDateFuncKinds = []Kind{KDateAdd, KDateSub, KDateTrunc, KDatetimeTrunc, KTimestampTrunc}

var typingBigQueryDateFuncLiteralType = map[Kind]DType{
	KDateAdd:        DT_DATE,
	KDateSub:        DT_DATE,
	KDateTrunc:      DT_DATE,
	KDatetimeTrunc:  DT_DATETIME,
	KTimestampTrunc: DT_TIMESTAMPTZ,
}

// typingBigQueryAnnotateDateFunc annotates DATE_ADD / DATE_SUB / *_TRUNC, which return their
// first arg's type.
//
// A typed first argument keeps its exact type (e.g. DATE_ADD(DATETIME, ...) -> DATETIME). For a
// string literal first argument, BigQuery implicitly casts it to the function's own temporal
// type, so the result is that type (e.g. DATE_ADD('2020-01-01', INTERVAL 1 DAY) -> DATE).
func typingBigQueryAnnotateDateFunc(a *TypeAnnotator, expression *Expr) *Expr {
	this := expression.This()

	// BigQuery rejects expressions like DATE_ADD(c, ...); it requires the first argument to be a literal
	if this.IsA(KLiteral) && this.IsString() {
		dtype, ok := typingBigQueryDateFuncLiteralType[expression.Kind()]
		if !ok {
			panic(&ValueError{Msg: expression.Kind().Name()})
		}
		return a.setType(expression, dtype)
	}

	return a.annotateByArgs(expression, "this")
}

// typingBigQueryAnnotateDatetimeOrTimestampAdd annotates DATETIME_ADD / DATETIME_SUB /
// TIMESTAMP_ADD / TIMESTAMP_SUB. Whichever of them is called, BigQuery returns a TIMESTAMP for a
// TIMESTAMP and a DATETIME for a DATE or DATETIME. Any other argument, e.g. a string literal,
// gets the function's own type.
func typingBigQueryAnnotateDatetimeOrTimestampAdd(a *TypeAnnotator, expression *Expr) *Expr {
	this := expression.This()
	if this.IsTypeOf(DT_TIMESTAMPTZ) {
		return a.setType(expression, DT_TIMESTAMPTZ)
	}
	if this.IsTypeOf(DT_DATE, DT_DATETIME, DT_TIMESTAMP) {
		return a.setType(expression, DT_DATETIME)
	}
	if expression.IsA(KTimestampAdd, KTimestampSub) {
		return a.setType(expression, DT_TIMESTAMPTZ)
	}
	return a.setType(expression, DT_DATETIME)
}

// typingBigQueryAnnotateMathFunctions annotates math functions such as CEIL and FLOOR, which
// return FLOAT64 for integers and strings and the input type otherwise:
//
//	+---------+---------+---------+------------+---------+
//	|  INPUT  | INT64   | NUMERIC | BIGNUMERIC | FLOAT64 |
//	+---------+---------+---------+------------+---------+
//	|  OUTPUT | FLOAT64 | NUMERIC | BIGNUMERIC | FLOAT64 |
//	+---------+---------+---------+------------+---------+
func typingBigQueryAnnotateMathFunctions(a *TypeAnnotator, expression *Expr) *Expr {
	this := expression.This()

	if annIsTypeSet(this, DataType_INTEGER_TYPES) || annIsTypeSet(this, DataType_TEXT_TYPES) {
		a.setType(expression, DT_DOUBLE)
	} else {
		a.setType(expression, annT(this.Type()))
	}
	return expression
}

// typingBigQueryAnnotateIntPairAsDouble annotates SAFE_DIVIDE and POW / POWER: both promote
// INT64 / INT64 to FLOAT64, and otherwise follow the numeric coercion. NULL operands coerce the
// way INT64 does, since BigQuery treats a bare NULL argument as an unresolved integer-family
// type for this promotion.
//
//	+------------+------------+------------+-------------+---------+
//	| INPUT      | INT64      | NUMERIC    | BIGNUMERIC  | FLOAT64 |
//	+------------+------------+------------+-------------+---------+
//	| INT64      | FLOAT64    | NUMERIC    | BIGNUMERIC  | FLOAT64 |
//	| NUMERIC    | NUMERIC    | NUMERIC    | BIGNUMERIC  | FLOAT64 |
//	| BIGNUMERIC | BIGNUMERIC | BIGNUMERIC | BIGNUMERIC  | FLOAT64 |
//	| FLOAT64    | FLOAT64    | FLOAT64    | FLOAT64     | FLOAT64 |
//	+------------+------------+------------+-------------+---------+
func typingBigQueryAnnotateIntPairAsDouble(a *TypeAnnotator, expression *Expr) *Expr {
	typingBigQueryAnnotateByArgsWithCoerce(a, expression)

	if expression.IsTypeOf(DT_NULL) || annIsTypeSet(expression, DataType_INTEGER_TYPES) {
		a.setType(expression, DT_DOUBLE)
	}

	return expression
}

// typingBigQueryAnnotateByArgsWithCoerce types a binary expression by coercing its operand types:
//
//	+------------+------------+------------+-------------+---------+
//	| INPUT      | INT64      | NUMERIC    | BIGNUMERIC  | FLOAT64 |
//	+------------+------------+------------+-------------+---------+
//	| INT64      | INT64      | NUMERIC    | BIGNUMERIC  | FLOAT64 |
//	| NUMERIC    | NUMERIC    | NUMERIC    | BIGNUMERIC  | FLOAT64 |
//	| BIGNUMERIC | BIGNUMERIC | BIGNUMERIC | BIGNUMERIC  | FLOAT64 |
//	| FLOAT64    | FLOAT64    | FLOAT64    | FLOAT64     | FLOAT64 |
//	+------------+------------+------------+-------------+---------+
func typingBigQueryAnnotateByArgsWithCoerce(a *TypeAnnotator, expression *Expr) *Expr {
	a.setType(expression, a.maybeCoerce(annT(expression.This().Type()), annT(expression.Expression().Type())))
	return expression
}

func typingBigQueryAnnotateByArgsApproxTop(a *TypeAnnotator, expression *Expr) *Expr {
	if expression.This().Type() == nil || expression.This().IsTypeOf(DT_UNKNOWN) {
		a.setType(expression, DT_UNKNOWN)
		return expression
	}

	structType := New(
		KDataType,
		"this", DT_STRUCT,
		"expressions", []*Expr{expression.This().Type(), New(KDataType, "this", DT_BIGINT)},
		"nested", true,
	)
	a.setType(expression, New(KDataType, "this", DT_ARRAY, "expressions", []*Expr{structType}, "nested", true))

	return expression
}

func typingBigQueryAnnotateConcat(a *TypeAnnotator, expression *Expr) *Expr {
	annotated := a.annotateByArgs(expression, "expressions")

	// Args must be BYTES or types that can be cast to STRING, return type is either BYTES or STRING
	// https://cloud.google.com/bigquery/docs/reference/standard-sql/string_functions#concat
	if !annotated.IsTypeOf(DT_BINARY, DT_UNKNOWN) {
		a.setType(annotated, DT_VARCHAR)
	}

	return annotated
}

// typingBigQueryAllStructKind reports whether every leaf SELECT of a (possibly nested) set
// operation is a SELECT AS STRUCT. It walks the operands iteratively, so long UNION chains do not
// deepen the stack.
func typingBigQueryAllStructKind(node *Expr) bool {
	stack := []*Expr{node}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n.IsA(KSetOperation) {
			stack = append(stack, n.This().Unnest(), n.Expression().Unnest())
		} else if kind, _ := n.Arg("kind").(string); !n.IsA(KSelect) || kind != "STRUCT" {
			return false
		}
	}

	return true
}

func typingBigQueryAnnotateArray(a *TypeAnnotator, expression *Expr) *Expr {
	arrayArgs := expression.Expressions()

	// BigQuery behaves as follows:
	//
	// SELECT t, TYPEOF(t) FROM (SELECT 'foo') AS t            -- foo, STRUCT<STRING>
	// SELECT ARRAY(SELECT 'foo'), TYPEOF(ARRAY(SELECT 'foo')) -- foo, ARRAY<STRING>
	// ARRAY(SELECT ... UNION ALL SELECT ...)                  -- ARRAY<type from coerced projections>
	// ARRAY(SELECT AS STRUCT 1 AS a, 'b' AS b)                -- ARRAY<STRUCT<INT64, STRING>>
	if len(arrayArgs) == 1 {
		unnested := arrayArgs[0].UnnestSubqueryOrParen()
		var projectionType any

		// Handle ARRAY(SELECT ...) - single SELECT query
		if unnested.IsA(KSelect) {
			queryType, _ := unnested.MetaGet("query_type").(*Expr)

			if queryType != nil && queryType.IsTypeOf(DT_STRUCT) {
				queryExprs := queryType.Expressions()

				var colDefs []*Expr
				for _, e := range queryExprs {
					if e.IsA(KColumnDef) && !(e.ArgE("kind") != nil && e.ArgE("kind").IsTypeOf(DT_UNKNOWN)) {
						colDefs = append(colDefs, e)
					}
				}

				if len(colDefs) == len(queryExprs) {
					if kind, _ := unnested.Arg("kind").(string); kind == "STRUCT" {
						// ARRAY(SELECT AS STRUCT ...) -> ARRAY<STRUCT<col1, col2, ...>>
						projectionType = queryType
					} else if len(colDefs) == 1 && colDefs[0].ArgE("kind") != nil {
						// ARRAY(SELECT col FROM ...) -> ARRAY<col_type>
						projectionType = colDefs[0].ArgE("kind")
					}
				}
			}
		} else if unnested.IsA(KSetOperation) {
			// Handle ARRAY(SELECT ... UNION ALL SELECT ...) - set operations

			colTypes := a.getSetopColumnTypes(unnested)

			if len(colTypes) > 0 && typingBigQueryAllStructKind(unnested) {
				// Each branch's row is itself the struct value, not N separate columns
				fields := make([]*Expr, 0, len(colTypes))
				known := true
				for _, ct := range colTypes {
					kind := DataTypeBuild(ct.typ, nil, false, true)
					if kind.IsTypeOf(DT_UNKNOWN) {
						known = false
						break
					}
					fields = append(fields, New(KColumnDef, "this", ToIdentifier(ct.name, nil), "kind", kind))
				}

				if known {
					projectionType = New(KDataType, "this", DT_STRUCT, "expressions", fields, "nested", true)
				}
			} else if len(colTypes) == 1 {
				// For ARRAY constructor, there should only be one projection
				// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array
				projectionType = colTypes[0].typ
			}
		}

		if projectionType != nil {
			dt, isDataType := projectionType.(*Expr)
			if !((isDataType && dt.IsTypeOf(DT_UNKNOWN)) || projectionType == DT_UNKNOWN) {
				var elementType *Expr
				if isDataType {
					elementType = dt.Copy()
				} else {
					elementType = New(KDataType, "this", projectionType)
				}
				arrayType := New(KDataType, "this", DT_ARRAY, "expressions", []*Expr{elementType}, "nested", true)
				a.setType(expression, arrayType)
				return expression
			}
		}
	}

	// BigQuery inherits field names from the first STRUCT in a literal array
	if len(arrayArgs) > 0 {
		if firstType := arrayArgs[0].Type(); firstType.IsTypeOf(DT_STRUCT) && len(firstType.Expressions()) > 0 {
			a.setType(expression, New(KDataType, "this", DT_ARRAY, "expressions", []*Expr{firstType.Copy()}, "nested", true))
			return expression
		}
	}

	return a.annotateByArgsFull(expression, false, true, "expressions")
}

func typingBigQueryMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.annotators(
		func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateMathFunctions(a, e) },
		KAvg,
		KCeil,
		KExp,
		KFloor,
		KLn,
		KLog,
		KRound,
		KSqrt,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KArgMax,
		KArgMin,
		KGroupConcat,
		KIgnoreNulls,
		KJSONExtract,
		KLeft,
		KLower,
		KNetFunc,
		KPad,
		KPercentileDisc,
		KRegexpExtract,
		KRegexpReplace,
		KRepeat,
		KReplace,
		KRespectNulls,
		KReverse,
		KRight,
		KSafeFunc,
		KSafeNegate,
		KSign,
		KSubstring,
		KTranslate,
		KTrim,
		KUpper,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateDateFunc(a, e) },
		typingBigQueryDateFuncKinds...)
	m.returns(
		DT_BIGINT,
		KBitwiseAndAgg,
		KBitwiseCount,
		KBitwiseOrAgg,
		KBitwiseXorAgg,
		KByteLength,
		KFarmFingerprint,
		KGrouping,
		KLaxInt64,
		KLength,
		KRangeBucket,
		KRegexpInstr,
		KUnixDate,
	)
	m.returns(
		DT_BINARY,
		KByteString,
		KCodePointsToBytes,
		KMD5Digest,
		KSHA,
		KSHA2,
		KSHA1Digest,
		KSHA2Digest,
		KUnhex,
	)
	m.returns(
		DT_BOOLEAN,
		KJSONBool,
		KLaxBool,
	)
	m.returns(
		DT_DATETIME,
		KParseDatetime,
		KTimestampFromParts,
	)
	m.returns(
		DT_DOUBLE,
		KAtan2,
		KCorr,
		KCosineDistance,
		KCoth,
		KCsc,
		KCsch,
		KEuclideanDistance,
		KFloat64,
		KLaxFloat64,
		KSec,
		KSech,
	)
	m.returns(
		DT_JSON,
		KJSONArray,
		KJSONArrayAppend,
		KJSONArrayInsert,
		KJSONObject,
		KJSONRemove,
		KJSONSet,
		KJSONStripNulls,
	)
	m.returns(
		DT_TIME,
		KParseTime,
		KTimeFromParts,
		KTimeTrunc,
		KTsOrDsToTime,
	)
	m.returns(
		DT_VARCHAR,
		KCodePointsToString,
		KFormat,
		KHost,
		KJSONExtractScalar,
		KJSONType,
		KLaxString,
		KLowerHex,
		KNormalize,
		KRegDomain,
		KSafeConvertBytesToString,
		KSoundex,
		KUuid,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateByArgsWithCoerce(a, e) },
		KPercentileCont,
		KSafeAdd,
		KSafeDivide,
		KSafeMultiply,
		KSafeSubtract,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgsFull(e, false, true, "this") },
		KApproxQuantiles,
		KJSONExtractArray,
		KRegexpExtractAll,
		KSplit,
	)
	m.returns(DT_TIMESTAMPTZ, typingTIMESTAMP_EXPRESSIONS...)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateDatetimeOrTimestampAdd(a, e) },
		KDatetimeAdd, KDatetimeSub, KTimestampAdd, KTimestampSub)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateByArgsApproxTop(a, e) }, KApproxTopK)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateByArgsApproxTop(a, e) }, KApproxTopSum)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateArray(a, e) }, KArray)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateConcat(a, e) }, KConcat)
	m.returns(DT_DATE, KDateFromUnixDate)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<TIMESTAMP>", MustDialect("bigquery"), false, true))
	}, KGenerateTimestampArray)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		if e.ArgB("to_json") {
			a.setType(e, DT_JSON)
		} else {
			a.setType(e, DT_VARCHAR)
		}
	}, KJSONFormat)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<VARCHAR>", MustDialect("bigquery"), false, true))
	}, KJSONKeysAtDepth)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<VARCHAR>", MustDialect("bigquery"), false, true))
	}, KJSONValueArray)
	m.returns(DT_BIGDECIMAL, KParseBignumeric)
	m.returns(DT_DECIMAL, KParseNumeric)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateIntPairAsDouble(a, e) }, KPow, KSafeDivide)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<BIGINT>", MustDialect("bigquery"), false, true))
	}, KToCodePoints)

	return m
}
