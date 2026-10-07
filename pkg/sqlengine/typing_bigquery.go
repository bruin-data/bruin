package sqlengine

// Port of sqlglot/typing/bigquery.py.

// DATE_ADD / DATE_SUB / *_TRUNC return the type of their first argument. BigQuery
// implicitly casts a string literal first arg to the function's own temporal type,
// so map each to that type (e.g. DATE_ADD('2020-01-01', ...) -> DATE,
// TIMESTAMP_TRUNC('...') -> TIMESTAMP).
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
			panic(&ValueError{Msg: expression.classRepr()})
		}
		return a.setType(expression, dtype)
	}

	return a.annotateByArgs(expression, "this")
}

// typingBigQueryAnnotateMathFunctions: many BigQuery math functions such as CEIL, FLOOR etc
// follow this return type convention:
//
//	+---------+---------+---------+------------+---------+
//	|  INPUT  | INT64   | NUMERIC | BIGNUMERIC | FLOAT64 |
//	+---------+---------+---------+------------+---------+
//	|  OUTPUT | FLOAT64 | NUMERIC | BIGNUMERIC | FLOAT64 |
//	+---------+---------+---------+------------+---------+
func typingBigQueryAnnotateMathFunctions(a *TypeAnnotator, expression *Expr) *Expr {
	this := expression.This()

	if annIsTypeSet(this, DataType_INTEGER_TYPES) {
		a.setType(expression, DT_DOUBLE)
	} else {
		a.setType(expression, annT(this.Type()))
	}
	return expression
}

// typingBigQueryAnnotateSafeDivide:
//
//	+------------+------------+------------+-------------+---------+
//	| INPUT      | INT64      | NUMERIC    | BIGNUMERIC  | FLOAT64 |
//	+------------+------------+------------+-------------+---------+
//	| INT64      | FLOAT64    | NUMERIC    | BIGNUMERIC  | FLOAT64 |
//	| NUMERIC    | NUMERIC    | NUMERIC    | BIGNUMERIC  | FLOAT64 |
//	| BIGNUMERIC | BIGNUMERIC | BIGNUMERIC | BIGNUMERIC  | FLOAT64 |
//	| FLOAT64    | FLOAT64    | FLOAT64    | FLOAT64     | FLOAT64 |
//	+------------+------------+------------+-------------+---------+
func typingBigQueryAnnotateSafeDivide(a *TypeAnnotator, expression *Expr) *Expr {
	if annIsTypeSet(expression.This(), DataType_INTEGER_TYPES) && annIsTypeSet(expression.Expression(), DataType_INTEGER_TYPES) {
		return a.setType(expression, DT_DOUBLE)
	}

	return typingBigQueryAnnotateByArgsWithCoerce(a, expression)
}

// typingBigQueryAnnotateByArgsWithCoerce:
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

			// Get all column types for the SetOperation
			colTypes := a.getSetopColumnTypes(unnested)
			// For ARRAY constructor, there should only be one projection
			// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array
			if colTypes.Len() > 0 && len(unnested.Left().Selects()) > 0 {
				firstColName := unnested.Left().Selects()[0].AliasOrName()
				projectionType, _ = colTypes.Get(firstColName)
			}
		}

		// If we successfully determine a projection type and it's not UNKNOWN, wrap it in ARRAY
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

	return a.annotateByArgsFull(expression, false, true, "expressions")
}

func typingBigQueryMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.annotators(
		"typing/bigquery.py:199", func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateMathFunctions(a, e) },
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
		"typing/bigquery.py:212", func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
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
	m.annotators("typing/bigquery.py:241", func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateDateFunc(a, e) },
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
		KCovarPop,
		KCovarSamp,
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
		"typing/bigquery.py:345", func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateByArgsWithCoerce(a, e) },
		KPercentileCont,
		KSafeAdd,
		KSafeDivide,
		KSafeMultiply,
		KSafeSubtract,
	)
	m.annotators(
		"typing/bigquery.py:355", func(a *TypeAnnotator, e *Expr) { a.annotateByArgsFull(e, false, true, "this") },
		KApproxQuantiles,
		KJSONExtractArray,
		KRegexpExtractAll,
		KSplit,
	)
	m.returns(DT_TIMESTAMPTZ, typingTIMESTAMP_EXPRESSIONS...)
	m.annotators("typing/bigquery.py:364", func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateByArgsApproxTop(a, e) }, KApproxTopK)
	m.annotators("typing/bigquery.py:365", func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateByArgsApproxTop(a, e) }, KApproxTopSum)
	m.annotators("typing/bigquery.py:127", func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateArray(a, e) }, KArray)
	m.annotators("typing/bigquery.py:116", func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateConcat(a, e) }, KConcat)
	m.returns(DT_DATE, KDateFromUnixDate)
	m.annotators("typing/bigquery.py:370", func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<TIMESTAMP>", MustDialect("bigquery"), false, true))
	}, KGenerateTimestampArray)
	m.annotators("typing/bigquery.py:375", func(a *TypeAnnotator, e *Expr) {
		if e.ArgB("to_json") {
			a.setType(e, DT_JSON)
		} else {
			a.setType(e, DT_VARCHAR)
		}
	}, KJSONFormat)
	m.annotators("typing/bigquery.py:380", func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<VARCHAR>", MustDialect("bigquery"), false, true))
	}, KJSONKeysAtDepth)
	m.annotators("typing/bigquery.py:385", func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<VARCHAR>", MustDialect("bigquery"), false, true))
	}, KJSONValueArray)
	m.returns(DT_BIGDECIMAL, KParseBignumeric)
	m.returns(DT_DECIMAL, KParseNumeric)
	m.annotators("typing/bigquery.py:391", func(a *TypeAnnotator, e *Expr) { typingBigQueryAnnotateSafeDivide(a, e) }, KSafeDivide)
	m.annotators("typing/bigquery.py:393", func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<BIGINT>", MustDialect("bigquery"), false, true))
	}, KToCodePoints)

	return m
}
