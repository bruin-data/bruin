package sqlengine

import (
	"fmt"
	"math/big"
)

var typingSnowflakeDATE_PARTS = newStrSet("DAY", "WEEK", "MONTH", "QUARTER", "YEAR")

const typingSnowflakeMAX_PRECISION = 38

const typingSnowflakeMAX_SCALE = 37

func typingSnowflakeAnnotateReverse(a *TypeAnnotator, expression *Expr) *Expr {
	expression = a.annotateByArgs(expression, "this")
	if expression.IsTypeOf(DT_NULL) {
		// Snowflake treats REVERSE(NULL) as a VARCHAR
		a.setType(expression, DT_VARCHAR)
	}

	return expression
}

// typingSnowflakeAnnotateTimestampFromParts types TIMESTAMP_FROM_PARTS as TIMESTAMPTZ with a time
// zone and as TIMESTAMP (Snowflake's default TIMESTAMP_NTZ) without one.
func typingSnowflakeAnnotateTimestampFromParts(a *TypeAnnotator, expression *Expr) *Expr {
	if expression.ArgB("zone") {
		a.setType(expression, DT_TIMESTAMPTZ)
	} else {
		a.setType(expression, DT_TIMESTAMP)
	}

	return expression
}

func typingSnowflakeAnnotateDateOrTimeAdd(a *TypeAnnotator, expression *Expr) *Expr {
	if expression.This().IsTypeOf(DT_DATE) && !typingSnowflakeDATE_PARTS.Has(pyUpper(expression.Text("unit"))) {
		a.setType(expression, DT_TIMESTAMPNTZ)
	} else {
		a.annotateByArgs(expression, "this")
	}
	return expression
}

// typingSnowflakeAnnotateDecodeCase types DECODE(expr, val1, ret1, val2, ret2, ..., default) from
// its return values (ret1, ret2, ..., default) only, not from expr or the compared values.
func typingSnowflakeAnnotateDecodeCase(a *TypeAnnotator, expression *Expr) *Expr {
	expressions := expression.Expressions()

	// Return values are at indices 2, 4, 6, ... and the last element (if even length)
	var returnTypes []any
	for i := 2; i < len(expressions); i += 2 {
		returnTypes = append(returnTypes, annT(expressions[i].Type()))
	}

	// If the total number of expressions is even, the last one is the default
	// Example:
	//   DECODE(x, 1, 'a', 2, 'b')             -> len=5 (odd), no default
	//   DECODE(x, 1, 'a', 2, 'b', 'default')  -> len=6 (even), has default
	if len(expressions)%2 == 0 {
		returnTypes = append(returnTypes, annT(expressions[len(expressions)-1].Type()))
	}

	var lastType any
	for _, retType := range returnTypes {
		first := lastType
		if first == nil {
			first = retType
		}
		lastType = a.maybeCoerce(first, retType)
	}

	a.setType(expression, lastType)
	return expression
}

func typingSnowflakeAnnotateArgMaxMin(a *TypeAnnotator, expression *Expr) *Expr {
	if expression.ArgB("count") {
		a.setType(expression, DT_ARRAY)
	} else {
		a.setType(expression, annT(expression.This().Type()))
	}
	return expression
}

func typingSnowflakeAnnotateWithinGroup(a *TypeAnnotator, expression *Expr) *Expr {
	orderExpr := expression.Expression()
	if expression.This().IsA(KPercentileDisc, KPercentileCont) &&
		orderExpr.IsA(KOrder) &&
		len(orderExpr.Expressions()) == 1 &&
		orderExpr.Expressions()[0].IsA(KOrdered) {
		a.setType(expression, annT(orderExpr.Expressions()[0].This().Type()))
	} else {
		a.setType(expression, annT(expression.This().Type()))
	}

	return expression
}

// typingSnowflakeToPyInt returns the integer value of a NUMBER(p, s) type parameter, panicking
// with a ValueError for anything else.
func typingSnowflakeToPyInt(param *Expr) int {
	this := param.This()
	if this.IsNumber() {
		if v, isInt := this.toPyNumber(); v != nil && isInt {
			if i, acc := v.Int64(); acc == big.Exact {
				return int(i)
			}
		}
	}
	panic(&ValueError{Msg: "Unsupported type parameter for Snowflake type annotation: " + annPyTypeName(this)})
}

// typingSnowflakeAnnotateMedian follows Snowflake's MEDIAN return types: DOUBLE for FLOAT/DOUBLE
// (synonyms) and NUMBER(min(p+3, 38), min(s+3, 37)) for NUMBER(p, s).
func typingSnowflakeAnnotateMedian(a *TypeAnnotator, expression *Expr) *Expr {
	expression = a.annotateByArgs(expression, "this")

	inputType := expression.This().Type()

	if inputType.IsTypeOf(DT_DOUBLE) {
		a.setType(expression, DT_DOUBLE)
	} else {
		exprs := inputType.Expressions()

		precision := typingSnowflakeMAX_PRECISION
		if precisionExpr := seqGet(exprs, 0); precisionExpr != nil {
			precision = typingSnowflakeToPyInt(precisionExpr)
		}

		scale := 0
		if scaleExpr := seqGet(exprs, 1); scaleExpr != nil {
			scale = typingSnowflakeToPyInt(scaleExpr)
		}

		newPrecision := min(precision+3, typingSnowflakeMAX_PRECISION)
		newScale := min(scale+3, typingSnowflakeMAX_SCALE)

		newType := DataTypeBuild(fmt.Sprintf("NUMBER(%d, %d)", newPrecision, newScale), MustDialect("snowflake"), false, true)
		a.setType(expression, newType)
	}

	return expression
}

// typingSnowflakeAnnotateVariance annotates variance functions (VAR_POP, VAR_SAMP, VARIANCE,
// VARIANCE_POP) with the correct return type.
//
// Based on Snowflake behavior:
//   - DECFLOAT -> DECFLOAT(38)
//   - FLOAT/DOUBLE -> FLOAT
//   - INT, NUMBER(p, 0) -> NUMBER(38, 6)
//   - NUMBER(p, s) -> NUMBER(38, max(12, s))
func typingSnowflakeAnnotateVariance(a *TypeAnnotator, expression *Expr) *Expr {
	expression = a.annotateByArgs(expression, "this")

	inputType := expression.This().Type()

	if inputType.IsTypeOf(DT_DECFLOAT) {
		a.setType(expression, DataTypeBuild("DECFLOAT", MustDialect("snowflake"), false, true))
	} else if inputType.IsTypeOf(DT_FLOAT, DT_DOUBLE) {
		a.setType(expression, DT_DOUBLE)
	} else {
		exprs := inputType.Expressions()
		scale := 0
		if scaleExpr := seqGet(exprs, 1); scaleExpr != nil {
			scale = typingSnowflakeToPyInt(scaleExpr)
		}

		// If scale is 0 (INT, BIGINT, NUMBER(p,0)): return NUMBER(38, 6)
		// Otherwise, Snowflake appears to assign scale through the formula MAX(12, s)
		newScale := 6
		if scale != 0 {
			newScale = max(12, scale)
		}

		newType := DataTypeBuild(fmt.Sprintf("NUMBER(%d, %d)", typingSnowflakeMAX_PRECISION, newScale), MustDialect("snowflake"), false, true)
		a.setType(expression, newType)
	}

	return expression
}

// typingSnowflakeAnnotateKurtosis annotates KURTOSIS with the correct return type.
//
// Based on Snowflake behavior:
//   - DECFLOAT input -> DECFLOAT
//   - DOUBLE or FLOAT input -> DOUBLE
//   - Other numeric types (INT, NUMBER) -> NUMBER(38, 12)
func typingSnowflakeAnnotateKurtosis(a *TypeAnnotator, expression *Expr) *Expr {
	expression = a.annotateByArgs(expression, "this")
	inputType := expression.This().Type()

	if inputType.IsTypeOf(DT_DECFLOAT) {
		a.setType(expression, DataTypeBuild("DECFLOAT", MustDialect("snowflake"), false, true))
	} else if inputType.IsTypeOf(DT_FLOAT, DT_DOUBLE) {
		a.setType(expression, DT_DOUBLE)
	} else {
		a.setType(expression, DataTypeBuild(fmt.Sprintf("NUMBER(%d, 12)", typingSnowflakeMAX_PRECISION), MustDialect("snowflake"), false, true))
	}

	return expression
}

// typingSnowflakeAnnotateMathWithFloatDecfloat types trigonometric and exponential math functions:
// DECFLOAT for DECFLOAT input, DOUBLE for any other numeric input.
func typingSnowflakeAnnotateMathWithFloatDecfloat(a *TypeAnnotator, expression *Expr) *Expr {
	expression = a.annotateByArgs(expression, "this")

	if expression.This().IsTypeOf(DT_DECFLOAT) {
		a.setType(expression, annT(expression.This().Type()))
	} else {
		a.setType(expression, DT_DOUBLE)
	}

	return expression
}

func typingSnowflakeAnnotateStrToTime(a *TypeAnnotator, expression *Expr) *Expr {
	var targetType any = DT_TIMESTAMP
	if targetTypeArg, ok := expression.Arg("target_type").(*Expr); ok && targetTypeArg.IsA(KDataType) {
		targetType = targetTypeArg.Arg("this")
	}
	a.setType(expression, targetType)
	return expression
}

// typingSnowflakeAnnotateFloorCeil annotates FLOOR and CEIL: a string argument is implicitly cast
// to FLOAT, or to NUMBER when a scale is given, and a VARIANT to FLOAT; any other argument goes
// through the numeric argument rule.
func typingSnowflakeAnnotateFloorCeil(a *TypeAnnotator, expression *Expr) *Expr {
	this := expression.This()
	if annIsTypeSet(this, DataType_TEXT_TYPES) {
		if expression.Arg("decimals") == nil {
			return a.setType(expression, DT_DOUBLE)
		}
		return a.setType(expression, DT_UNKNOWN)
	}
	if this.IsTypeOf(DT_VARIANT) {
		return a.setType(expression, DT_DOUBLE)
	}
	typingAnnotateByNumericArg(a, expression)
	return expression
}

func typingSnowflakeMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateFloorCeil(a, e) }, KCeil, KFloor)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		if annIsTypeSet(e.This(), DataType_FLOAT_TYPES) {
			a.setType(e, DT_DOUBLE)
		} else {
			a.setType(e, DT_INT)
		}
	}, KSign)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KAddMonths,
		KArrayDistinct,
		KArrayExcept,
		KDateTrunc,
		KLeft,
		KMode,
		KPad,
		KRight,
		KRound,
		KStuff,
		KSubstring,
		KTimeSlice,
		KTimestampTrunc,
	)
	m.returns(
		DT_ARRAY,
		KApproxTopK,
		KApproxTopKEstimate,
		KArray,
		KArrayAgg,
		KArrayAppend,
		KArrayCompact,
		KArrayConcat,
		KArrayConstructCompact,
		KArrayPrepend,
		KArrayRemove,
		KArraysZip,
		KArrayUniqueAgg,
		KArrayUnionAgg,
		KMapKeys,
		KRegexpExtractAll,
		KSplit,
		KStringToArray,
		KStrtokToArray,
	)
	m.returns(
		DT_BIGINT,
		KBitmapBitPosition,
		KBitmapBucketNumber,
		KBitmapCount,
		KFactorial,
		KGroupingId,
		KMD5NumberLower64,
		KMD5NumberUpper64,
		KRand,
		KSeq8,
		KZipf,
	)
	m.returns(
		DT_BINARY,
		KBase64DecodeBinary,
		KBitmapConstructAgg,
		KBitmapOrAgg,
		KCompress,
		KDecompressBinary,
		KDecrypt,
		KDecryptRaw,
		KEncrypt,
		KEncryptRaw,
		KHexString,
		KMD5Digest,
		KSHA1Digest,
		KSHA2Digest,
		KToBinary,
		KTryBase64DecodeBinary,
		KTryHexDecodeBinary,
		KUnhex,
	)
	m.returns(
		DT_BOOLEAN,
		KBooland,
		KBoolnot,
		KBoolor,
		KBoolxorAgg,
		KEqualNull,
		KIsNullValue,
		KMapContainsKey,
		KSearch,
		KSearchIp,
		KToBoolean,
	)
	m.returns(
		DT_DATE,
		KNextDay,
		KPreviousDay,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) {
			a.setType(e, DataTypeBuild("NUMBER", MustDialect("snowflake"), false, true))
		},
		KBitwiseAndAgg,
		KBitwiseOrAgg,
		KBitwiseXorAgg,
		KRegexpCount,
		KRegexpInstr,
		KToNumber,
	)
	m.returns(
		DT_DOUBLE,
		KApproxPercentileEstimate,
		KApproximateSimilarity,
		KCosineDistance,
		KCovarPop,
		KCovarSamp,
		KDotProduct,
		KEuclideanDistance,
		KManhattanDistance,
		KMonthsBetween,
		KNormal,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateKurtosis(a, e) }, KKurtosis)
	m.returns(
		DT_DECFLOAT,
		KToDecfloat,
		KTryToDecfloat,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateMathWithFloatDecfloat(a, e) },
		KAcos,
		KAsin,
		KAtan,
		KAtan2,
		KCbrt,
		KCos,
		KCot,
		KDegrees,
		KExp,
		KLn,
		KLog,
		KPow,
		KRadians,
		KRegrAvgx,
		KRegrAvgy,
		KRegrCount,
		KRegrIntercept,
		KRegrR2,
		KRegrSlope,
		KRegrSxx,
		KRegrSxy,
		KRegrSyy,
		KRegrValx,
		KRegrValy,
		KSin,
		KSqrt,
		KTan,
		KTanh,
	)
	m.returns(
		DT_INT,
		KByteLength,
		KDenseRank,
		KGrouping,
		KJarowinklerSimilarity,
		KMapSize,
		KMinute,
		KNtile,
		KRank,
		KRowNumber,
		KRtrimmedLength,
		KSecond,
		KSeq1,
		KSeq2,
		KSeq4,
		KWidthBucket,
	)
	m.returns(
		DT_OBJECT,
		KApproxPercentileAccumulate,
		KApproxPercentileCombine,
		KApproxTopKAccumulate,
		KApproxTopKCombine,
		KObjectAgg,
		KParseIp,
		KParseUrl,
		KXMLGet,
	)
	m.returns(
		DT_MAP,
		KMapCat,
		KMapDelete,
		KMapInsert,
		KMapPick,
	)
	m.returns(
		DT_FILE,
		KToFile,
	)
	m.returns(
		DT_TIME,
		KTimeFromParts,
		KTsOrDsToTime,
	)
	m.returns(
		DT_TIMESTAMPLTZ,
		KCurrentTimestamp,
		KLocaltimestamp,
	)
	m.returns(
		DT_TINYINT,
		KDayOfMonth,
		KDayOfWeek,
		KDayOfYear,
		KQuarter,
	)
	m.returns(
		DT_VARCHAR,
		KAIAgg,
		KAIClassify,
		KAISummarizeAgg,
		KBase64DecodeString,
		KBase64Encode,
		KCheckJson,
		KCheckXml,
		KCollate,
		KCollation,
		KCurrentAccount,
		KCurrentAccountName,
		KCurrentAvailableRoles,
		KCurrentClient,
		KCurrentDatabase,
		KCurrentIpAddress,
		KCurrentSchemas,
		KCurrentSecondaryRoles,
		KCurrentSession,
		KCurrentStatement,
		KCurrentTransaction,
		KCurrentWarehouse,
		KCurrentOrganizationUser,
		KCurrentRegion,
		KCurrentRole,
		KCurrentRoleType,
		KCurrentOrganizationName,
		KDecompressString,
		KHexDecodeString,
		KJSONFormat,
		KHex,
		KRandstr,
		KRegexpExtract,
		KRegexpReplace,
		KReplace,
		KSoundex,
		KSoundexP123,
		KSplitPart,
		KStrtok,
		KTryBase64DecodeString,
		KTryHexDecodeString,
		KUuid,
	)
	m.returns(
		DT_VARIANT,
		KMinhash,
		KMinhashCombine,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateVariance(a, e) },
		KVariance,
		KVariancePop,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateArgMaxMin(a, e) }, KArgMax)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateArgMaxMin(a, e) }, KArgMin)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "expressions") }, KConcatWs)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		if e.ArgB("source_tz") {
			a.setType(e, DT_TIMESTAMPNTZ)
		} else {
			a.setType(e, DT_TIMESTAMPTZ)
		}
	}, KConvertTimezone)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateDateOrTimeAdd(a, e) }, KDateAdd)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateDecodeCase(a, e) }, KDecodeCase)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("NUMBER(19, 0)", MustDialect("snowflake"), false, true))
	}, KHashAgg)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateMedian(a, e) }, KMedian)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateReverse(a, e) }, KReverse)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateStrToTime(a, e) }, KStrToTime)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateDateOrTimeAdd(a, e) }, KTimeAdd)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateTimestampFromParts(a, e) }, KTimestampFromParts)
	m.annotators(func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateWithinGroup(a, e) }, KWithinGroup)

	return m
}
