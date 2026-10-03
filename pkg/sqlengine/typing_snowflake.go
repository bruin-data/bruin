package sqlengine

import (
	"fmt"
	"math/big"
)

// Port of sqlglot/typing/snowflake.py.

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

// typingSnowflakeAnnotateTimestampFromParts annotates TimestampFromParts with the correct type
// based on arguments:
// TIMESTAMP_FROM_PARTS with time_zone -> TIMESTAMPTZ
// TIMESTAMP_FROM_PARTS without time_zone -> TIMESTAMP (defaults to TIMESTAMP_NTZ).
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

// typingSnowflakeAnnotateDecodeCase annotates DecodeCase with the type inferred from return
// values only.
//
// DECODE uses the format: DECODE(expr, val1, ret1, val2, ret2, ..., default)
// We only look at the return values (ret1, ret2, ..., default) to determine the type,
// not the comparison values (val1, val2, ...) or the expression being compared.
func typingSnowflakeAnnotateDecodeCase(a *TypeAnnotator, expression *Expr) *Expr {
	expressions := expression.Expressions()

	// Return values are at indices 2, 4, 6, ... and the last element (if even length)
	// DECODE(expr, val1, ret1, val2, ret2, ..., default)
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

	// Determine the common type from all return values
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

// typingSnowflakeAnnotateWithinGroup annotates WithinGroup with the correct type based on the
// inner function:
//  1. Annotate args first
//  2. Check if this is PercentileDisc/PercentileCont and if so, re-annotate its type to match
//     the ordered expression's type
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

// typingSnowflakeToPyInt mirrors `param.this.to_py()` for the integer parameters of NUMBER(p, s).
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

// typingSnowflakeAnnotateMedian annotates the MEDIAN function with the correct return type.
//
// Based on Snowflake documentation:
//   - If the expr is FLOAT/DOUBLE -> annotate as DOUBLE (FLOAT is a synonym for DOUBLE)
//   - If the expr is NUMBER(p, s) -> annotate as NUMBER(min(p+3, 38), min(s+3, 37))
func typingSnowflakeAnnotateMedian(a *TypeAnnotator, expression *Expr) *Expr {
	// First annotate the argument to get its type
	expression = a.annotateByArgs(expression, "this")

	// Get the input type
	inputType := expression.This().Type()

	if inputType.IsTypeOf(DT_DOUBLE) {
		// If input is FLOAT/DOUBLE, return DOUBLE (FLOAT is normalized to DOUBLE in Snowflake)
		a.setType(expression, DT_DOUBLE)
	} else {
		// If input is NUMBER(p, s), return NUMBER(min(p+3, 38), min(s+3, 37))
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

		// Build the new NUMBER type
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
	// First annotate the argument to get its type
	expression = a.annotateByArgs(expression, "this")

	// Get the input type
	inputType := expression.This().Type()

	if inputType.IsTypeOf(DT_DECFLOAT) {
		// Special case: DECFLOAT -> DECFLOAT(38)
		a.setType(expression, DataTypeBuild("DECFLOAT", MustDialect("snowflake"), false, true))
	} else if inputType.IsTypeOf(DT_FLOAT, DT_DOUBLE) {
		// Special case: FLOAT/DOUBLE -> DOUBLE
		a.setType(expression, DT_DOUBLE)
	} else {
		// For NUMBER types: determine the scale
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

		// Build the new NUMBER type
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

// typingSnowflakeAnnotateMathWithFloatDecfloat annotates math functions that preserve DECFLOAT
// but return DOUBLE for others.
//
// In Snowflake, trigonometric and exponential math functions:
//   - If input is DECFLOAT -> return DECFLOAT
//   - For integer types (INT, BIGINT, etc.) -> return DOUBLE
//   - For other numeric types (NUMBER, DECIMAL, DOUBLE) -> return DOUBLE
func typingSnowflakeAnnotateMathWithFloatDecfloat(a *TypeAnnotator, expression *Expr) *Expr {
	expression = a.annotateByArgs(expression, "this")

	// If input is DECFLOAT, preserve
	if expression.This().IsTypeOf(DT_DECFLOAT) {
		a.setType(expression, annT(expression.This().Type()))
	} else {
		// For all other types (integers, decimals, etc.), return DOUBLE
		a.setType(expression, DT_DOUBLE)
	}

	return expression
}

func typingSnowflakeAnnotateStrToTime(a *TypeAnnotator, expression *Expr) *Expr {
	// target_type is stored as a DataType instance
	var targetType any = DT_TIMESTAMP
	if targetTypeArg, ok := expression.Arg("target_type").(*Expr); ok && targetTypeArg.IsA(KDataType) {
		targetType = targetTypeArg.Arg("this")
	}
	a.setType(expression, targetType)
	return expression
}

func typingSnowflakeMetadata() ExprMetadataType {
	m := typingMetadata("").copy()

	m.annotators(
		"typing/snowflake.py:245", func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KAddMonths,
		KCeil,
		KDateTrunc,
		KFloor,
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
		"typing/snowflake.py:346", func(a *TypeAnnotator, e *Expr) {
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
	m.annotators("typing/snowflake.py:189", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateKurtosis(a, e) }, KKurtosis)
	m.returns(
		DT_DECFLOAT,
		KToDecfloat,
		KTryToDecfloat,
	)
	m.annotators(
		"typing/snowflake.py:212", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateMathWithFloatDecfloat(a, e) },
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
		"typing/snowflake.py:149", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateVariance(a, e) },
		KVariance,
		KVariancePop,
	)
	m.annotators("typing/snowflake.py:83", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateArgMaxMin(a, e) }, KArgMax)
	m.annotators("typing/snowflake.py:83", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateArgMaxMin(a, e) }, KArgMin)
	m.annotators("typing/snowflake.py:547", func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "expressions") }, KConcatWs)
	m.annotators("typing/snowflake.py:549", func(a *TypeAnnotator, e *Expr) {
		if e.ArgB("source_tz") {
			a.setType(e, DT_TIMESTAMPNTZ)
		} else {
			a.setType(e, DT_TIMESTAMPTZ)
		}
	}, KConvertTimezone)
	m.annotators("typing/snowflake.py:43", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateDateOrTimeAdd(a, e) }, KDateAdd)
	m.annotators("typing/snowflake.py:54", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateDecodeCase(a, e) }, KDecodeCase)
	m.annotators("typing/snowflake.py:557", func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("NUMBER(19, 0)", MustDialect("snowflake"), false, true))
	}, KHashAgg)
	m.annotators("typing/snowflake.py:111", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateMedian(a, e) }, KMedian)
	m.annotators("typing/snowflake.py:19", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateReverse(a, e) }, KReverse)
	m.annotators("typing/snowflake.py:232", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateStrToTime(a, e) }, KStrToTime)
	m.annotators("typing/snowflake.py:43", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateDateOrTimeAdd(a, e) }, KTimeAdd)
	m.annotators("typing/snowflake.py:28", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateTimestampFromParts(a, e) }, KTimestampFromParts)
	m.annotators("typing/snowflake.py:91", func(a *TypeAnnotator, e *Expr) { typingSnowflakeAnnotateWithinGroup(a, e) }, KWithinGroup)

	return m
}
