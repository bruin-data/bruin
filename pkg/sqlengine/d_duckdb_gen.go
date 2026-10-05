package sqlengine

// Port of the DuckDBGenerator class of sqlglot/generators/duckdb.py (sqlglot v30.13.0):
// TRANSFORMS table and method overrides. Methods continue in d_duckdb_gen2.go.

import (
	"fmt"
	"sort"
	"strings"
)

func duckdbCustomizeGenerator(d *Dialect) {
	G := d.G

	// AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS (module level)
	for _, k := range []string{"cluster", "distribute", "sort"} {
		delete(G.AFTER_HAVING_MODIFIER_TRANSFORMS, k)
	}
	keys := make([]string, 0, len(G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS))
	for _, k := range G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS {
		if k != "cluster" && k != "distribute" && k != "sort" {
			keys = append(keys, k)
		}
	}
	G.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = keys

	dateDelta := duckdbDateDeltaToBinaryIntervalOp(true)

	T := G.TRANSFORMS
	T[KAnyValue] = duckdbAnyvalueSQL
	T[KApproxDistinct] = approxCountDistinctSQL
	T[KBoolnot] = duckdbBoolnotSQL
	T[KBooland] = duckdbBoolandSQL
	T[KBoolor] = duckdbBoolorSQL
	T[KArray] = transformPreprocess([]func(*Expr) *Expr{transformInheritStructFieldNames}, inlineArrayUnlessQuery)
	T[KArrayAppend] = arrayAppendSQL("LIST_APPEND", false)
	T[KArrayCompact] = arrayCompactSQL
	T[KArrayConstructCompact] = func(g *Generator, e *Expr) string {
		return g.sql(New(KArrayCompact, "this", New(KArray, "expressions", e.Expressions())))
	}
	T[KArrayConcat] = arrayConcatSQL("LIST_CONCAT")
	T[KArrayContains] = duckdbArrayContainsSQL
	T[KArrayOverlaps] = duckdbArrayOverlapsSQL
	T[KArrayFilter] = renameFunc("LIST_FILTER")
	T[KArrayInsert] = duckdbArrayInsertSQL
	T[KArrayPosition] = func(g *Generator, e *Expr) string {
		if e.ArgB("zero_based") {
			return g.sql(New(
				KSub,
				"this", New(KArrayPosition, "this", e.This(), "expression", e.Expression()),
				"expression", LiteralInt(1),
			))
		}
		return g.fn("ARRAY_POSITION", e.Arg("this"), e.Arg("expression"))
	}
	T[KArrayRemoveAt] = duckdbArrayRemoveAtSQL
	T[KArrayRemove] = removeFromArrayUsingFilter
	T[KArraySort] = duckdbArraySortSQL
	T[KArrayPrepend] = arrayAppendSQL("LIST_PREPEND", true)
	T[KArraySum] = renameFunc("LIST_SUM")
	T[KArrayMax] = renameFunc("LIST_MAX")
	T[KArrayMin] = renameFunc("LIST_MIN")
	T[KBase64DecodeBinary] = func(g *Generator, e *Expr) string { return duckdbBase64DecodeSQL(g, e, false) }
	T[KBase64DecodeString] = func(g *Generator, e *Expr) string { return duckdbBase64DecodeSQL(g, e, true) }
	T[KBitwiseAnd] = func(g *Generator, e *Expr) string { return duckdbBitwiseOp(g, e, "&") }
	T[KBitwiseAndAgg] = duckdbBitwiseAggSQL
	T[KBitwiseCount] = renameFunc("BIT_COUNT")
	T[KBitwiseLeftShift] = duckdbBitshiftSQL
	T[KBitwiseOr] = func(g *Generator, e *Expr) string { return duckdbBitwiseOp(g, e, "|") }
	T[KBitwiseOrAgg] = duckdbBitwiseAggSQL
	T[KBitwiseRightShift] = duckdbBitshiftSQL
	T[KBitwiseXorAgg] = duckdbBitwiseAggSQL
	T[KCommentColumnConstraint] = noCommentColumnConstraintSQL
	T[KCorr] = func(g *Generator, e *Expr) string { return duckdbCorrSQL(g, e) }
	T[KCosineDistance] = renameFunc("LIST_COSINE_DISTANCE")
	T[KCurrentTime] = func(g *Generator, e *Expr) string { return "CURRENT_TIME" }
	T[KCurrentSchemas] = func(g *Generator, e *Expr) string {
		arg := e.This()
		if arg == nil {
			arg = Boolean(true)
		}
		return g.fn("current_schemas", arg)
	}
	T[KCurrentTimestamp] = func(g *Generator, e *Expr) string {
		if e.ArgB("sysdate") {
			return g.sql(New(KAtTimeZone, "this", VarExpr("CURRENT_TIMESTAMP"), "zone", LiteralString("UTC")))
		}
		return "CURRENT_TIMESTAMP"
	}
	T[KCurrentVersion] = renameFunc("version")
	T[KLocaltime] = func(g *Generator, e *Expr) string {
		duckdbUnsupportedArgs(g, e, "this")
		return "LOCALTIME"
	}
	T[KDayOfMonth] = renameFunc("DAYOFMONTH")
	T[KDayOfWeek] = renameFunc("DAYOFWEEK")
	T[KDayOfWeekIso] = renameFunc("ISODOW")
	T[KDayOfYear] = renameFunc("DAYOFYEAR")
	T[KDayname] = func(g *Generator, e *Expr) string {
		if e.ArgB("abbreviated") {
			return g.fn("STRFTIME", e.Arg("this"), LiteralString("%a"))
		}
		return g.fn("DAYNAME", e.Arg("this"))
	}
	T[KMonthname] = func(g *Generator, e *Expr) string {
		if e.ArgB("abbreviated") {
			return g.fn("STRFTIME", e.Arg("this"), LiteralString("%b"))
		}
		return g.fn("MONTHNAME", e.Arg("this"))
	}
	T[KDataType] = duckdbDatatypeSQL
	T[KDate] = duckdbDateSQL
	T[KDateAdd] = dateDelta
	T[KDateFromParts] = duckdbDateFromPartsSQL
	T[KDateSub] = dateDelta
	T[KDateDiff] = duckdbDateDiffSQL
	T[KDateStrToDate] = datestrtodateSQL
	T[KDatetime] = noDatetimeSQL
	T[KDatetimeDiff] = duckdbDateDiffSQL
	T[KDatetimeSub] = dateDelta
	T[KDatetimeAdd] = dateDelta
	T[KDateToDi] = func(g *Generator, e *Expr) string {
		return "CAST(STRFTIME(" + g.sqlKey(e, "this") + ", " + g.d.S.DATEINT_FORMAT + ") AS INT)"
	}
	T[KDecode] = func(g *Generator, e *Expr) string { return encodeDecodeSQL(g, e, "DECODE", false) }
	T[KHexDecodeString] = func(g *Generator, e *Expr) string {
		return g.sql(New(KDecode, "this", New(KUnhex, "this", e.This())))
	}
	T[KDiToDate] = func(g *Generator, e *Expr) string {
		return "CAST(STRPTIME(CAST(" + g.sqlKey(e, "this") + " AS TEXT), " + g.d.S.DATEINT_FORMAT + ") AS DATE)"
	}
	T[KEncode] = func(g *Generator, e *Expr) string { return encodeDecodeSQL(g, e, "ENCODE", false) }
	T[KEqualNull] = func(g *Generator, e *Expr) string {
		return g.sql(New(KNullSafeEQ, "this", e.This(), "expression", e.Expression()))
	}
	T[KEuclideanDistance] = renameFunc("LIST_DISTANCE")
	T[KGenerateDateArray] = duckdbGenerateDatetimeArraySQL
	T[KGenerateSeries] = generateSeriesSQL("GENERATE_SERIES", "RANGE")
	T[KGenerateTimestampArray] = duckdbGenerateDatetimeArraySQL
	T[KGetbit] = getbitSQL
	T[KGroupConcat] = func(g *Generator, e *Expr) string {
		return groupconcatSQL(g, e, "LISTAGG", ",", false, false)
	}
	T[KExplode] = renameFunc("UNNEST")
	T[KIcebergProperty] = func(g *Generator, e *Expr) string { return "" }
	T[KIntDiv] = func(g *Generator, e *Expr) string { return g.binary(e, "//") }
	T[KIsInf] = renameFunc("ISINF")
	T[KIsNan] = renameFunc("ISNAN")
	T[KIsNullValue] = func(g *Generator, e *Expr) string {
		return g.sql(duckdbEQ(duckdbFunc("JSON_TYPE", e.This()), LiteralString("NULL")))
	}
	T[KIsArray] = func(g *Generator, e *Expr) string {
		return g.sql(duckdbEQ(duckdbFunc("JSON_TYPE", e.This()), LiteralString("ARRAY")))
	}
	T[KCeil] = duckdbCeilFloor
	T[KFloor] = duckdbCeilFloor
	T[KJSONBExists] = renameFunc("JSON_EXISTS")
	T[KJSONExtract] = duckdbArrowJSONExtractSQL
	T[KJSONExtractArray] = duckdbJSONExtractValueArraySQL
	T[KJSONFormat] = duckdbJSONFormatSQL
	T[KJSONValueArray] = duckdbJSONExtractValueArraySQL
	T[KLateral] = duckdbExplodeToUnnestSQL
	T[KLogicalOr] = func(g *Generator, e *Expr) string { return g.fn("BOOL_OR", duckdbCastToBoolean(e.This())) }
	T[KLogicalAnd] = func(g *Generator, e *Expr) string { return g.fn("BOOL_AND", duckdbCastToBoolean(e.This())) }
	T[KSelect] = transformPreprocess([]func(*Expr) *Expr{
		duckdbConnectByToRecursiveCte,
		duckdbSeqToRangeInGenerator,
	}, nil)
	T[KSeq1] = func(g *Generator, e *Expr) string { return duckdbSeqSQL(g, e, 1) }
	T[KSeq2] = func(g *Generator, e *Expr) string { return duckdbSeqSQL(g, e, 2) }
	T[KSeq4] = func(g *Generator, e *Expr) string { return duckdbSeqSQL(g, e, 4) }
	T[KSeq8] = func(g *Generator, e *Expr) string { return duckdbSeqSQL(g, e, 8) }
	T[KBoolxorAgg] = duckdbBoolxorAggSQL
	T[KMakeInterval] = func(g *Generator, e *Expr) string { return noMakeIntervalSQL(g, e, " ") }
	T[KInitcap] = duckdbInitcapSQL
	T[KMD5Digest] = func(g *Generator, e *Expr) string { return g.fn("UNHEX", g.fn("MD5", e.This())) }
	T[KSHA] = func(g *Generator, e *Expr) string { return duckdbShaSQL(g, e, "SHA1", false) }
	T[KSHA1Digest] = func(g *Generator, e *Expr) string { return duckdbShaSQL(g, e, "SHA1", true) }
	T[KSHA2] = func(g *Generator, e *Expr) string { return duckdbShaSQL(g, e, "SHA256", false) }
	T[KSHA2Digest] = func(g *Generator, e *Expr) string { return duckdbShaSQL(g, e, "SHA256", true) }
	T[KMonthsBetween] = monthsBetweenSQL
	T[KNextDay] = duckdbDayNavigationSQL
	T[KPercentileCont] = renameFunc("QUANTILE_CONT")
	T[KPercentileDisc] = renameFunc("QUANTILE_DISC")
	// DuckDB doesn't allow qualified columns inside of PIVOT expressions.
	// See: https://github.com/duckdb/duckdb/blob/671faf92411182f81dce42ac43de8bfb05d9909e/src/planner/binder/tableref/bind_pivot.cpp#L61-L62
	T[KPivot] = transformPreprocess([]func(*Expr) *Expr{transformUnqualifyColumns}, nil)
	T[KPreviousDay] = duckdbDayNavigationSQL
	T[KRegexpILike] = func(g *Generator, e *Expr) string {
		return g.fn("REGEXP_MATCHES", e.Arg("this"), e.Arg("expression"), LiteralString("i"))
	}
	T[KRegexpSplit] = renameFunc("STR_SPLIT_REGEX")
	T[KRegrValx] = duckdbRegrValSQL
	T[KRegrValy] = duckdbRegrValSQL
	T[KReturn] = func(g *Generator, e *Expr) string { return g.sqlKey(e, "this") }
	T[KReturnsProperty] = func(g *Generator, e *Expr) string {
		if e.This().IsA(KSchema) {
			return "TABLE"
		}
		return ""
	}
	T[KStrToUnix] = func(g *Generator, e *Expr) string {
		return g.fn("EPOCH", g.fn("STRPTIME", e.This(), duckdbOptStr(g.formatTime(e, nil, nil))))
	}
	T[KStruct] = duckdbStructSQL
	T[KTransform] = renameFunc("LIST_TRANSFORM")
	T[KTimeAdd] = dateDelta
	T[KTimeSub] = dateDelta
	T[KTime] = noTimeSQL
	T[KTimeDiff] = duckdbTimediffSQL
	T[KTimestamp] = noTimestampSQL
	T[KTimestampAdd] = dateDelta
	T[KTimestampDiff] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_DIFF", LiteralString(duckdbTimeUnitName(e)), e.Arg("expression"), e.Arg("this"))
	}
	T[KTimestampSub] = dateDelta
	T[KTimeStrToDate] = func(g *Generator, e *Expr) string {
		return g.sql(CastExpr(e.This(), DT_DATE, true, nil))
	}
	T[KTimeStrToTime] = func(g *Generator, e *Expr) string { return timestrtotimeSQL(g, e, false) }
	T[KTimeStrToUnix] = func(g *Generator, e *Expr) string {
		return g.fn("EPOCH", CastExpr(e.This(), DT_TIMESTAMP, true, nil))
	}
	T[KTimeToStr] = func(g *Generator, e *Expr) string {
		return g.fn("STRFTIME", e.Arg("this"), duckdbOptStr(g.formatTime(e, nil, nil)))
	}
	T[KToBoolean] = duckdbToBooleanSQL
	T[KToVariant] = func(g *Generator, e *Expr) string {
		return g.sql(CastExpr(e.This(), duckdbDataTypeFromStr("VARIANT", MustDialect("duckdb")), true, nil))
	}
	T[KTimeToUnix] = renameFunc("EPOCH")
	T[KTsOrDiToDi] = func(g *Generator, e *Expr) string {
		return "CAST(SUBSTR(REPLACE(CAST(" + g.sqlKey(e, "this") + " AS TEXT), '-', ''), 1, 8) AS INT)"
	}
	T[KTsOrDsAdd] = dateDelta
	T[KTsOrDsDiff] = func(g *Generator, e *Expr) string {
		unit := "DAY"
		if u := e.Arg("unit"); truthy(u) {
			unit = duckdbPyStr(u)
		}
		return g.fn(
			"DATE_DIFF",
			"'"+unit+"'",
			CastExpr(e.Expression(), DT_TIMESTAMP, true, nil),
			CastExpr(e.This(), DT_TIMESTAMP, true, nil),
		)
	}
	T[KUnixMicros] = func(g *Generator, e *Expr) string {
		return g.fn("EPOCH_US", duckdbImplicitDatetimeCast(e.This(), DT_DATE))
	}
	T[KUnixMillis] = func(g *Generator, e *Expr) string {
		return g.fn("EPOCH_MS", duckdbImplicitDatetimeCast(e.This(), DT_DATE))
	}
	T[KUnixSeconds] = func(g *Generator, e *Expr) string {
		return g.sql(duckdbCastAny(g.fn("EPOCH", duckdbImplicitDatetimeCast(e.This(), DT_DATE)), DT_BIGINT, true, nil))
	}
	T[KUnixToStr] = func(g *Generator, e *Expr) string {
		return g.fn("STRFTIME", g.fn("TO_TIMESTAMP", e.This()), duckdbOptStr(g.formatTime(e, nil, nil)))
	}
	T[KDatetimeTrunc] = func(g *Generator, e *Expr) string {
		return g.fn("DATE_TRUNC", unitToStr(e, "DAY"), CastExpr(e.This(), DT_DATETIME, true, nil))
	}
	T[KUnixToTime] = duckdbUnixToTimeSQL
	T[KUnixToTimeStr] = func(g *Generator, e *Expr) string {
		return "CAST(TO_TIMESTAMP(" + g.sqlKey(e, "this") + ") AS TEXT)"
	}
	T[KVariancePop] = renameFunc("VAR_POP")
	T[KWeekOfYear] = renameFunc("WEEKOFYEAR")
	T[KYearOfWeek] = func(g *Generator, e *Expr) string {
		return g.sql(New(KExtract, "this", New(KVar, "this", "ISOYEAR"), "expression", e.This()))
	}
	T[KYearOfWeekIso] = func(g *Generator, e *Expr) string {
		return g.sql(New(KExtract, "this", New(KVar, "this", "ISOYEAR"), "expression", e.This()))
	}
	T[KXor] = duckdbXorSQL
	T[KJSONObjectAgg] = renameFunc("JSON_GROUP_OBJECT")
	T[KJSONBObjectAgg] = renameFunc("JSON_GROUP_OBJECT")
	T[KDateBin] = renameFunc("TIME_BUCKET")
	T[KLastDay] = duckdbLastDaySQL

	// Dialect-only <key>_sql methods
	M := G.methods
	M[KTimeSlice] = duckdbTimesliceSQL
	M[KBitmapBucketNumber] = duckdbBitmapbucketnumberSQL
	M[KBitmapBitPosition] = duckdbBitmapbitpositionSQL
	M[KBitmapConstructAgg] = duckdbBitmapconstructaggSQL
	M[KGetIgnoreCase] = func(g *Generator, e *Expr) string {
		g.unsupported("DuckDB does not support the GET_IGNORE_CASE() function")
		return g.functionFallbackSQL(e)
	}
	M[KCompress] = func(g *Generator, e *Expr) string {
		g.unsupported("DuckDB does not support the COMPRESS() function")
		return g.functionFallbackSQL(e)
	}
	M[KEncrypt] = func(g *Generator, e *Expr) string {
		g.unsupported("ENCRYPT is not supported in DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KDecrypt] = func(g *Generator, e *Expr) string {
		funcName := "DECRYPT"
		if e.ArgB("safe") {
			funcName = "TRY_DECRYPT"
		}
		g.unsupported(funcName + " is not supported in DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KDecryptRaw] = func(g *Generator, e *Expr) string {
		funcName := "DECRYPT_RAW"
		if e.ArgB("safe") {
			funcName = "TRY_DECRYPT_RAW"
		}
		g.unsupported(funcName + " is not supported in DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KEncryptRaw] = func(g *Generator, e *Expr) string {
		g.unsupported("ENCRYPT_RAW is not supported in DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KParseUrl] = func(g *Generator, e *Expr) string {
		g.unsupported("PARSE_URL is not supported in DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KParseIp] = func(g *Generator, e *Expr) string {
		g.unsupported("PARSE_IP is not supported in DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KDecompressString] = func(g *Generator, e *Expr) string {
		g.unsupported("DECOMPRESS_STRING is not supported in DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KDecompressBinary] = func(g *Generator, e *Expr) string {
		g.unsupported("DECOMPRESS_BINARY is not supported in DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KJarowinklerSimilarity] = duckdbJarowinklersimilaritySQL
	M[KNthValue] = duckdbNthvalueSQL
	M[KRandstr] = duckdbRandstrSQL
	M[KReduce] = duckdbReduceSQL
	M[KZipf] = duckdbZipfSQL
	M[KToBinary] = duckdbTobinarySQL
	M[KGenerator] = duckdbGeneratorSQL
	M[KGreatest] = duckdbGreatestLeastSQL
	M[KLeast] = duckdbGreatestLeastSQL
	M[KSoundex] = func(g *Generator, e *Expr) string {
		g.unsupported("SOUNDEX is not supported in DuckDB")
		return g.fn("SOUNDEX", e.Arg("this"))
	}
	M[KSortArray] = duckdbSortarraySQL
	M[KApproxTopK] = func(g *Generator, e *Expr) string {
		g.unsupported("APPROX_TOP_K cannot be transpiled to DuckDB due to incompatible return types. ")
		return g.functionFallbackSQL(e)
	}
	M[KStrPosition] = duckdbStrpositionSQL
	M[KSubstring] = duckdbSubstringSQL
	M[KParseTime] = duckdbParsetimeSQL
	M[KCheckJson] = duckdbCheckjsonSQL
	M[KUnicode] = duckdbUnicodeSQL
	M[KStripNullValue] = duckdbStripnullvalueSQL
	M[KTrunc] = duckdbTruncSQL
	M[KNormal] = duckdbNormalSQL
	M[KUniform] = duckdbUniformSQL
	M[KTimeFromParts] = duckdbTimefrompartsSQL
	M[KTimestampFromParts] = duckdbTimestampfrompartsSQL
	M[KTimestampLtzFromParts] = duckdbTimestampltzfrompartsSQL
	M[KTimestampTzFromParts] = duckdbTimestamptzfrompartsSQL
	M[KCountIf] = duckdbCountifSQL
	M[KLength] = duckdbLengthSQL
	M[KBitLength] = duckdbBitlengthSQL
	M[KCollation] = func(g *Generator, e *Expr) string {
		g.unsupported("COLLATION function is not supported by DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KRegexpCount] = duckdbRegexpcountSQL
	M[KRegexpReplace] = duckdbRegexpreplaceSQL
	M[KRegexpLike] = duckdbRegexplikeSQL
	M[KLevenshtein] = duckdbLevenshteinSQL
	M[KMinhash] = duckdbMinhashSQL
	M[KMinhashCombine] = duckdbMinhashcombineSQL
	M[KApproximateSimilarity] = duckdbApproximatesimilaritySQL
	M[KArrayUniqueAgg] = duckdbArrayuniqueaggSQL
	M[KArrayUnionAgg] = func(g *Generator, e *Expr) string {
		g.unsupported("ARRAY_UNION_AGG is not supported in DuckDB")
		return g.functionFallbackSQL(e)
	}
	M[KArrayDistinct] = duckdbArraydistinctSQL
	M[KArrayIntersect] = duckdbArrayintersectSQL
	M[KArrayExcept] = duckdbArrayexceptSQL
	M[KArraySlice] = duckdbArraysliceSQL
	M[KArraysZip] = duckdbArrayszipSQL
	M[KLower] = func(g *Generator, e *Expr) string {
		resultSQL := g.fn("LOWER", duckdbCastToVarchar(e.This()))
		return duckdbGenWithCastToBlob(g, e, resultSQL)
	}
	M[KUpper] = func(g *Generator, e *Expr) string {
		resultSQL := g.fn("UPPER", duckdbCastToVarchar(e.This()))
		return duckdbGenWithCastToBlob(g, e, resultSQL)
	}
	M[KReverse] = func(g *Generator, e *Expr) string {
		resultSQL := g.fn("REVERSE", duckdbCastToVarchar(e.This()))
		return duckdbGenWithCastToBlob(g, e, resultSQL)
	}
	M[KLeft] = func(g *Generator, e *Expr) string { return duckdbLeftRightSQL(g, e, "LEFT") }
	M[KRight] = func(g *Generator, e *Expr) string { return duckdbLeftRightSQL(g, e, "RIGHT") }
	M[KRtrimmedLength] = func(g *Generator, e *Expr) string {
		return g.fn("LENGTH", New(KTrim, "this", e.This(), "position", "TRAILING"))
	}
	M[KStuff] = duckdbStuffSQL
	M[KByteLength] = duckdbBytelengthSQL
	M[KBase64Encode] = duckdbBase64encodeSQL
	M[KReplace] = duckdbReplaceSQL
	M[KObjectInsert] = duckdbObjectinsertSQL
	M[KMapCat] = func(g *Generator, e *Expr) string {
		result := duckdbReplacePlaceholders(duckdbMapcatTemplate.get().Copy(), map[string]*Expr{
			"map1": e.This(),
			"map2": e.Expression(),
		})
		return g.sql(result)
	}
	M[KMapContainsKey] = func(g *Generator, e *Expr) string {
		return g.fn("ARRAY_CONTAINS", duckdbFunc("MAP_KEYS", e.ArgE("key")), e.Arg("this"))
	}
	M[KMapDelete] = duckdbMapdeleteSQL
	M[KMapPick] = duckdbMappickSQL
	M[KMapSize] = func(g *Generator, e *Expr) string { return g.fn("CARDINALITY", e.Arg("this")) }
	M[KMapInsert] = duckdbMapinsertSQL
	M[KStartsWith] = func(g *Generator, e *Expr) string {
		return g.fn("STARTS_WITH", duckdbCastToVarchar(e.This()), duckdbCastToVarchar(e.Expression()))
	}
	M[KSplit] = duckdbSplitSQL
	M[KSplitPart] = duckdbSplitpartSQL
	M[KArrayToString] = duckdbArraytostringSQL
	M[KRegexpExtract] = duckdbRegexpExtractSQL
	M[KRegexpExtractAll] = duckdbRegexpExtractSQL
	M[KRegexpInstr] = duckdbRegexpinstrSQL
	M[KNumberToStr] = duckdbNumbertostrSQL
	M[KPosexplode] = duckdbPosexplodeSQL
	M[KAddMonths] = duckdbAddmonthsSQL
	M[KFormat] = func(g *Generator, e *Expr) string {
		if pyLower(e.Name()) == "%s" && len(e.Expressions()) == 1 {
			return g.fn("FORMAT", "'{}'", e.Expressions()[0])
		}
		return g.functionFallbackSQL(e)
	}
	M[KDateTrunc] = duckdbDatetruncSQL
	M[KTimestampTrunc] = duckdbTimestamptruncSQL
	M[KRound] = duckdbRoundSQL
	M[KStrtok] = duckdbStrtokSQL
	M[KStrtokToArray] = duckdbStrtoktoarraySQL
	M[KApproxQuantile] = duckdbApproxquantileSQL
	M[KApproxQuantiles] = duckdbApproxquantilesSQL
	M[KJSONExtractScalar] = duckdbJsonextractscalarSQL

	// Method overrides of the base Generator
	H := &G.h
	H.tonumberSQL = duckdbTonumberSQL
	H.lambdaSQL = duckdbLambdaSQL
	H.showSQL = duckdbShowSQL
	H.installSQL = duckdbInstallSQL
	H.strtotimeSQL = duckdbStrtotimeSQL
	H.strtodateSQL = duckdbStrtodateSQL
	H.parsedatetimeSQL = duckdbParsedatetimeSQL
	H.tsordstotimeSQL = duckdbTsordstotimeSQL
	H.currentdateSQL = duckdbCurrentdateSQL
	H.parsejsonSQL = duckdbParsejsonSQL
	H.extractSQL = duckdbExtractSQL
	H.tablesampleSQL = duckdbTablesampleSQL
	H.joinSQL = duckdbJoinSQL
	H.bracketSQL = duckdbBracketSQL
	H.withingroupSQL = duckdbWithingroupSQL
	H.chrSQL = duckdbChrSQL
	H.collateSQL = duckdbCollateSQL
	H.padSQL = duckdbPadSQL
	H.randSQL = duckdbRandSQL
	H.hexSQL = duckdbHexSQL
	H.bitwisexorSQL = duckdbBitwisexorSQL
	H.spaceSQL = duckdbSpaceSQL
	H.tablefromrowsSQL = duckdbTablefromrowsSQL
	H.unnestSQL = duckdbUnnestSQL
	H.ignorenullsSQL = duckdbIgnorenullsSQL
	H.respectnullsSQL = duckdbRespectnullsSQL
	H.concatwsSQL = duckdbConcatwsSQL
	H.autoincrementcolumnconstraintSQL = func(g *Generator, _ *Expr) string {
		g.unsupported("The AUTOINCREMENT column constraint is not supported by DuckDB")
		return ""
	}
	H.aliasesSQL = duckdbAliasesSQL
	H.hexstringSQL = func(g *Generator, e *Expr, _ string) string {
		// UNHEX('FF') correctly produces blob \xFF in DuckDB
		return g.baseHexstringSQL(e, "UNHEX")
	}
	H.trimSQL = duckdbTrimSQL
	H.trycastSQL = duckdbTrycastSQL
	H.bitwisenotSQL = duckdbBitwisenotSQL
	H.windowSQL = duckdbWindowSQL
	H.filterSQL = duckdbFilterSQL
	H.uuidSQL = duckdbUuidSQL
}

// duckdbUnsupportedArgs mirrors the @unsupported_args(...) decorator for plain argument names.
func duckdbUnsupportedArgs(g *Generator, e *Expr, args ...string) {
	for _, arg := range args {
		if e.ArgB(arg) {
			g.unsupported(fmt.Sprintf("Argument '%s' is not supported for expression '%s' when targeting %s.", arg, e.Kind().Name(), g.d.ClassName))
		}
	}
}

// duckdbOptStr maps a Generator.format_time result ("" = None) to a Generator.func argument.
func duckdbOptStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// duckdbPyStr mirrors f"{value}" for an arg value (expressions render with the default dialect).
func duckdbPyStr(v any) string {
	switch x := v.(type) {
	case nil:
		return "None"
	case *Expr:
		return exprSQL(x)
	case string:
		return x
	case bool:
		if x {
			return "True"
		}
		return "False"
	}
	return fmt.Sprint(v)
}

// duckdbTimeUnitName mirrors TimeUnit.unit (via `e.unit`) rendered as Literal.string(e.unit):
// sqlglot calls str() on the unit expression.
func duckdbTimeUnitName(e *Expr) string {
	return duckdbPyStr(e.Arg("unit"))
}

// duckdbRaw wraps an already generated SQL string so it can be placed in an expression list
// (Python puts the string itself in the list; Var renders its text verbatim).
func duckdbRaw(sql string) *Expr { return New(KVar, "this", sql) }

// duckdbCastAny mirrors exp.cast(x, to, copy, dialect) where x may be a SQL string (parsed with
// the default dialect, like maybe_parse).
func duckdbCastAny(x any, to any, copy bool, d *Dialect) *Expr {
	switch v := x.(type) {
	case string:
		return CastExpr(MaybeParse(v, KNone, "", nil), to, false, d)
	case *Expr:
		return CastExpr(v, to, copy, d)
	}
	panic(parsePanic{&ParseError{Msg: "SQL cannot be None"}})
}

// duckdbNonNil drops nil entries (Python Anonymous expressions lists containing None are
// skipped by Generator.func).
func duckdbNonNil(xs ...*Expr) []*Expr {
	out := make([]*Expr, 0, len(xs))
	for _, x := range xs {
		if x != nil {
			out = append(out, x)
		}
	}
	return out
}

// duckdbArrayBagSQL mirrors DuckDBGenerator._array_bag_sql.
func duckdbArrayBagSQL(g *Generator, condition *Expr, arr1, arr2 *Expr) string {
	cond := New(KParen, "this", duckdbReplacePlaceholders(condition, map[string]*Expr{"arr1": arr1, "arr2": arr2}))
	return g.sql(duckdbReplacePlaceholders(duckdbArrayBagTemplate.get(), map[string]*Expr{
		"arr1": arr1, "arr2": arr2, "cond": cond,
	}))
}

// duckdbTimesliceSQL mirrors timeslice_sql.
//
// Transform Snowflake's TIME_SLICE to DuckDB's time_bucket.
func duckdbTimesliceSQL(g *Generator, e *Expr) string {
	dateExpr := e.This()
	sliceLength := e.Expression()
	unit := e.ArgE("unit")
	kind := pyUpper(e.Text("kind"))

	// Create INTERVAL expression: INTERVAL 'N' UNIT
	intervalExpr := New(KInterval, "this", sliceLength, "unit", unit)

	// Create base time_bucket expression
	timeBucketExpr := duckdbFunc("time_bucket", intervalExpr, dateExpr)

	// Check if we need the end of the slice (default is start)
	if kind != "END" {
		// For 'START', return time_bucket directly
		return g.sql(timeBucketExpr)
	}

	// For 'END', add the interval to get end of slice
	addExpr := New(KAdd, "this", timeBucketExpr, "expression", intervalExpr.Copy())

	// If input is DATE type, cast result back to DATE to preserve type
	// DuckDB converts DATE to TIMESTAMP when adding intervals
	if duckdbIsType(dateExpr, DT_DATE) {
		return g.sql(CastExpr(addExpr, DT_DATE, true, nil))
	}

	return g.sql(addExpr)
}

// duckdbBitmapbucketnumberSQL mirrors bitmapbucketnumber_sql.
func duckdbBitmapbucketnumberSQL(g *Generator, e *Expr) string {
	value := e.This()

	positiveFormula := duckdbAdd(duckdbIntDiv(duckdbSub(value, 1), 32768), 1)
	nonPositiveFormula := duckdbIntDiv(value, 32768)

	// CASE WHEN value > 0 THEN ((value - 1) // 32768) + 1 ELSE value // 32768 END
	caseExpr := duckdbWhen(duckdbCase(), New(KGT, "this", value, "expression", LiteralInt(0)), positiveFormula, true)
	caseExpr = duckdbElse(caseExpr, nonPositiveFormula, true)
	return g.sql(caseExpr)
}

// duckdbBitmapbitpositionSQL mirrors bitmapbitposition_sql.
func duckdbBitmapbitpositionSQL(g *Generator, e *Expr) string {
	this := e.This()

	return g.sql(New(
		KMod,
		"this", New(KParen, "this", New(
			KIf,
			"this", New(KGT, "this", this, "expression", LiteralInt(0)),
			"true", duckdbSub(this, LiteralInt(1)),
			"false", New(KAbs, "this", this),
		)),
		"expression", duckdbMaxBitPosition(),
	))
}

// duckdbBitmapconstructaggSQL mirrors bitmapconstructagg_sql.
func duckdbBitmapconstructaggSQL(g *Generator, e *Expr) string {
	arg := e.This()
	return "(" + g.sql(duckdbReplacePlaceholders(duckdbBitmapConstructAggTemplate.get(), map[string]*Expr{"arg": arg})) + ")"
}

// duckdbJarowinklersimilaritySQL mirrors jarowinklersimilarity_sql.
func duckdbJarowinklersimilaritySQL(g *Generator, e *Expr) string {
	this := e.This()
	expr := e.Expression()

	if e.ArgB("case_insensitive") {
		this = New(KUpper, "this", this)
		expr = New(KUpper, "this", expr)
	}

	result := duckdbFunc("JARO_WINKLER_SIMILARITY", this, expr)

	if e.ArgB("integer_scale") {
		result = CastExpr(duckdbMul(result, 100), "INTEGER", true, nil)
	}

	return g.sql(result)
}

// duckdbNthvalueSQL mirrors nthvalue_sql.
func duckdbNthvalueSQL(g *Generator, e *Expr) string {
	fromFirst := true
	if e.HasArgKey("from_first") {
		fromFirst = truthy(e.Arg("from_first"))
	}
	if !fromFirst {
		g.unsupported("DuckDB's NTH_VALUE doesn't support starting from the end ")
	}

	return g.functionFallbackSQL(e)
}

// duckdbRandstrSQL mirrors randstr_sql.
//
// Transpile Snowflake's RANDSTR to DuckDB equivalent using deterministic hash-based random.
func duckdbRandstrSQL(g *Generator, e *Expr) string {
	length := e.This()
	generator := e.ArgE("generator")

	var seedValue *Expr
	if generator != nil {
		if generator.IsA(KRand) {
			// If it's RANDOM(), use its seed if available, otherwise use RANDOM() itself
			seedValue = generator.This()
			if seedValue == nil {
				seedValue = generator
			}
		} else {
			// Const/int or other expression - use as seed directly
			seedValue = generator
		}
	} else {
		// No generator specified, use default seed (arbitrary but deterministic)
		seedValue = LiteralInt(duckdbRandstrSeed)
	}

	return "(" + g.sql(duckdbReplacePlaceholders(duckdbRandstrTemplate.get(), map[string]*Expr{
		"seed": seedValue, "length": length,
	})) + ")"
}

// duckdbReduceSQL mirrors reduce_sql.
func duckdbReduceSQL(g *Generator, e *Expr) string {
	duckdbUnsupportedArgs(g, e, "finish")
	arrayArg := e.This()
	initialValue := e.ArgE("initial")
	mergeLambda := e.ArgE("merge")

	if mergeLambda != nil {
		mergeLambda.Set("colon", true)
	}

	return g.fn("list_reduce", arrayArg, mergeLambda, initialValue)
}

// duckdbZipfSQL mirrors zipf_sql.
//
// Transpile Snowflake's ZIPF to DuckDB using CDF-based inverse sampling.
func duckdbZipfSQL(g *Generator, e *Expr) string {
	s := e.This()
	n := e.ArgE("elementcount")
	gen := e.ArgE("gen")

	var randomExpr *Expr
	if !gen.IsA(KRand) {
		// (ABS(HASH(seed)) % 1000000) / 1000000.0
		randomExpr = New(
			KDiv,
			"this", New(KParen, "this", New(
				KMod,
				"this", New(KAbs, "this", New(KAnonymous, "this", "HASH", "expressions", []*Expr{gen.Copy()})),
				"expression", LiteralInt(1000000),
			)),
			"expression", LiteralNumber("1000000.0"),
		)
	} else {
		// Use RANDOM() for non-deterministic output
		randomExpr = New(KRand)
	}

	return "(" + g.sql(duckdbReplacePlaceholders(duckdbZipfTemplate.get(), map[string]*Expr{
		"s": s, "n": n, "random_expr": randomExpr,
	})) + ")"
}

// duckdbTobinarySQL mirrors tobinary_sql.
func duckdbTobinarySQL(g *Generator, e *Expr) string {
	value := e.This()
	formatArg := e.ArgE("format")
	isSafe := e.ArgB("safe")
	isBinary := duckdbIsBinary(e)

	if formatArg == nil && !isBinary {
		funcName := "TO_BINARY"
		if isSafe {
			funcName = "TRY_TO_BINARY"
		}
		return g.fn(funcName, value)
	}

	// Snowflake defaults to HEX encoding when no format is specified
	fmtName := "HEX"
	if formatArg != nil {
		fmtName = pyUpper(formatArg.Name())
	}

	var result string
	switch fmtName {
	case "UTF-8", "UTF8":
		// DuckDB ENCODE always uses UTF-8, no charset parameter needed
		result = g.fn("ENCODE", value)
	case "BASE64":
		result = g.fn("FROM_BASE64", value)
	case "HEX":
		result = g.fn("UNHEX", value)
	default:
		if isSafe {
			return g.sql(Null())
		}
		g.unsupported(fmt.Sprintf("format %s is not supported", fmtName))
		result = g.fn("TO_BINARY", value)
	}
	if isSafe {
		return "TRY(" + result + ")"
	}
	return result
}

// duckdbTonumberSQL mirrors tonumber_sql.
func duckdbTonumberSQL(g *Generator, e *Expr) string {
	format := e.ArgE("format")
	precision := e.ArgE("precision")
	scale := e.ArgE("scale")

	if format == nil && precision != nil && scale != nil {
		return g.sql(CastExpr(e.This(), "DECIMAL("+precision.Name()+", "+scale.Name()+")", true, MustDialect("duckdb")))
	}

	return g.baseTonumberSQL(e)
}

// duckdbGreatestLeastSQL mirrors _greatest_least_sql.
//
// Handle GREATEST/LEAST functions with dialect-aware NULL behavior.
func duckdbGreatestLeastSQL(g *Generator, e *Expr) string {
	// Get all arguments
	allArgs := append([]*Expr{e.This()}, e.Expressions()...)
	fallbackSQL := g.functionFallbackSQL(e)

	if e.ArgB("ignore_nulls") {
		// DuckDB/PostgreSQL behavior: use native GREATEST/LEAST (ignores NULLs)
		return g.sql(fallbackSQL)
	}

	// return NULL if any argument is NULL
	conds := make([]*Expr, 0, len(allArgs))
	for _, arg := range allArgs {
		conds = append(conds, duckdbIs(arg, Null()))
	}
	caseExpr := duckdbWhen(duckdbCase(), OrExprOpts(conds, false, true), Null(), false)
	caseExpr.Set("default", fallbackSQL)
	return g.sql(caseExpr)
}

// duckdbGeneratorSQL mirrors generator_sql.
func duckdbGeneratorSQL(g *Generator, e *Expr) string {
	// Transpile Snowflake GENERATOR to DuckDB range()
	rowcount := e.ArgE("rowcount")
	timeLimit := e.ArgE("time_limit")

	if timeLimit != nil {
		g.unsupported("GENERATOR TIMELIMIT parameter is not supported in DuckDB")
	}

	if rowcount == nil {
		g.unsupported("GENERATOR without ROWCOUNT is not supported in DuckDB")
		return g.fn("range", LiteralInt(0))
	}

	return g.fn("range", rowcount)
}

// duckdbLambdaSQL mirrors lambda_sql.
func duckdbLambdaSQL(g *Generator, e *Expr, arrowSep string, wrap bool) string {
	prefix := ""
	if e.ArgB("colon") {
		prefix = "LAMBDA "
		arrowSep = ":"
		wrap = false
	}

	lambdaSQL := g.baseLambdaSQL(e, arrowSep, wrap)
	return prefix + lambdaSQL
}

// duckdbShowSQL mirrors show_sql.
func duckdbShowSQL(g *Generator, e *Expr) string {
	from := g.sqlKey(e, "from_")
	if from != "" {
		from = " FROM " + from
	}
	return "SHOW " + e.Name() + from
}

// duckdbSortarraySQL mirrors sortarray_sql.
func duckdbSortarraySQL(g *Generator, e *Expr) string {
	arr := e.This()
	asc := e.ArgE("asc")
	nullsFirst := e.ArgE("nulls_first")

	if !asc.IsA(KBoolean) && !nullsFirst.IsA(KBoolean) {
		return g.fn("LIST_SORT", arr, asc, nullsFirst)
	}

	nullsAreFirst := nullsFirst != nil && nullsFirst.Equal(Boolean(true))
	var nullsFirstSQL *Expr
	if nullsAreFirst {
		nullsFirstSQL = LiteralString("NULLS FIRST")
	}

	if !asc.IsA(KBoolean) {
		return g.fn("LIST_SORT", arr, asc, nullsFirstSQL)
	}

	descending := asc.Equal(Boolean(false))

	if !descending && !nullsAreFirst {
		return g.fn("LIST_SORT", arr)
	}
	if !nullsAreFirst {
		return g.fn("ARRAY_REVERSE_SORT", arr)
	}
	order := "ASC"
	if descending {
		order = "DESC"
	}
	return g.fn("LIST_SORT", arr, LiteralString(order), LiteralString("NULLS FIRST"))
}

// duckdbInstallSQL mirrors install_sql.
func duckdbInstallSQL(g *Generator, e *Expr) string {
	force := ""
	if e.ArgB("force") {
		force = "FORCE "
	}
	this := g.sqlKey(e, "this")
	fromClause := ""
	if f := e.Arg("from_"); truthy(f) {
		// f-string formatting renders the expression with the default dialect
		fromClause = " FROM " + duckdbPyStr(f)
	}
	return force + "INSTALL " + this + fromClause
}

// duckdbStrpositionSQL mirrors strposition_sql.
func duckdbStrpositionSQL(g *Generator, e *Expr) string {
	this := e.This()
	substr := e.ArgE("substr")
	position := e.ArgE("position")

	// For BINARY/BLOB: DuckDB's STRPOS doesn't support BLOB types
	// Convert to HEX strings, use STRPOS, then convert hex position to byte position
	if duckdbIsBinary(this) {
		// Build expression: STRPOS(HEX(haystack), HEX(needle))
		hexStrpos := New(
			KStrPosition,
			"this", New(KHex, "this", this),
			"substr", New(KHex, "this", substr),
		)

		return g.sql(CastExpr(duckdbDiv(duckdbAdd(hexStrpos, 1), 2), DT_INT, true, nil))
	}

	// For VARCHAR: handle clamp_position
	if e.ArgB("clamp_position") && position != nil {
		e = e.Copy()
		e.Set("position", New(
			KIf,
			"this", New(KLTE, "this", position, "expression", LiteralInt(0)),
			"true", LiteralInt(1),
			"false", position.Copy(),
		))
	}

	return strpositionSQL(g, e, "STRPOS", false, false, true)
}

// duckdbSubstringSQL mirrors substring_sql.
func duckdbSubstringSQL(g *Generator, e *Expr) string {
	if e.ArgB("zero_start") {
		start := e.ArgE("start")
		length := e.ArgE("length")

		if start != nil {
			start = New(KIf, "this", duckdbEQ(start, 0), "true", LiteralInt(1), "false", start)
		}
		if length != nil {
			length = New(KIf, "this", duckdbLT(length, 0), "true", LiteralInt(0), "false", length)
		}

		return g.fn("SUBSTRING", e.Arg("this"), start, length)
	}

	return g.functionFallbackSQL(e)
}

// duckdbStrptimeDefaultYear mirrors _strptime_default_year. The formatted time is nil (None),
// a string or an expression.
func duckdbStrptimeDefaultYear(g *Generator, e *Expr) (*Expr, any) {
	value := e.This()
	formattedTime := duckdbOptStr(g.formatTime(e, nil, nil))

	if defaultYear := e.ArgE("default_year"); defaultYear != nil {
		value = New(KDPipe, "this", LiteralString(defaultYear.Name()+" "), "expression", value)
		formattedTime = New(KDPipe, "this", LiteralString("%Y "), "expression", formattedTime)
	}

	return value, formattedTime
}

// duckdbStrtotimeSQL mirrors strtotime_sql.
func duckdbStrtotimeSQL(g *Generator, e *Expr) string {
	// Check if target_type requires TIMESTAMPTZ (for LTZ/TZ variants)
	targetType := e.ArgE("target_type")
	needsTz := false
	if targetType != nil {
		t := targetType.Arg("this")
		needsTz = t == DT_TIMESTAMPLTZ || t == DT_TIMESTAMPTZ
	}

	value, formattedTime := duckdbStrptimeDefaultYear(g, e)

	if e.ArgB("safe") {
		castType := DT_TIMESTAMP
		if needsTz {
			castType = DT_TIMESTAMPTZ
		}
		return g.sql(duckdbCastAny(g.fn("TRY_STRPTIME", value, formattedTime), castType, true, nil))
	}

	baseSQL := g.fn("STRPTIME", value, formattedTime)
	if needsTz {
		return g.sql(duckdbCastAny(baseSQL, duckdbNewTypeExpr(DT_TIMESTAMPTZ), true, nil))
	}
	return baseSQL
}

// duckdbStrtodateSQL mirrors strtodate_sql.
func duckdbStrtodateSQL(g *Generator, e *Expr) string {
	value, formattedTime := duckdbStrptimeDefaultYear(g, e)
	functionName := "STRPTIME"
	if e.ArgB("safe") {
		functionName = "TRY_STRPTIME"
	}
	return g.sql(duckdbCastAny(g.fn(functionName, value, formattedTime), duckdbNewTypeExpr(DT_DATE), true, nil))
}

// duckdbParsedatetimeSQL mirrors parsedatetime_sql.
func duckdbParsedatetimeSQL(g *Generator, e *Expr) string {
	value, formattedTime := duckdbStrptimeDefaultYear(g, e)
	return g.fn("STRPTIME", value, formattedTime)
}

// duckdbParsetimeSQL mirrors parsetime_sql.
func duckdbParsetimeSQL(g *Generator, e *Expr) string {
	formattedTime := duckdbOptStr(g.formatTime(e, nil, nil))
	return g.sql(duckdbCastAny(g.fn("STRPTIME", e.This(), formattedTime), duckdbNewTypeExpr(DT_TIME), true, nil))
}

// duckdbTsordstotimeSQL mirrors tsordstotime_sql.
func duckdbTsordstotimeSQL(g *Generator, e *Expr) string {
	this := e.This()
	timeFormat := g.formatTime(e, nil, nil)
	safe := e.ArgB("safe")
	timeType := duckdbDataTypeFromStr("TIME", MustDialect("duckdb"))
	castExpr := KCast
	if safe {
		castExpr = KTryCast
	}

	if timeFormat != "" {
		funcName := "STRPTIME"
		if safe {
			funcName = "TRY_STRPTIME"
		}
		strptime := New(KAnonymous, "this", funcName, "expressions", []*Expr{this, duckdbRaw(timeFormat)})
		return g.sql(New(castExpr, "this", strptime, "to", timeType))
	}

	if this.IsA(KTsOrDsToTime) || duckdbIsType(this, DT_TIME) {
		return g.sql(this)
	}

	return g.sql(New(castExpr, "this", this, "to", timeType))
}

// duckdbCurrentdateSQL mirrors currentdate_sql.
func duckdbCurrentdateSQL(g *Generator, e *Expr) string {
	if e.This() == nil {
		return "CURRENT_DATE"
	}

	expr := New(
		KCast,
		"this", New(KAtTimeZone, "this", New(KCurrentTimestamp), "zone", e.This()),
		"to", duckdbNewTypeExpr(DT_DATE),
	)
	return g.sql(expr)
}

// duckdbCheckjsonSQL mirrors checkjson_sql.
func duckdbCheckjsonSQL(g *Generator, e *Expr) string {
	arg := e.This()
	c := duckdbWhen(duckdbCase(),
		OrExpr(duckdbIs(arg, Null()), duckdbEQ(arg, ""), duckdbFunc("json_valid", arg)),
		Null(), true)
	c = duckdbElse(c, LiteralString("Invalid JSON"), true)
	return g.sql(c)
}

// duckdbParsejsonSQL mirrors parsejson_sql.
func duckdbParsejsonSQL(g *Generator, e *Expr) string {
	arg := e.This()
	if e.ArgB("safe") {
		c := duckdbWhen(duckdbCase(), duckdbFunc("json_valid", arg), CastExpr(arg.Copy(), "JSON", true, nil), true)
		c = duckdbElse(c, Null(), true)
		return g.sql(c)
	}
	return g.fn("JSON", arg)
}

// duckdbUnicodeSQL mirrors unicode_sql.
func duckdbUnicodeSQL(g *Generator, e *Expr) string {
	if e.ArgB("empty_is_zero") {
		c := duckdbWhen(duckdbCase(), duckdbEQ(e.This(), LiteralString("")), LiteralInt(0), true)
		c = duckdbElse(c, New(KAnonymous, "this", "UNICODE", "expressions", []*Expr{e.This()}), true)
		return g.sql(c)
	}

	return g.fn("UNICODE", e.Arg("this"))
}

// duckdbStripnullvalueSQL mirrors stripnullvalue_sql.
func duckdbStripnullvalueSQL(g *Generator, e *Expr) string {
	c := duckdbWhen(duckdbCase(), duckdbEQ(duckdbFunc("json_type", e.This()), "NULL"), Null(), true)
	c = duckdbElse(c, e.This(), true)
	return g.sql(c)
}

// duckdbTruncSQL mirrors trunc_sql.
func duckdbTruncSQL(g *Generator, e *Expr) string {
	decimals := e.ArgE("decimals")
	if e.ArgB("fractions_supported") && decimals != nil && !duckdbIsType(decimals, DT_INT) {
		decimals = CastExpr(decimals, DT_INT, true, MustDialect("duckdb"))
	}

	return g.fn("TRUNC", e.Arg("this"), decimals)
}

// duckdbNormalSQL mirrors normal_sql.
//
// Transpile Snowflake's NORMAL(mean, stddev, gen) to DuckDB using the Box-Muller transform.
func duckdbNormalSQL(g *Generator, e *Expr) string {
	mean := e.This()
	stddev := e.ArgE("stddev")
	gen := e.ArgE("gen")

	// Build two uniform random values [0, 1) for Box-Muller transform
	var u1, u2 *Expr
	if gen.IsA(KRand) && gen.This() == nil {
		u1 = New(KRand)
		u2 = New(KRand)
	} else {
		// Seeded: derive two values using HASH with different inputs
		seed := gen
		if gen.IsA(KRand) {
			seed = gen.This()
		}
		u1 = duckdbReplacePlaceholders(duckdbSeededRandomTemplate.get(), map[string]*Expr{"seed": seed})
		u2 = duckdbReplacePlaceholders(duckdbSeededRandomTemplate.get(), map[string]*Expr{
			"seed": New(KAdd, "this", seed.Copy(), "expression", LiteralInt(1)),
		})
	}

	return g.sql(duckdbReplacePlaceholders(duckdbNormalTemplate.get(), map[string]*Expr{
		"mean": mean, "stddev": stddev, "u1": u1, "u2": u2,
	}))
}

// duckdbUniformSQL mirrors uniform_sql.
//
// Transpile Snowflake's UNIFORM(min, max, gen) to DuckDB.
func duckdbUniformSQL(g *Generator, e *Expr) string {
	minVal := e.This()
	maxVal := e.Expression()
	gen := e.ArgE("gen")

	// Determine if result should be integer (both bounds are integers).
	// We do this to emulate Snowflake's behavior, INT -> INT, FLOAT -> FLOAT
	isIntResult := minVal.IsInt() && maxVal.IsInt()

	// Build the random value expression [0, 1)
	var randomExpr *Expr
	if !gen.IsA(KRand) {
		// Seed value: (ABS(HASH(seed)) % 1000000) / 1000000.0
		randomExpr = New(
			KDiv,
			"this", New(KParen, "this", New(
				KMod,
				"this", New(KAbs, "this", New(KAnonymous, "this", "HASH", "expressions", []*Expr{gen})),
				"expression", LiteralInt(1000000),
			)),
			"expression", LiteralNumber("1000000.0"),
		)
	} else {
		randomExpr = New(KRand)
	}

	// Build: min + random * (max - min [+ 1 for int])
	rangeExpr := New(KSub, "this", maxVal, "expression", minVal)
	if isIntResult {
		rangeExpr = New(KAdd, "this", rangeExpr, "expression", LiteralInt(1))
	}

	result := New(
		KAdd,
		"this", minVal,
		"expression", New(KMul, "this", randomExpr, "expression", New(KParen, "this", rangeExpr)),
	)

	if isIntResult {
		result = New(KCast, "this", New(KFloor, "this", result), "to", duckdbNewTypeExpr(DT_BIGINT))
	}

	return g.sql(result)
}

// duckdbTimefrompartsSQL mirrors timefromparts_sql.
func duckdbTimefrompartsSQL(g *Generator, e *Expr) string {
	nano := e.ArgE("nano")
	overflow := e.ArgB("overflow")

	// Snowflake's TIME_FROM_PARTS supports overflow
	if overflow {
		hour := e.ArgE("hour")
		minute := e.ArgE("min")
		sec := e.ArgE("sec")

		// Check if values are within normal ranges - use MAKE_TIME for efficiency
		if nano == nil && hour.IsInt() && minute.IsInt() && sec.IsInt() {
			hVal, _ := duckdbToPyInt(hour)
			mVal, _ := duckdbToPyInt(minute)
			sVal, _ := duckdbToPyInt(sec)
			if 0 <= hVal && hVal <= 23 && 0 <= mVal && mVal <= 59 && 0 <= sVal && sVal <= 59 {
				return renameFunc("MAKE_TIME")(g, e)
			}
		}

		// Overflow or nanoseconds detected - use INTERVAL arithmetic
		if nano != nil {
			sec = duckdbAdd(sec, duckdbDiv(nano.Pop(), LiteralNumber("1000000000.0")))
		}

		totalSeconds := duckdbAdd(duckdbAdd(duckdbMul(hour, LiteralInt(3600)), duckdbMul(minute, LiteralInt(60))), sec)

		return g.sql(New(
			KAdd,
			"this", New(KCast, "this", LiteralString("00:00:00"), "to", duckdbNewTypeExpr(DT_TIME)),
			"expression", New(KInterval, "this", totalSeconds, "unit", VarExpr("SECOND")),
		))
	}

	// Default: MAKE_TIME
	if nano != nil {
		e.Set("sec", duckdbAdd(e.ArgE("sec"), duckdbDiv(nano.Pop(), LiteralNumber("1000000000.0"))))
	}

	return renameFunc("MAKE_TIME")(g, e)
}

// duckdbExtractSQL mirrors extract_sql.
//
// Transpile EXTRACT/DATE_PART for DuckDB, handling specifiers not natively supported.
func duckdbExtractSQL(g *Generator, e *Expr) string {
	this := e.This()
	datetimeExpr := e.Expression()

	// TIMESTAMPTZ extractions may produce different results between Snowflake and DuckDB
	// because Snowflake applies server timezone while DuckDB uses local timezone
	if duckdbIsType(datetimeExpr, DT_TIMESTAMPTZ, DT_TIMESTAMPLTZ) {
		g.unsupported("EXTRACT from TIMESTAMPTZ / TIMESTAMPLTZ may produce different results due to timezone handling differences")
	}

	partName := pyUpper(this.Name())

	if m, ok := g.s.EXTRACT_STRFTIME_MAPPINGS[partName]; ok {
		fmtStr, castType := m[0][0], m[1][0]

		// Problem: strftime doesn't accept TIME and there's no NANOSECOND function
		// So, for NANOSECOND with TIME, fallback to MICROSECOND * 1000
		isNanoTime := partName == "NANOSECOND" && duckdbIsType(datetimeExpr, DT_TIME, DT_TIMETZ)

		if isNanoTime {
			g.unsupported("Parameter NANOSECOND is not supported with TIME type in DuckDB")
			return g.sql(CastExpr(
				New(
					KMul,
					"this", New(KExtract, "this", VarExpr("MICROSECOND"), "expression", datetimeExpr),
					"expression", LiteralInt(1000),
				),
				duckdbDataTypeFromStr(castType, MustDialect("duckdb")),
				true, nil,
			))
		}

		// For NANOSECOND, cast to TIMESTAMP_NS to preserve nanosecond precision
		strftimeInput := datetimeExpr
		if partName == "NANOSECOND" {
			strftimeInput = CastExpr(datetimeExpr, DT_TIMESTAMP_NS, true, nil)
		}

		return g.sql(CastExpr(
			New(KAnonymous, "this", "STRFTIME", "expressions", []*Expr{strftimeInput, LiteralString(fmtStr)}),
			duckdbDataTypeFromStr(castType, MustDialect("duckdb")),
			true, nil,
		))
	}

	if funcName, ok := g.s.EXTRACT_EPOCH_MAPPINGS[partName]; ok {
		result := New(KAnonymous, "this", funcName, "expressions", []*Expr{datetimeExpr})
		// EPOCH returns float, cast to BIGINT for integer result
		if partName == "EPOCH_SECOND" {
			result = CastExpr(result, duckdbDataTypeFromStr("BIGINT", MustDialect("duckdb")), true, nil)
		}
		return g.sql(result)
	}

	return g.baseExtractSQL(e)
}

// duckdbTimestampfrompartsSQL mirrors timestampfromparts_sql.
func duckdbTimestampfrompartsSQL(g *Generator, e *Expr) string {
	// Check if this is the date/time expression form: TIMESTAMP_FROM_PARTS(date_expr, time_expr)
	dateExpr := e.This()
	timeExpr := e.Expression()

	if dateExpr != nil && timeExpr != nil {
		// In DuckDB, DATE + TIME produces TIMESTAMP
		return g.sql(New(KAdd, "this", dateExpr, "expression", timeExpr))
	}

	// Component-based form: TIMESTAMP_FROM_PARTS(year, month, day, hour, minute, second, ...)
	sec := e.ArgE("sec")
	if sec == nil {
		// This shouldn't happen with valid input, but handle gracefully
		return renameFunc("MAKE_TIMESTAMP")(g, e)
	}

	milli := e.ArgE("milli")
	if milli != nil {
		sec = duckdbAdd(sec, duckdbDiv(milli.Pop(), LiteralNumber("1000.0")))
	}

	nano := e.ArgE("nano")
	if nano != nil {
		sec = duckdbAdd(sec, duckdbDiv(nano.Pop(), LiteralNumber("1000000000.0")))
	}

	if milli != nil || nano != nil {
		e.Set("sec", sec)
	}

	return renameFunc("MAKE_TIMESTAMP")(g, e)
}

// duckdbTimestampltzfrompartsSQL mirrors timestampltzfromparts_sql.
func duckdbTimestampltzfrompartsSQL(g *Generator, e *Expr) string {
	duckdbUnsupportedArgs(g, e, "nano")
	// Pop nano so rename_func only passes args that MAKE_TIMESTAMP accepts
	if nano := e.ArgE("nano"); nano != nil {
		nano.Pop()
	}

	timestamp := renameFunc("MAKE_TIMESTAMP")(g, e)
	return "CAST(" + timestamp + " AS TIMESTAMPTZ)"
}

// duckdbTimestamptzfrompartsSQL mirrors timestamptzfromparts_sql.
func duckdbTimestamptzfrompartsSQL(g *Generator, e *Expr) string {
	duckdbUnsupportedArgs(g, e, "nano")
	// Extract zone before popping
	zone := e.ArgE("zone")
	// Pop zone and nano so rename_func only passes args that MAKE_TIMESTAMP accepts
	if zone != nil {
		zone = zone.Pop()
	}

	if nano := e.ArgE("nano"); nano != nil {
		nano.Pop()
	}

	timestamp := renameFunc("MAKE_TIMESTAMP")(g, e)

	if zone != nil {
		// Use AT TIME ZONE to apply the explicit timezone
		return timestamp + " AT TIME ZONE " + g.sql(zone)
	}

	return timestamp
}

// duckdbTablesampleSQL mirrors tablesample_sql.
func duckdbTablesampleSQL(g *Generator, e *Expr, tablesampleKeyword string) string {
	if !e.Parent().IsA(KSelect) {
		// This sample clause only applies to a single source, not the entire resulting relation
		tablesampleKeyword = "TABLESAMPLE"
	}

	if e.ArgB("size") {
		method := e.ArgE("method")
		if method != nil && pyUpper(method.Name()) != "RESERVOIR" {
			g.unsupported(fmt.Sprintf("Sampling method %s is not supported with a discrete sample count, "+
				"defaulting to reservoir sampling", exprSQL(method)))
			e.Set("method", VarExpr("RESERVOIR"))
		}
	}

	return g.baseTablesampleSQL(e, tablesampleKeyword)
}

// duckdbJoinSQL mirrors join_sql.
func duckdbJoinSQL(g *Generator, e *Expr) string {
	kind := e.KindText()
	if !e.ArgB("using") &&
		!e.ArgB("on") &&
		e.MethodText() == "" &&
		(kind == "" || kind == "INNER" || kind == "OUTER") {
		// Some dialects support `LEFT/INNER JOIN UNNEST(...)` without an explicit ON clause
		// DuckDB doesn't, but we can just add a dummy ON clause that is always true
		if e.This().IsA(KUnnest) {
			return g.baseJoinSQL(e.JoinOn([]*Expr{Boolean(true)}, true, true))
		}

		e.Set("side", nil)
		e.Set("kind", nil)
	}

	return g.baseJoinSQL(e)
}

// duckdbCountifSQL mirrors countif_sql.
func duckdbCountifSQL(g *Generator, e *Expr) string {
	if !duckdbVersionLT(g.d, 1, 2) {
		this := e.This()
		if e.ArgB("zero_on_all_null") && !this.IsA(KDistinct) {
			// DuckDB >= 1.2's COUNT_IF returns NULL when the condition is NULL on all rows,
			// so we wrap the condition in IS TRUE to preserve count-like semantics
			e = New(KCountIf, "this", duckdbIs(ParenExpr(this, true), Boolean(true)))
		}
		return g.functionFallbackSQL(e)
	}

	// https://github.com/tobymao/sqlglot/pull/4749
	return countIfToSum(g, e)
}

// duckdbBracketSQL mirrors bracket_sql.
func duckdbBracketSQL(g *Generator, e *Expr) string {
	if !duckdbVersionLT(g.d, 1, 2) {
		return g.baseBracketSQL(e)
	}

	// https://duckdb.org/2025/02/05/announcing-duckdb-120.html#breaking-changes
	this := e.This()
	if this.IsA(KArray) {
		this.Replace(ParenExpr(this, true))
	}

	bracket := g.baseBracketSQL(e)

	if !e.ArgB("returns_list_for_maps") {
		if this.Type() == nil {
			this = annotateTypes(this, g.d)
		}

		if duckdbIsType(this, DT_MAP) {
			bracket = "(" + bracket + ")[1]"
		}
	}

	return bracket
}

// duckdbWithingroupSQL mirrors withingroup_sql.
func duckdbWithingroupSQL(g *Generator, e *Expr) string {
	fn := e.This()

	// For ARRAY_AGG, DuckDB requires ORDER BY inside the function, not in WITHIN GROUP
	// Transform: ARRAY_AGG(x) WITHIN GROUP (ORDER BY y) -> ARRAY_AGG(x ORDER BY y)
	if fn.IsA(KArrayAgg) {
		order := e.Expression()
		if !order.IsA(KOrder) {
			return g.sql(fn)
		}

		// Save the original column for FILTER clause (before wrapping with Order)
		originalThis := fn.This()

		// Move ORDER BY inside ARRAY_AGG by wrapping its argument with Order
		// ArrayAgg.this should become Order(this=ArrayAgg.this, expressions=order.expressions)
		fn.Set("this", New(
			KOrder,
			"this", fn.This().Copy(),
			"expressions", order.Expressions(),
		))

		// Generate the ARRAY_AGG function with ORDER BY and add FILTER clause if needed
		// Use original_this (not the Order-wrapped version) for the FILTER condition
		arrayAggSQL := g.functionFallbackSQL(fn)
		return g.addArrayaggNullFilter(arrayAggSQL, fn, originalThis)
	}

	// For other functions (like PERCENTILES), use existing logic
	expressionSQL := g.sqlKey(e, "expression")

	if fn.IsA(KPercentileCont, KPercentileDisc) {
		// Make the order key the first arg and slide the fraction to the right
		// https://duckdb.org/docs/sql/aggregates#ordered-set-aggregate-functions
		orderCol := e.Find(KOrdered)
		if orderCol != nil {
			fn.Set("expression", fn.This())
			fn.Set("this", orderCol.This())
		}
	}

	this := strings.TrimRight(g.sqlKey(e, "this"), ")")

	return this + expressionSQL + ")"
}

// duckdbLengthSQL mirrors length_sql.
func duckdbLengthSQL(g *Generator, e *Expr) string {
	arg := e.This()

	// Dialects like BQ and Snowflake also accept binary values as args, so
	// DDB will attempt to infer the type or resort to case/when resolution
	if !e.ArgB("binary") || arg.IsString() {
		return g.fn("LENGTH", arg)
	}

	if arg.Type() == nil {
		arg = annotateTypes(arg, g.d)
	}

	if duckdbIsType(arg, DataType_TEXT_TYPES.Items()...) {
		return g.fn("LENGTH", arg)
	}

	// We need these casts to make duckdb's static type checker happy
	blob := CastExpr(arg, DT_VARBINARY, true, nil)
	varchar := CastExpr(arg, DT_VARCHAR, true, nil)

	c := CaseExpr(New(KAnonymous, "this", "TYPEOF", "expressions", []*Expr{arg}), true)
	c = duckdbWhen(c, LiteralString("BLOB"), New(KByteLength, "this", blob), true)
	c = duckdbElse(c, New(KAnonymous, "this", "LENGTH", "expressions", []*Expr{varchar}), true)
	return g.sql(c)
}

// duckdbBitlengthSQL mirrors bitlength_sql.
func duckdbBitlengthSQL(g *Generator, e *Expr) string {
	arg := e.This()
	if !duckdbIsBinary(arg) {
		return g.fn("BIT_LENGTH", arg)
	}

	blob := CastExpr(arg, DT_VARBINARY, true, nil)
	return g.sql(duckdbMul(New(KByteLength, "this", blob), LiteralInt(8)))
}

// duckdbChrSQL mirrors chr_sql.
func duckdbChrSQL(g *Generator, e *Expr, _ string) string {
	arg := e.Expressions()[0]
	if duckdbIsType(arg, DataType_REAL_TYPES.Items()...) {
		arg = CastExpr(arg, DT_INT, true, nil)
	}
	return g.fn("CHR", arg)
}

// duckdbCollateSQL mirrors collate_sql.
func duckdbCollateSQL(g *Generator, e *Expr) string {
	if !e.Expression().IsString() {
		return g.baseCollateSQL(e)
	}

	raw := e.Expression().Name()
	if raw == "" {
		return g.sql(e.Arg("this"))
	}

	var parts []string
	for _, part := range strings.Split(raw, "-") {
		lower := pyLower(part)
		if !duckdbSnowflakeCollationDefaults.Has(lower) {
			if duckdbSnowflakeCollationUnsupported.Has(lower) {
				g.unsupported("Snowflake collation specifier '" + part + "' has no DuckDB equivalent")
			}
			parts = append(parts, lower)
		}
	}

	if len(parts) == 0 {
		return g.sql(e.Arg("this"))
	}
	return g.baseCollateSQL(New(KCollate, "this", e.This(), "expression", VarChecked(strings.Join(parts, "."))))
}

// duckdbValidateRegexpFlags mirrors _validate_regexp_flags; "" mirrors None.
//
// Validate and filter regexp flags for DuckDB compatibility.
func duckdbValidateRegexpFlags(g *Generator, flags *Expr, supportedFlags string) string {
	if flags == nil {
		return ""
	}

	if !flags.IsString() {
		g.unsupported("Non-literal regexp flags are not fully supported in DuckDB")
		return ""
	}

	flagStr := flags.ThisS()
	unsupportedSet := map[string]bool{}
	for _, f := range flagStr {
		if !strings.ContainsRune(supportedFlags, f) {
			unsupportedSet[string(f)] = true
		}
	}

	if len(unsupportedSet) > 0 {
		var items []string
		for k := range unsupportedSet {
			items = append(items, k)
		}
		sort.Strings(items)
		reprs := make([]string, len(items))
		for i, it := range items {
			reprs[i] = pyRepr(it)
		}
		g.unsupported("Regexp flags [" + strings.Join(reprs, ", ") + "] are not supported in this context")
	}

	var b strings.Builder
	for _, f := range flagStr {
		if strings.ContainsRune(supportedFlags, f) {
			b.WriteRune(f)
		}
	}
	return b.String()
}
