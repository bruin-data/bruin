package sqlengine

import "sync"

// Port of sqlglot/typing/__init__.py (EXPRESSION_METADATA) plus the selection of the per-dialect
// metadata (Dialect.EXPRESSION_METADATA class attribute).

// ExprMetadata mirrors an entry of an EXPRESSION_METADATA dict: either an annotator callback
// ({"annotator": fn}) or a static return type ({"returns": DType | DataType}).
type ExprMetadata struct {
	Annotator func(a *TypeAnnotator, e *Expr)
	// Returns is a DType or a DataType *Expr (shared, like the Python object).
	Returns any
}

// ExprMetadataType mirrors sqlglot.typing.ExprMetadataType: metadata keyed by exact expression class.
type ExprMetadataType map[Kind]*ExprMetadata

// copy mirrors dict.copy() / {**EXPRESSION_METADATA}.
func (m ExprMetadataType) copy() ExprMetadataType {
	out := make(ExprMetadataType, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// annotators mirrors `**{expr_type: {"annotator": fn} for expr_type in kinds}`.
func (m ExprMetadataType) annotators(fn func(a *TypeAnnotator, e *Expr), kinds ...Kind) {
	spec := &ExprMetadata{Annotator: fn}
	for _, k := range kinds {
		m[k] = spec
	}
}

// returns mirrors `**{expr_type: {"returns": r} for expr_type in kinds}`.
func (m ExprMetadataType) returns(r any, kinds ...Kind) {
	spec := &ExprMetadata{Returns: r}
	for _, k := range kinds {
		m[k] = spec
	}
}

// typingTIMESTAMP_EXPRESSIONS mirrors sqlglot.typing.TIMESTAMP_EXPRESSIONS.
var typingTIMESTAMP_EXPRESSIONS = []Kind{
	KCurrentTimestamp,
	KStrToTime,
	KTimeStrToTime,
	KTimestampAdd,
	KTimestampSub,
	KUnixToTime,
}

// typingBinarySubclasses mirrors subclasses(exp.__name__, exp.Binary).
var typingBinarySubclasses = []Kind{
	KAdd, KAdjacent, KAnd, KArrayContainedBy, KArrayContains, KArrayContainsAll, KArrayOverlaps,
	KArrayPosition, KBinary, KBitwiseAnd, KBitwiseLeftShift, KBitwiseOr, KBitwiseRightShift, KBitwiseXor,
	KCollate, KConnector, KCorr, KDPipe, KDistance, KDistanceNd, KDiv, KDot, KEQ, KEscape, KExtendsLeft,
	KExtendsRight, KGT, KGTE, KGlob, KILike, KIntDiv, KIs, KJSONArrayContains, KJSONBContains,
	KJSONBContainsAllTopKeys, KJSONBContainsAnyTopKeys, KJSONBDeleteAtPath, KJSONBExtract,
	KJSONBExtractScalar, KJSONBPathExists, KJSONExtract, KJSONExtractScalar, KKwarg, KLT, KLTE, KLike,
	KMatch, KMod, KMul, KNEQ, KNestedJSONSelect, KNullSafeEQ, KNullSafeNEQ, KOperator, KOr, KOverlaps,
	KPow, KPropertyEQ, KRegexpFullMatch, KRegexpILike, KRegexpLike, KSimilarTo, KSub, KXor,
}

// typingUnarySubclasses mirrors subclasses(exp.__name__, (exp.Unary, exp.Alias, exp.IgnoreNulls, exp.RespectNulls)).
var typingUnarySubclasses = []Kind{
	KAlias, KBitwiseNot, KIgnoreNulls, KNeg, KNot, KParen, KPivotAlias, KRespectNulls, KUnary,
}

// typingBaseMetadata builds sqlglot.typing.EXPRESSION_METADATA.
func typingBaseMetadata() ExprMetadataType {
	m := ExprMetadataType{}

	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateBinary(e) }, typingBinarySubclasses...)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateUnary(e) }, typingUnarySubclasses...)
	m.returns(
		DT_BIGINT,
		KApproxDistinct,
		KArraySize,
		KCountIf,
		KDenseRank,
		KInt64,
		KNtile,
		KRank,
		KRowNumber,
		KUnixSeconds,
		KUnixMicros,
		KUnixMillis,
	)
	m.returns(
		DT_BINARY,
		KFromBase32,
		KFromBase64,
	)
	m.returns(
		DT_BOOLEAN,
		KAll,
		KAny,
		KArrayContains,
		KBetween,
		KBoolean,
		KContains,
		KEndsWith,
		KExists,
		KIn,
		KIsInf,
		KIsNan,
		KLogicalAnd,
		KLogicalOr,
		KRegexpLike,
		KStartsWith,
	)
	m.returns(
		DT_DATE,
		KCurrentDate,
		KDate,
		KDateFromParts,
		KDateStrToDate,
		KDiToDate,
		KLastDay,
		KStrToDate,
		KTimeStrToDate,
		KTsOrDsToDate,
	)
	m.returns(
		DT_DATETIME,
		KCurrentDatetime,
		KDatetime,
		KDatetimeAdd,
		KDatetimeSub,
	)
	m.returns(
		DT_DOUBLE,
		KAsin,
		KAsinh,
		KAcos,
		KCovarPop,
		KCovarSamp,
		KAcosh,
		KApproxQuantile,
		KAtan,
		KAtanh,
		KAvg,
		KCbrt,
		KCos,
		KCosh,
		KCot,
		KDegrees,
		KExp,
		KKurtosis,
		KLn,
		KLog,
		KPi,
		KPow,
		KPercentileCont,
		KQuantile,
		KRadians,
		KRound,
		KSafeDivide,
		KSin,
		KSinh,
		KSqrt,
		KStddev,
		KStddevPop,
		KStddevSamp,
		KRand,
		KTan,
		KTanh,
		KToDouble,
		KCumeDist,
		KPercentRank,
		KVariance,
		KVariancePop,
		KSkewness,
	)
	m.returns(
		DT_INT,
		KAscii,
		KBitLength,
		KCeil,
		KDatetimeDiff,
		KDayOfMonth,
		KDayOfWeek,
		KDayOfYear,
		KGetbit,
		KHour,
		KTimestampDiff,
		KTimeDiff,
		KUnicode,
		KDateToDi,
		KLevenshtein,
		KLength,
		KSign,
		KStrPosition,
		KTsOrDiToDi,
		KQuarter,
		KUnixDate,
	)
	m.returns(
		DT_INTERVAL,
		KInterval,
		KJustifyDays,
		KJustifyHours,
		KJustifyInterval,
		KMakeInterval,
	)
	m.returns(
		DT_JSON,
		KParseJSON,
	)
	m.returns(
		DT_TIME,
		KCurrentTime,
		KLocaltime,
		KTime,
		KTimeAdd,
		KTimeSub,
	)
	m.returns(
		DT_TIMESTAMPLTZ,
		KTimestampLtzFromParts,
	)
	m.returns(
		DT_TIMESTAMPTZ,
		KCurrentTimestampLTZ,
		KTimestampTzFromParts,
	)
	m.returns(DT_TIMESTAMP, typingTIMESTAMP_EXPRESSIONS...)
	m.returns(
		DT_TINYINT,
		KDay,
		KDayOfWeekIso,
		KMonth,
		KWeek,
		KWeekOfYear,
		KYear,
		KYearOfWeek,
		KYearOfWeekIso,
	)
	m.returns(
		DT_VARCHAR,
		KArrayToString,
		KConcat,
		KConcatWs,
		KChr,
		KCurrentCatalog,
		KCurrentSchema,
		KCurrentVersion,
		KCurrentUser,
		KDayname,
		KDateToDateStr,
		KDPipe,
		KGroupConcat,
		KInitcap,
		KLower,
		KMD5,
		KMonthname,
		KRawString,
		KRepeat,
		KSHA,
		KSHA2,
		KSessionUser,
		KSpace,
		KString,
		KSubstring,
		KTimeToStr,
		KTimeToTimeStr,
		KTrim,
		KToBase32,
		KToBase64,
		KTranslate,
		KTsOrDsToDateStr,
		KTypeof,
		KUnixToStr,
		KUnixToTimeStr,
		KUpper,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this") },
		KAbs,
		KAnyValue,
		KArrayConcatAgg,
		KArrayReverse,
		KArraySlice,
		KFilter,
		KFirstValue,
		KHavingMax,
		KLastValue,
		KLimit,
		KNthValue,
		KOrder,
		KSortArray,
		KWindow,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this", "expressions") },
		KArrayConcat,
		KCoalesce,
		KGreatest,
		KLeast,
		KMax,
		KMin,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateByArrayElement(e) },
		KArrayFirst,
		KArrayLast,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, annT(a.schema.GetUDFType(e, nil, nil)))
	}, KAnonymous)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateTimeunit(e) },
		KDateAdd,
		KDateSub,
		KDateTrunc,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.setType(e, annT(e.ArgE("to"))) },
		KCast,
		KTryCast,
	)
	m.annotators(
		func(a *TypeAnnotator, e *Expr) { a.annotateMap(e) },
		KMap,
		KVarMap,
	)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.annotateByArgsFull(e, false, true, "expressions")
	}, KArray)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.annotateByArgsFull(e, false, true, "this")
	}, KArrayAgg)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateBracket(e) }, KBracket)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		var args []any
		for _, ifExpr := range e.ArgL("ifs") {
			args = append(args, annT(ifExpr.ArgE("true")))
		}
		args = append(args, "default")
		a.annotateByArgs(e, args...)
	}, KCase)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		if e.ArgB("big_int") {
			a.setType(e, DT_BIGINT)
		} else {
			a.setType(e, DT_INT)
		}
	}, KCount)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		if e.ArgB("big_int") {
			a.setType(e, DT_BIGINT)
		} else {
			a.setType(e, DT_INT)
		}
	}, KDateDiff)
	m.annotators(func(a *TypeAnnotator, e *Expr) {}, KDataType)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateDiv(e) }, KDiv)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "expressions") }, KDistinct)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateDot(e) }, KDot)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateExplode(e) }, KExplode)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateExtract(e) }, KExtract)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		if e.ArgB("is_integer") {
			a.setType(e, DT_BIGINT)
		} else {
			a.setType(e, DT_BINARY)
		}
	}, KHexString)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.annotateByArgsFull(e, false, true, "start", "end", "step")
	}, KGenerateSeries)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<DATE>", nil, false, true))
	}, KGenerateDateArray)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.setType(e, DataTypeBuild("ARRAY<TIMESTAMP>", nil, false, true))
	}, KGenerateTimestampArray)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "true", "false") }, KIf)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this", "default") }, KLag)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this", "default") }, KLead)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateLiteral(e) }, KLiteral)
	m.returns(DT_NULL, KNull)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "this", "expression") }, KNullif)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateByArgs(e, "expression") }, KPropertyEQ)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateStruct(e) }, KStruct)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		a.annotateByArgsFull(e, true, false, "this", "expressions")
	}, KSum)
	m.annotators(func(a *TypeAnnotator, e *Expr) {
		if e.ArgB("with_tz") {
			a.setType(e, DT_TIMESTAMPTZ)
		} else {
			a.setType(e, DT_TIMESTAMP)
		}
	}, KTimestamp)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateToMap(e) }, KToMap)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateUnnest(e) }, KUnnest)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateWithinGroup(e) }, KWithinGroup)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateSubquery(e) }, KSubquery)

	return m
}

// typingModules maps a sqlglot.typing module name to its EXPRESSION_METADATA builder. Builders of
// derived modules start from their parent module's table (e.g. typing.spark from typing.spark2).
// It is populated in init() to avoid an initialization cycle (builders call typingMetadata).
var typingModules map[string]func() ExprMetadataType

func init() {
	typingModules = map[string]func() ExprMetadataType{
		"":           typingBaseMetadata,
		"bigquery":   typingBigQueryMetadata,
		"clickhouse": typingClickHouseMetadata,
		"databricks": typingDatabricksMetadata,
		"duckdb":     typingDuckDBMetadata,
		"hive":       typingHiveMetadata,
		"mysql":      typingMySQLMetadata,
		"postgres":   typingPostgresMetadata,
		"presto":     typingPrestoMetadata,
		"redshift":   typingRedshiftMetadata,
		"snowflake":  typingSnowflakeMetadata,
		"spark":      typingSparkMetadata,
		"spark2":     typingSpark2Metadata,
		"tsql":       typingTSQLMetadata,
	}
}

var (
	typingCacheMu sync.Mutex
	typingCache   = map[string]ExprMetadataType{}
	typingOnce    = map[string]*sync.Once{}
)

// typingMetadata returns the (cached) EXPRESSION_METADATA of a sqlglot.typing module. Tables are
// built lazily because some entries are DataTypes parsed at module import time.
func typingMetadata(module string) ExprMetadataType {
	typingCacheMu.Lock()
	once, ok := typingOnce[module]
	if !ok {
		once = &sync.Once{}
		typingOnce[module] = once
	}
	typingCacheMu.Unlock()

	once.Do(func() {
		m := typingModules[module]()
		typingCacheMu.Lock()
		typingCache[module] = m
		typingCacheMu.Unlock()
	})

	typingCacheMu.Lock()
	defer typingCacheMu.Unlock()
	return typingCache[module]
}

// expressionMetadataFor mirrors the Dialect.EXPRESSION_METADATA class attribute: the closest
// dialect class in the inheritance chain that defines it (every dialect with a sqlglot.typing
// module does), falling back to the base sqlglot.typing.EXPRESSION_METADATA.
func expressionMetadataFor(d *Dialect) ExprMetadataType {
	if d == nil {
		return typingMetadata("")
	}
	for _, name := range append([]string{d.Name}, d.parents...) {
		if name == "" {
			continue
		}
		if _, ok := typingModules[name]; ok {
			return typingMetadata(name)
		}
	}
	return typingMetadata("")
}
