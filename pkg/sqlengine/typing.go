package sqlengine

import "sync"

// ExprMetadata is a type annotation table entry: an annotator callback or a static return type.
type ExprMetadata struct {
	Annotator func(a *TypeAnnotator, e *Expr)
	// Returns is a DType or a DataType *Expr (shared, not copied per use).
	Returns any
}

// ExprMetadataType is a type annotation table: metadata keyed by exact expression kind.
type ExprMetadataType map[Kind]*ExprMetadata

// copy returns a shallow copy of m (entries are shared).
func (m ExprMetadataType) copy() ExprMetadataType {
	out := make(ExprMetadataType, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func (m ExprMetadataType) annotators(fn func(a *TypeAnnotator, e *Expr), kinds ...Kind) {
	spec := &ExprMetadata{Annotator: fn}
	for _, k := range kinds {
		m[k] = spec
	}
}

func (m ExprMetadataType) returns(r any, kinds ...Kind) {
	spec := &ExprMetadata{Returns: r}
	for _, k := range kinds {
		m[k] = spec
	}
}

var typingTIMESTAMP_EXPRESSIONS = []Kind{
	KCurrentTimestamp,
	KStrToTime,
	KTimeStrToTime,
	KTimestampAdd,
	KTimestampSub,
	KUnixToTime,
}

var typingBinarySubclasses = []Kind{
	KAdd, KAdjacent, KAnd, KArrayContainedBy, KArrayContains, KArrayContainsAll, KArrayOverlaps,
	KArrayPosition, KBinary, KBitwiseAnd, KBitwiseLeftShift, KBitwiseOr, KBitwiseRightShift, KBitwiseXor,
	KCollate, KConnector, KCorr, KDPipe, KDistance, KDistanceNd, KDiv, KDot, KEQ, KEscape, KExtendsLeft,
	KExtendsRight, KGT, KGTE, KGlob, KILike, KIntDiv, KIs, KJSONArrayContains, KJSONBContains,
	KJSONBContainsAllTopKeys, KJSONBContainsAnyTopKeys, KJSONBContainsTopKey, KJSONBDeleteAtPath, KJSONBExtract,
	KJSONBExtractScalar, KJSONBPathExists, KJSONExtract, KJSONExtractScalar, KKwarg, KLT, KLTE, KLike,
	KMatch, KMod, KMul, KNEQ, KNestedJSONSelect, KNullSafeEQ, KNullSafeNEQ, KOperator, KOr, KOverlaps,
	KPow, KPropertyEQ, KRegexpFullMatch, KRegexpILike, KRegexpLike, KSimilarTo, KSub, KXor,
}

var typingUnarySubclasses = []Kind{
	KAlias, KBitwiseNot, KIgnoreNulls, KNeg, KNot, KParen, KPivotAlias, KRespectNulls, KUnary,
}

func typingAnnotateByNumericArg(a *TypeAnnotator, e *Expr) {
	t := e.This().Type()
	if t != nil && annDTypeIn(t.Arg("this"), DataType_NUMERIC_TYPES.With(DT_UNKNOWN)) {
		a.setType(e, t)
	} else {
		a.setType(e, DT_INT)
	}
}

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
		KToChar,
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
		KArraySort,
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
	m.annotators(typingAnnotateByNumericArg, KCeil, KFloor)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateToMap(e) }, KToMap)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateUnnest(e) }, KUnnest)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateWithinGroup(e) }, KWithinGroup)
	m.annotators(func(a *TypeAnnotator, e *Expr) { a.annotateSubquery(e) }, KSubquery)

	return m
}

// typingModules maps a dialect name ("" for the base table) to its type annotation table builder.
// Builders of derived dialects start from their parent's table (e.g. spark from spark2).
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

// typingMetadata returns the (cached) type annotation table registered under module. Tables are
// built lazily, on first use, because some entries are DataTypes parsed from SQL.
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

// expressionMetadataFor returns the type annotation table of the closest dialect in d's
// inheritance chain (d itself, then its parents) that registers one in typingModules, falling
// back to the base table.
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
