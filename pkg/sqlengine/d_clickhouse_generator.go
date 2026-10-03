package sqlengine

import (
	"fmt"
	"math/big"
	"strings"
	"time"
)

// Port of sqlglot/generators/clickhouse.py (ClickHouseGenerator).

// ---------------------------------------------------------------------------------------------
// Module-level helpers
// ---------------------------------------------------------------------------------------------

// clickhouseUnixToTimeSQL mirrors _unix_to_time_sql.
func clickhouseUnixToTimeSQL(g *Generator, e *Expr) string {
	scale := e.ArgE("scale")
	timestamp := e.This()

	if scale == nil || scale.Equal(LiteralInt(0)) {
		return g.fn("fromUnixTimestamp", CastExpr(timestamp, DT_BIGINT, true, nil))
	}
	if scale.Equal(LiteralInt(3)) {
		return g.fn("fromUnixTimestamp64Milli", CastExpr(timestamp, DT_BIGINT, true, nil))
	}
	if scale.Equal(LiteralInt(6)) {
		return g.fn("fromUnixTimestamp64Micro", CastExpr(timestamp, DT_BIGINT, true, nil))
	}
	if scale.Equal(LiteralInt(9)) {
		return g.fn("fromUnixTimestamp64Nano", CastExpr(timestamp, DT_BIGINT, true, nil))
	}

	return g.fn(
		"fromUnixTimestamp",
		CastExpr(New(KDiv, "this", timestamp, "expression", dhFunc("POW", 10, scale)), DT_BIGINT, true, nil),
	)
}

// clickhouseLowerFunc mirrors _lower_func.
func clickhouseLowerFunc(sql string) string {
	index := strings.Index(sql, "(")
	if index < 0 {
		panic(&ValueError{Msg: "substring not found"})
	}
	return pyLower(sql[:index]) + sql[index:]
}

// clickhouseQuantileSQL mirrors _quantile_sql.
func clickhouseQuantileSQL(g *Generator, e *Expr) string {
	quantile := e.ArgE("quantile")
	args := "(" + g.sqlKey(e, "this") + ")"

	var fn string
	if quantile.IsA(KArray) {
		qs := quantile.Expressions()
		anyArgs := make([]any, len(qs))
		for i, q := range qs {
			anyArgs[i] = q
		}
		fn = g.fn("quantiles", anyArgs...)
	} else {
		fn = g.fn("quantile", quantile)
	}

	return fn + args
}

// clickhouseDatetimeDeltaSQL mirrors _datetime_delta_sql(name).
func clickhouseDatetimeDeltaSQL(name string) GenFunc {
	return func(g *Generator, e *Expr) string {
		if !e.ArgB("unit") {
			return renameFunc(name)(g, e)
		}

		return g.fn(
			name,
			unitToVar(e, "DAY"),
			e.Arg("expression"),
			e.Arg("this"),
			e.Arg("zone"),
		)
	}
}

// clickhouseTimestrtotimeSQL mirrors _timestrtotime_sql.
func clickhouseTimestrtotimeSQL(g *Generator, e *Expr) string {
	ts := e.This()

	tz := e.ArgE("zone")
	if tz != nil && ts.IsA(KLiteral) {
		// Clickhouse will not accept timestamps that include a UTC offset, so we must remove them.
		// The first step to removing is parsing the string with `datetime.datetime.fromisoformat`.
		//
		// In python <3.11, `fromisoformat()` can only parse timestamps of millisecond (3 digit)
		// or microsecond (6 digit) precision. It will error if passed any other number of fractional
		// digits, so we extract the fractional seconds and pad to 6 digits before parsing.
		tsString := pyStrip(ts.Name())

		// separate [date and time] from [fractional seconds and UTC offset]
		tsParts := strings.Split(tsString, ".")
		if len(tsParts) == 2 {
			// separate fractional seconds and UTC offset
			offsetSep := "-"
			if strings.Contains(tsParts[1], "+") {
				offsetSep = "+"
			}
			tsFracParts := strings.Split(tsParts[1], offsetSep)
			numFracParts := len(tsFracParts)

			// pad to 6 digits if fractional seconds present
			if n := len([]rune(tsFracParts[0])); n < 6 {
				tsFracParts[0] += strings.Repeat("0", 6-n)
			}
			sep, offset := "", ""
			if numFracParts > 1 {
				sep = offsetSep
				offset = tsFracParts[1]
			}
			tsString = tsParts[0] + "." + tsFracParts[0] + sep + offset
		}

		// return literal with no timezone, eg turn '2020-01-01 12:13:14-08:00' into '2020-01-01 12:13:14'
		// this is because Clickhouse encodes the timezone as a data type parameter and throws an error if
		// it's part of the timestamp string
		tsWithoutTz, ok := clickhouseISOFormatWithoutTz(tsString)
		if !ok {
			panic(&ValueError{Msg: fmt.Sprintf("Invalid isoformat string: %s", pyRepr(tsString))})
		}
		ts = LiteralString(tsWithoutTz)
	}

	// Non-nullable DateTime64 with microsecond precision
	expressions := []*Expr{New(KDataTypeParam, "this", LiteralNumber("6"))}
	if tz != nil {
		expressions = append(expressions, New(KDataTypeParam, "this", tz))
	}
	datatype := New(KDataType, "this", DT_DATETIME64)
	datatype.Set("expressions", expressions)
	datatype.Set("nullable", false)

	return g.sql(CastExpr(ts, datatype, true, g.d))
}

// clickhouseISOFormatWithoutTz mirrors
// datetime.datetime.fromisoformat(s).replace(tzinfo=None).isoformat(sep=" ").
func clickhouseISOFormatWithoutTz(s string) (string, bool) {
	year, month, day, hour, minute, second, microsecond, ok := clickhouseFromISOFormat(s)
	if !ok {
		return "", false
	}
	out := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, minute, second)
	if microsecond != 0 {
		out += fmt.Sprintf(".%06d", microsecond)
	}
	return out, true
}

// clickhouseFromISOFormat mirrors CPython's datetime.fromisoformat (C implementation) and returns
// the parsed datetime fields; ok=false where Python raises ValueError. It follows
// dhFromISOFormatMicrosecond but keeps all components.
func clickhouseFromISOFormat(s string) (year, month, day, hour, minute, second, microsecond int, ok bool) {
	r := []rune(s)
	n := len(r)
	if n < 7 {
		return
	}
	at := func(i int) rune {
		if i >= 0 && i < n {
			return r[i]
		}
		return 0
	}
	isDigit := func(c rune) bool { return c >= '0' && c <= '9' }
	parseDigits := func(p, num int) (int, int, bool) {
		v := 0
		for i := 0; i < num; i++ {
			c := at(p + i)
			if !isDigit(c) {
				return 0, p, false
			}
			v = v*10 + int(c-'0')
		}
		return v, p + num, true
	}

	// _find_isoformat_datetime_separator
	var sep int
	if n == 7 {
		sep = 7
	} else if at(4) == '-' {
		if at(5) == 'W' {
			if n < 8 {
				return
			}
			if n > 8 && at(8) == '-' {
				if n == 9 {
					return
				}
				if n > 10 && isDigit(at(10)) {
					sep = 8
				} else {
					sep = 10
				}
			} else {
				sep = 8
			}
		} else {
			sep = 10
		}
	} else if at(4) == 'W' {
		idx := 7
		for ; idx < n; idx++ {
			if !isDigit(at(idx)) {
				break
			}
		}
		if idx < 9 {
			sep = idx
		} else if idx%2 == 0 {
			sep = 7
		} else {
			sep = 8
		}
	} else {
		sep = 8
	}

	// parse_isoformat_date
	var p int
	var good bool
	year, p, good = parseDigits(0, 4)
	if !good {
		return
	}
	usesSeparator := at(p) == '-'
	if usesSeparator {
		p++
	}
	if at(p) == 'W' {
		p++
		isoWeek, np, good := parseDigits(p, 2)
		if !good {
			return
		}
		p = np
		isoDay := 1
		if p < sep {
			if usesSeparator {
				if at(p) != '-' {
					return
				}
				p++
			}
			isoDay, _, good = parseDigits(p, 1)
			if !good {
				return
			}
		}
		if !dhISOToYMD(year, isoWeek, isoDay) {
			return
		}
		// iso_to_ymd: Monday of ISO week 1 is the Monday of the week containing Jan 4th.
		jan4 := time.Date(year, time.January, 4, 0, 0, 0, 0, time.UTC)
		wd := (int(jan4.Weekday()) + 6) % 7 // Monday == 0
		d := jan4.AddDate(0, 0, -wd+(isoWeek-1)*7+(isoDay-1))
		year, month, day = d.Year(), int(d.Month()), d.Day()
	} else {
		month, p, good = parseDigits(p, 2)
		if !good {
			return
		}
		if usesSeparator {
			if at(p) != '-' {
				return
			}
			p++
		}
		day, _, good = parseDigits(p, 2)
		if !good {
			return
		}
		if year < 1 || month < 1 || month > 12 || day < 1 || day > dhDaysInMonth(year, month) {
			return
		}
	}

	if n <= sep {
		ok = true
		return
	}

	// parse_isoformat_time
	start := sep + 1
	end := n
	tzPos := start
	for {
		c := at(tzPos)
		if c == 'Z' || c == '+' || c == '-' {
			break
		}
		tzPos++
		if tzPos >= end {
			break
		}
	}

	h, mi, sec, us, rv := dhParseHHMMSSFF(at, start, tzPos)
	if rv < 0 {
		return
	}
	if tzPos == end {
		if rv == 1 {
			return
		}
	} else if at(tzPos) == 'Z' {
		if at(tzPos+1) != 0 {
			return
		}
	} else {
		tzh, tzm, tzs, tzus, rv := dhParseHHMMSSFF(at, tzPos+1, end)
		if rv != 0 {
			return
		}
		// timezone offsets must be strictly between -24h and 24h (timedelta normalizes the components)
		total := ((tzh*3600+tzm*60+tzs)*1000000 + tzus)
		if total >= 24*3600*1000000 {
			return
		}
	}

	if h > 23 || mi > 59 || sec > 59 || us > 999999 {
		return
	}
	hour, minute, second, microsecond = h, mi, sec, us
	ok = true
	return
}

// clickhouseMapSQL mirrors _map_sql.
func clickhouseMapSQL(g *Generator, e *Expr) string {
	if !(e.Parent() != nil && e.Parent().ArgKey() == "settings") {
		return clickhouseLowerFunc(varMapSQL(g, e, "MAP"))
	}

	keys := e.Arg("keys")
	values := e.Arg("values")
	keysE, _ := keys.(*Expr)
	valuesE, _ := values.(*Expr)

	if !keysE.IsA(KArray) || !valuesE.IsA(KArray) {
		g.unsupported("Cannot convert array columns into map.")
		return ""
	}

	var args []string
	ks, vs := keysE.Expressions(), valuesE.Expressions()
	for i := 0; i < len(ks) && i < len(vs); i++ {
		args = append(args, g.sql(ks[i])+": "+g.sql(vs[i]))
	}

	csvArgs := strings.Join(args, ", ")

	return "{" + csvArgs + "}"
}

// clickhouseJSONCastSQL mirrors _json_cast_sql.
func clickhouseJSONCastSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	to := e.ArgE("to")
	toSQL := g.sql(to)

	if len(to.Expressions()) > 0 {
		toSQL = g.sql(ToIdentifier(toSQL, nil))
	}

	return this + ".:" + toSQL
}

// ---------------------------------------------------------------------------------------------
// Generator customization
// ---------------------------------------------------------------------------------------------

func customizeClickHouseGenerator(d *Dialect) {
	s := d.G

	// AFTER_HAVING_MODIFIER_TRANSFORMS = generator.AFTER_HAVING_MODIFIER_TRANSFORMS
	delete(s.AFTER_HAVING_MODIFIER_TRANSFORMS, "cluster")
	delete(s.AFTER_HAVING_MODIFIER_TRANSFORMS, "distribute")
	delete(s.AFTER_HAVING_MODIFIER_TRANSFORMS, "sort")
	var keys []string
	for _, k := range s.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS {
		if k != "cluster" && k != "distribute" && k != "sort" {
			keys = append(keys, k)
		}
	}
	s.AFTER_HAVING_MODIFIER_TRANSFORMS_KEYS = keys

	// TRANSFORMS
	for k, f := range map[Kind]GenFunc{
		KAnyValue:       renameFunc("any"),
		KApproxDistinct: renameFunc("uniq"),
		KArrayDistinct:  renameFunc("arrayDistinct"),
		KArrayConcat:    renameFunc("arrayConcat"),
		KArrayContains:  renameFunc("has"),
		KArrayFilter: func(g *Generator, e *Expr) string {
			return g.fn("arrayFilter", e.Arg("expression"), e.Arg("this"))
		},
		KTransform: func(g *Generator, e *Expr) string {
			return g.fn("arrayMap", e.Arg("expression"), e.Arg("this"))
		},
		KArrayRemove:     removeFromArrayUsingFilter,
		KArrayReverse:    renameFunc("arrayReverse"),
		KArraySlice:      renameFunc("arraySlice"),
		KArraySum:        renameFunc("arraySum"),
		KArrayMax:        renameFunc("arrayMax"),
		KArrayMin:        renameFunc("arrayMin"),
		KArgMax:          argMaxOrMinNoCount("argMax"),
		KArgMin:          argMaxOrMinNoCount("argMin"),
		KArray:           inlineArraySQL,
		KCityHash64:      renameFunc("cityHash64"),
		KCastToStrType:   renameFunc("CAST"),
		KCurrentDatabase: renameFunc("CURRENT_DATABASE"),
		KCurrentSchemas:  renameFunc("CURRENT_SCHEMAS"),
		KCountIf:         renameFunc("countIf"),
		KCosineDistance:  renameFunc("cosineDistance"),
		KCompressColumnConstraint: func(g *Generator, e *Expr) string {
			return "CODEC(" + g.expressions(e, exprsOpts{key: "this", flat: true}) + ")"
		},
		KComputedColumnConstraint: func(g *Generator, e *Expr) string {
			kw := "ALIAS"
			if e.ArgB("persisted") {
				kw = "MATERIALIZED"
			}
			return kw + " " + g.sqlKey(e, "this")
		},
		KCurrentDate:     func(g *Generator, e *Expr) string { return g.fn("CURRENT_DATE") },
		KCurrentVersion:  renameFunc("VERSION"),
		KDateAdd:         clickhouseDatetimeDeltaSQL("DATE_ADD"),
		KDateDiff:        clickhouseDatetimeDeltaSQL("DATE_DIFF"),
		KDateStrToDate:   renameFunc("toDate"),
		KDateSub:         clickhouseDatetimeDeltaSQL("DATE_SUB"),
		KExplode:         renameFunc("arrayJoin"),
		KFarmFingerprint: renameFunc("farmFingerprint64"),
		KFinal: func(g *Generator, e *Expr) string {
			return g.sqlKey(e, "this") + " FINAL"
		},
		KIsNan:                 renameFunc("isNaN"),
		KJarowinklerSimilarity: jarowinklerSimilarity("jaroWinklerSimilarity"),
		KJSONCast:              clickhouseJSONCastSQL,
		KJSONExtract:           jsonExtractSegments("JSONExtractString", false, ""),
		KJSONExtractScalar:     jsonExtractSegments("JSONExtractString", false, ""),
		KJSONPathKey:           jsonPathKeyOnlyName,
		KJSONPathRoot:          func(g *Generator, e *Expr) string { return "" },
		KLength:                lengthOrCharLengthSQL,
		KMap:                   clickhouseMapSQL,
		KMedian:                renameFunc("median"),
		KNullif:                renameFunc("nullIf"),
		KPartitionedByProperty: func(g *Generator, e *Expr) string {
			return "PARTITION BY " + g.sqlKey(e, "this")
		},
		KPivot:    noPivotSQL,
		KQuantile: clickhouseQuantileSQL,
		KRegexpLike: func(g *Generator, e *Expr) string {
			return g.fn("match", e.Arg("this"), e.Arg("expression"))
		},
		KRand:              renameFunc("randCanonical"),
		KStartsWith:        renameFunc("startsWith"),
		KStruct:            renameFunc("tuple"),
		KTrunc:             renameFunc("trunc"),
		KEndsWith:          renameFunc("endsWith"),
		KEuclideanDistance: renameFunc("L2Distance"),
		KStrPosition: func(g *Generator, e *Expr) string {
			return strpositionSQL(g, e, "POSITION", true, false, false)
		},
		KTimeToStr: func(g *Generator, e *Expr) string {
			this := e.This()
			if this.IsA(KTsOrDsToTimestamp) {
				this = this.This()
			}
			var format any
			if f := g.formatTime(e, nil, nil); f != "" {
				format = f
			}
			return g.fn("formatDateTime", this, format, e.Arg("zone"))
		},
		KTimeStrToTime: clickhouseTimestrtotimeSQL,
		KTimestampAdd:  clickhouseDatetimeDeltaSQL("TIMESTAMP_ADD"),
		KTimestampSub:  clickhouseDatetimeDeltaSQL("TIMESTAMP_SUB"),
		KTypeof:        renameFunc("toTypeName"),
		KVarMap:        clickhouseMapSQL,
		KXor: func(g *Generator, e *Expr) string {
			return g.fn("xor", e.Arg("this"), e.Arg("expression"))
		},
		KMD5Digest: renameFunc("MD5"),
		KMD5: func(g *Generator, e *Expr) string {
			return g.fn("LOWER", g.fn("HEX", g.fn("MD5", e.This())))
		},
		KSHA:        renameFunc("SHA1"),
		KSHA1Digest: renameFunc("SHA1"),
		KSHA2:       sha256SQL,
		KSHA2Digest: sha2DigestSQL,
		KSplit: func(g *Generator, e *Expr) string {
			return g.fn("splitByString", e.Arg("expression"), e.Arg("this"), e.Arg("limit"))
		},
		KRegexpSplit: func(g *Generator, e *Expr) string {
			return g.fn("splitByRegexp", e.Arg("expression"), e.Arg("this"), e.Arg("limit"))
		},
		KUnixToTime: clickhouseUnixToTimeSQL,
		KTrim: func(g *Generator, e *Expr) string {
			return trimSQL(g, e, "BOTH")
		},
		KVariance:              renameFunc("varSamp"),
		KSchemaCommentProperty: func(g *Generator, e *Expr) string { return g.nakedProperty(e) },
		KStddev:                renameFunc("stddevSamp"),
		KChr:                   renameFunc("CHAR"),
		KLag: func(g *Generator, e *Expr) string {
			return g.fn("lagInFrame", e.Arg("this"), e.Arg("offset"), e.Arg("default"))
		},
		KLead: func(g *Generator, e *Expr) string {
			return g.fn("leadInFrame", e.Arg("this"), e.Arg("offset"), e.Arg("default"))
		},
		KLevenshtein: func(g *Generator, e *Expr) string {
			dhUnsupportedArgs(g, e, "ins_cost", "del_cost", "sub_cost", "max_dist")
			return renameFunc("editDistance")(g, e)
		},
		KParseDatetime: func(g *Generator, e *Expr) string {
			return g.fn("parseDateTime", e.Arg("this"), e.Arg("format"), e.Arg("zone"))
		},
	} {
		s.TRANSFORMS[k] = f
	}

	// Dialect-only <key>_sql methods
	s.methods[KGroupConcat] = clickhouseGroupconcatSQL
	s.methods[KRegexpILike] = clickhouseRegexpilikeSQL
	s.methods[KPartitionId] = clickhousePartitionidSQL
	s.methods[KReplacePartition] = clickhouseReplacepartitionSQL
	s.methods[KProjectionDef] = clickhouseProjectiondefSQL
	s.methods[KNestedJSONSelect] = clickhouseNestedjsonselectSQL
	s.methods[KTimestampTrunc] = clickhouseTimestamptruncSQL
	s.methods[KDateTrunc] = clickhouseDatetruncSQL

	// Method overrides
	s.h.offsetSQL = clickhouseOffsetSQL
	s.h.strtodateSQL = clickhouseStrtodateSQL
	s.h.castSQL = clickhouseCastSQL
	s.h.trycastSQL = clickhouseTrycastSQL
	s.h.jsonpathsubscriptSQL = clickhouseJsonpathsubscriptSQL
	s.h.likepropertySQL = clickhouseLikepropertySQL
	s.h.eqSQL = clickhouseEqSQL
	s.h.neqSQL = clickhouseNeqSQL
	s.h.datatypeSQL = clickhouseDatatypeSQL
	s.h.cteSQL = clickhouseCteSQL
	s.h.afterLimitModifiers = clickhouseAfterLimitModifiers
	s.h.placeholderSQL = clickhousePlaceholderSQL
	s.h.onclusterSQL = clickhouseOnclusterSQL
	s.h.createableSQL = clickhouseCreateableSQL
	s.h.createSQL = clickhouseCreateSQL
	s.h.prewhereSQL = clickhousePrewhereSQL
	s.h.indexcolumnconstraintSQL = clickhouseIndexcolumnconstraintSQL
	s.h.partitionSQL = clickhousePartitionSQL
	s.h.isSQL = clickhouseIsSQL
	s.h.inSQL = clickhouseInSQL
	s.h.notSQL = clickhouseNotSQL
	s.h.valuesSQL = clickhouseValuesSQL
}

// ---------------------------------------------------------------------------------------------
// Methods
// ---------------------------------------------------------------------------------------------

// clickhouseGroupconcatSQL mirrors groupconcat_sql.
func clickhouseGroupconcatSQL(g *Generator, e *Expr) string {
	this := e.This()
	separator := e.ArgE("separator")

	if this.IsA(KLimit) && this.ArgB("this") {
		limit := this
		this = limit.This().Pop()
		return g.sql(New(
			KParameterizedAgg,
			"this", "groupConcat",
			"params", []*Expr{this},
			"expressions", []*Expr{separator, limit.Expression()},
		))
	}

	if separator != nil {
		return g.sql(New(
			KParameterizedAgg,
			"this", "groupConcat",
			"params", []*Expr{this},
			"expressions", []*Expr{separator},
		))
	}

	return g.fn("groupConcat", this)
}

// clickhouseOffsetSQL mirrors offset_sql.
func clickhouseOffsetSQL(g *Generator, e *Expr) string {
	offset := g.baseOffsetSQL(e)

	// OFFSET ... FETCH syntax requires a "ROW" or "ROWS" keyword
	// https://clickhouse.com/docs/sql-reference/statements/select/offset
	parent := e.Parent()
	if parent.IsA(KSelect) && parent.ArgE("limit").IsA(KFetch) {
		offset = offset + " ROWS"
	}

	return offset
}

// clickhouseStrtodateSQL mirrors strtodate_sql.
func clickhouseStrtodateSQL(g *Generator, e *Expr) string {
	strtodateSQL := g.functionFallbackSQL(e)

	if !e.Parent().IsA(KCast) {
		// StrToDate returns DATEs in other dialects (eg. postgres), so
		// this branch aims to improve the transpilation to clickhouse
		return g.castSQL(CastExpr(e, "DATE", true, nil), "")
	}

	return strtodateSQL
}

// clickhouseCastSQL mirrors cast_sql.
func clickhouseCastSQL(g *Generator, e *Expr, safePrefix string) string {
	this := e.This()

	if this.IsA(KStrToDate) && e.ArgE("to").Equal(NewDataType(DT_DATETIME)) {
		return g.sql(this)
	}

	return g.baseCastSQL(e, safePrefix)
}

// clickhouseNonNullableTypes returns NON_NULLABLE_TYPES as is_type arguments.
func clickhouseNonNullableTypes(g *Generator) []any {
	items := g.s.NON_NULLABLE_TYPES.Items()
	out := make([]any, len(items))
	for i, t := range items {
		out[i] = t
	}
	return out
}

// clickhouseTrycastSQL mirrors trycast_sql.
func clickhouseTrycastSQL(g *Generator, e *Expr) string {
	dtype := e.ArgE("to")
	if !DataTypeIsType(dtype, clickhouseNonNullableTypes(g), true) {
		// Casting x into Nullable(T) appears to behave similarly to TRY_CAST(x AS T)
		dtype.Set("nullable", true)
	}

	return g.baseCastSQL(e, "")
}

// clickhouseJsonpathsubscriptSQL mirrors _jsonpathsubscript_sql.
func clickhouseJsonpathsubscriptSQL(g *Generator, e *Expr) string {
	this := g.jsonPathPart(e.Arg("this"))
	if isPyInt(this) {
		n, ok := new(big.Int).SetString(strings.ReplaceAll(pyStrip(this), "_", ""), 10)
		if ok {
			return n.Add(n, big.NewInt(1)).String()
		}
	}
	return this
}

// clickhouseLikepropertySQL mirrors likeproperty_sql.
func clickhouseLikepropertySQL(g *Generator, e *Expr) string {
	return "AS " + g.sqlKey(e, "this")
}

// clickhouseAnyToHas mirrors _any_to_has.
func clickhouseAnyToHas(g *Generator, e *Expr, def func(g *Generator, e *Expr) string, prefix string) string {
	var arr, this *Expr
	if e.This().IsA(KAny) {
		arr = e.This()
		this = e.Expression()
	} else if e.Expression().IsA(KAny) {
		arr = e.Expression()
		this = e.This()
	} else {
		return def(g, e)
	}

	return prefix + g.fn("has", arr.This().Unnest(), this)
}

// clickhouseEqSQL mirrors eq_sql.
func clickhouseEqSQL(g *Generator, e *Expr) string {
	return clickhouseAnyToHas(g, e, (*Generator).baseEqSQL, "")
}

// clickhouseNeqSQL mirrors neq_sql.
func clickhouseNeqSQL(g *Generator, e *Expr) string {
	return clickhouseAnyToHas(g, e, (*Generator).baseNeqSQL, "NOT ")
}

// clickhouseRegexpilikeSQL mirrors regexpilike_sql.
func clickhouseRegexpilikeSQL(g *Generator, e *Expr) string {
	// Manually add a flag to make the search case-insensitive
	regex := g.fn("CONCAT", "'(?i)'", e.Arg("expression"))
	return g.fn("match", e.Arg("this"), regex)
}

// clickhouseDatatypeSQL mirrors datatype_sql.
func clickhouseDatatypeSQL(g *Generator, e *Expr) string {
	// String is the standard ClickHouse type, every other variant is just an alias.
	// Additionally, any supplied length parameter will be ignored.
	//
	// https://clickhouse.com/docs/en/sql-reference/data-types/string
	var dtype string
	isString := false
	if t, ok := e.Arg("this").(DType); ok {
		_, isString = g.s.STRING_TYPE_MAPPING[t]
	}
	if isString {
		dtype = "String"
	} else {
		dtype = g.baseDatatypeSQL(e)
	}

	// This section changes the type to `Nullable(...)` if the following conditions hold:
	// - It's marked as nullable - this ensures we won't wrap ClickHouse types with `Nullable`
	//   and change their semantics
	// - It's not the key type of a `Map`. This is because ClickHouse enforces the following
	//   constraint: "Type of Map key must be a type, that can be represented by integer or
	//   String or FixedString (possibly LowCardinality) or UUID or IPv6"
	// - It's not a composite type, e.g. `Nullable(Array(...))` is not a valid type
	parent := e.Parent()
	nullable := e.Arg("nullable")
	nullableTrue := false
	if b, ok := nullable.(bool); ok && b {
		nullableTrue = true
	}
	if nullableTrue || (nullable == nil &&
		!(parent.IsA(KDataType) &&
			DataTypeIsType(parent, []any{DT_MAP}, true) &&
			(e.Index() < 0 || e.Index() == 0)) &&
		!DataTypeIsType(e, clickhouseNonNullableTypes(g), true)) {
		dtype = "Nullable(" + dtype + ")"
	}

	return dtype
}

// clickhouseCteSQL mirrors cte_sql.
func clickhouseCteSQL(g *Generator, e *Expr) string {
	if e.ArgB("scalar") {
		this := g.sqlKey(e, "this")
		alias := g.sqlKey(e, "alias")
		return this + " AS " + alias
	}

	return g.baseCteSQL(e)
}

// clickhouseAfterLimitModifiers mirrors after_limit_modifiers.
func clickhouseAfterLimitModifiers(g *Generator, e *Expr) []string {
	settings := ""
	if e.ArgB("settings") {
		settings = g.seg("SETTINGS ") + g.expressions(e, exprsOpts{key: "settings", flat: true})
	}
	format := ""
	if e.ArgB("format") {
		format = g.seg("FORMAT ") + g.sqlKey(e, "format")
	}
	return append(g.baseAfterLimitModifiers(e), settings, format)
}

// clickhousePlaceholderSQL mirrors placeholder_sql.
func clickhousePlaceholderSQL(g *Generator, e *Expr) string {
	return "{" + e.Name() + ": " + g.sqlKey(e, "kind") + "}"
}

// clickhouseOnclusterSQL mirrors oncluster_sql.
func clickhouseOnclusterSQL(g *Generator, e *Expr) string {
	return "ON CLUSTER " + g.sqlKey(e, "this")
}

// clickhouseCreateableSQL mirrors createable_sql.
func clickhouseCreateableSQL(g *Generator, e *Expr, locations propLocations) string {
	kind := pyUpper(e.ArgS("kind"))
	if g.s.ON_CLUSTER_TARGETS.Has(kind) && len(locations[Loc_POST_NAME]) > 0 {
		target := e
		if e.This().IsA(KSchema) {
			target = e.This()
		}
		thisName := g.sqlKey(target, "this")
		props := make([]string, 0, len(locations[Loc_POST_NAME]))
		for _, prop := range locations[Loc_POST_NAME] {
			props = append(props, g.sql(prop))
		}
		thisProperties := strings.Join(props, " ")
		thisSchema := g.schemaColumnsSQL(e.This())
		if thisSchema != "" {
			thisSchema = g.sep(" ") + thisSchema
		}

		return thisName + g.sep(" ") + thisProperties + thisSchema
	}

	return g.baseCreateableSQL(e, locations)
}

// clickhouseCreateSQL mirrors create_sql.
func clickhouseCreateSQL(g *Generator, e *Expr) string {
	// The comment property comes last in CTAS statements, i.e. after the query
	query := e.Expression()
	var commentProp *Expr
	if query.IsA(KQuery) {
		commentProp = e.Find(KSchemaCommentProperty)
		if commentProp != nil {
			commentProp.Pop()
			query.Replace(ParenExpr(query, true))
		}
	}

	createSQL := g.baseCreateSQL(e)

	commentSQL := g.sql(commentProp)
	if commentSQL != "" {
		commentSQL = " " + commentSQL
	}

	return createSQL + commentSQL
}

// clickhousePrewhereSQL mirrors prewhere_sql.
func clickhousePrewhereSQL(g *Generator, e *Expr) string {
	this := g.indentDefault(g.sqlKey(e, "this"))
	return g.seg("PREWHERE") + g.sep(" ") + this
}

// clickhouseIndexcolumnconstraintSQL mirrors indexcolumnconstraint_sql.
func clickhouseIndexcolumnconstraintSQL(g *Generator, e *Expr) string {
	this := g.sqlKey(e, "this")
	if this != "" {
		this = " " + this
	}
	expr := g.sqlKey(e, "expression")
	if expr != "" {
		expr = " " + expr
	}
	indexType := g.sqlKey(e, "index_type")
	if indexType != "" {
		indexType = " TYPE " + indexType
	}
	granularity := g.sqlKey(e, "granularity")
	if granularity != "" {
		granularity = " GRANULARITY " + granularity
	}

	return "INDEX" + this + expr + indexType + granularity
}

// clickhousePartitionSQL mirrors partition_sql.
func clickhousePartitionSQL(g *Generator, e *Expr) string {
	return "PARTITION " + g.expressions(e, exprsOpts{flat: true})
}

// clickhousePartitionidSQL mirrors partitionid_sql.
func clickhousePartitionidSQL(g *Generator, e *Expr) string {
	return "ID " + g.sql(e.Arg("this"))
}

// clickhouseReplacepartitionSQL mirrors replacepartition_sql.
func clickhouseReplacepartitionSQL(g *Generator, e *Expr) string {
	return "REPLACE " + g.sql(e.Arg("expression")) + " FROM " + g.sqlKey(e, "source")
}

// clickhouseProjectiondefSQL mirrors projectiondef_sql.
func clickhouseProjectiondefSQL(g *Generator, e *Expr) string {
	return "PROJECTION " + g.sql(e.Arg("this")) + " " + g.wrap(e.Expression())
}

// clickhouseNestedjsonselectSQL mirrors nestedjsonselect_sql.
func clickhouseNestedjsonselectSQL(g *Generator, e *Expr) string {
	return g.sqlKey(e, "this") + ".^" + g.sqlKey(e, "expression")
}

// clickhouseIsSQL mirrors is_sql.
func clickhouseIsSQL(g *Generator, e *Expr) string {
	isSQL := g.baseIsSQL(e)

	if e.Parent().IsA(KNot) {
		// value IS NOT NULL -> NOT (value IS NULL)
		isSQL = g.wrap(isSQL)
	}

	return isSQL
}

// clickhouseInSQL mirrors in_sql.
func clickhouseInSQL(g *Generator, e *Expr) string {
	inSQL := g.baseInSQL(e)

	if e.Parent().IsA(KNot) && e.ArgB("is_global") {
		inSQL = strings.Replace(inSQL, "GLOBAL IN", "GLOBAL NOT IN", 1)
	}

	return inSQL
}

// clickhouseNotSQL mirrors not_sql.
func clickhouseNotSQL(g *Generator, e *Expr) string {
	if e.This().IsA(KIn) {
		if e.This().ArgB("is_global") {
			// let `GLOBAL IN` child interpose `NOT`
			return g.sqlKey(e, "this")
		}

		e.Set("this", ParenExpr(e.This(), false))
	}

	return g.baseNotSQL(e)
}

// clickhouseValuesSQL mirrors values_sql.
func clickhouseValuesSQL(g *Generator, e *Expr, valuesAsTable bool) string {
	// If the VALUES clause contains tuples of expressions, we need to treat it
	// as a table since Clickhouse will automatically alias it as such.
	alias := e.ArgE("alias")

	if alias != nil && alias.ArgB("columns") && len(e.Expressions()) > 0 {
		values := e.Expressions()[0].Expressions()
		valuesAsTable = false
		for _, value := range values {
			if value.IsA(KTuple) {
				valuesAsTable = true
				break
			}
		}
	} else {
		valuesAsTable = true
	}

	return g.baseValuesSQL(e, valuesAsTable)
}

// clickhouseTimestamptruncSQL mirrors timestamptrunc_sql.
func clickhouseTimestamptruncSQL(g *Generator, e *Expr) string {
	unit := unitToStr(e, "DAY")
	// https://clickhouse.com/docs/whats-new/changelog/2023#improvement
	v := g.d.Version
	if (v[0] < 23 || (v[0] == 23 && v[1] < 12)) && unit != nil && unit.IsString() {
		unit = LiteralString(pyLower(unit.Name()))
	}
	return g.fn("dateTrunc", unit, e.Arg("this"), e.Arg("zone"))
}

// clickhouseDatetruncSQL mirrors datetrunc_sql.
func clickhouseDatetruncSQL(g *Generator, e *Expr) string {
	return clickhouseTimestamptruncSQL(g, e)
}
