package sqlengine

import "time"

// Canonicalize converts a SQL expression into a standard form. It relies on type annotation
// because many of the conversions rely on type inference. d == nil means the default dialect.
func Canonicalize(expression *Expr, d *Dialect) *Expr {
	dialect := d
	if dialect == nil {
		dialect = MustDialect("")
	}

	canonicalize := func(expression *Expr) *Expr {
		if !expression.IsA(canonicalizeTypes...) {
			return expression
		}
		expression = canonicalizeAddTextToConcat(expression)
		expression = canonicalizeReplaceDateFuncs(expression, dialect)
		expression = canonicalizeCoerceType(expression, dialect.S.PROMOTE_TO_INFERRED_DATETIME_TYPE)
		expression = canonicalizeRemoveRedundantCasts(expression)
		expression = canonicalizeEnsureBools(expression, canonicalizeReplaceIntPredicate)
		expression = canonicalizeRemoveAscendingOrder(expression)
		return expression
	}

	return ReplaceTree(expression, canonicalize, nil)
}

var canonicalizeCoercibleDateOps = []Kind{
	KAdd,
	KSub,
	KEQ,
	KNEQ,
	KGT,
	KGTE,
	KLT,
	KLTE,
	KNullSafeEQ,
	KNullSafeNEQ,
}

// canonicalizeTypes lists the kinds that any canonicalize function can act on.
var canonicalizeTypes = append(
	append([]Kind{
		// canonicalizeAddTextToConcat
		KAdd,
		// canonicalizeReplaceDateFuncs
		KDate,
		KTsOrDsToDate,
		KTimestamp,
	}, canonicalizeCoercibleDateOps...),
	// canonicalizeCoerceType (the coercible date ops above, plus these)
	KBetween,
	KExtract,
	KDateAdd,
	KDateSub,
	KDateTrunc,
	KDateDiff,
	// canonicalizeRemoveRedundantCasts
	KCast,
	// canonicalizeEnsureBools
	KConnector,
	KNot,
	KIf,
	KWhere,
	KHaving,
	// canonicalizeRemoveAscendingOrder
	KOrdered,
)

// canonicalizeAddTextToConcat rewrites a text-typed a + b as CONCAT(a, b) without NULL
// coalescing.
func canonicalizeAddTextToConcat(node *Expr) *Expr {
	if node.IsA(KAdd) && node.Type() != nil && DataType_TEXT_TYPES.Has(node.Type().DTypeOf()) {
		node = New(
			KConcat,
			"expressions", []*Expr{node.Left(), node.Right()},
			// All known dialects, i.e. Redshift and T-SQL, that support
			// concatenating strings with the + operator do not coalesce NULLs.
			"coalesce", false,
		)
	}
	return node
}

// canonicalizeReplaceDateFuncs rewrites DATE('iso date') and TS_OR_DS_TO_DATE('iso date') with no
// other arguments as CAST(... AS DATE), and a zoneless TIMESTAMP(x) as a cast to its inferred
// type.
func canonicalizeReplaceDateFuncs(node *Expr, d *Dialect) *Expr {
	if node.IsA(KDate, KTsOrDsToDate) &&
		len(node.Expressions()) == 0 &&
		!node.ArgB("zone") &&
		node.This().IsString() &&
		optxIsISODate(node.This().Name()) {
		return CastExpr(node.This(), DT_DATE, true, nil)
	}
	if node.IsA(KTimestamp) && !node.ArgB("zone") {
		if node.Type() == nil {
			node = AnnotateTypes(node, AnnotateOptions{Dialect: d})
		}
		var to any = DT_TIMESTAMP
		if t := node.Type(); t != nil {
			to = t
		}
		return CastExpr(node.This(), to, true, nil)
	}

	return node
}

// canonicalizeCoerceType casts the operands of date comparisons and arithmetic, BETWEEN, EXTRACT,
// unit-based date functions and DATEDIFF to compatible temporal types.
func canonicalizeCoerceType(node *Expr, promoteToInferredDatetimeType bool) *Expr {
	switch {
	case node.IsA(canonicalizeCoercibleDateOps...):
		canonicalizeCoerceDate(node.Left(), node.Right(), promoteToInferredDatetimeType)
	case node.IsA(KBetween):
		canonicalizeCoerceDate(node.This(), node.ArgE("low"), promoteToInferredDatetimeType)
	case node.IsA(KExtract) && !optxIsType(node.argAttr("expression", "is_type"), optxDTypes(DataType_TEMPORAL_TYPES)...):
		canonicalizeReplaceCast(node.Expression(), DT_DATETIME)
	case node.IsA(KDateAdd, KDateSub, KDateTrunc):
		canonicalizeCoerceTimeunitArg(node.This(), node.ArgE("unit"))
	case node.IsA(KDateDiff):
		canonicalizeCoerceDatediffArgs(node)
	}

	return node
}

// canonicalizeRemoveRedundantCasts removes casts to the operand's own type, and DATE() or
// TS_OR_DS_TO_DATE() of a DATE value.
func canonicalizeRemoveRedundantCasts(expression *Expr) *Expr {
	if expression.IsA(KCast) &&
		expression.This().Type() != nil &&
		expression.ArgE("to").Equal(expression.This().Type()) {
		return expression.This()
	}

	if expression.IsA(KDate, KTsOrDsToDate) &&
		expression.This().Type() != nil &&
		expression.This().Type().Arg("this") == DT_DATE &&
		len(expression.This().Type().Expressions()) == 0 {
		return expression.This()
	}

	return expression
}

// canonicalizeEnsureBools applies replaceFunc to operands that must be booleans: those of AND/OR
// and NOT, IF and searched CASE WHEN conditions, WHERE and HAVING.
func canonicalizeEnsureBools(expression *Expr, replaceFunc func(*Expr)) *Expr {
	switch {
	case expression.IsA(KConnector):
		replaceFunc(expression.Left())
		replaceFunc(expression.Right())
	case expression.IsA(KNot):
		replaceFunc(expression.This())
	// We can't replace num in CASE x WHEN num ..., because it's not the full predicate
	case expression.IsA(KIf) && !(expression.Parent().IsA(KCase) && expression.Parent().This() != nil):
		replaceFunc(expression.This())
	case expression.IsA(KWhere, KHaving):
		replaceFunc(expression.This())
	}

	return expression
}

func canonicalizeRemoveAscendingOrder(expression *Expr) *Expr {
	if expression.IsA(KOrdered) {
		if desc, ok := expression.Arg("desc").(bool); ok && !desc {
			// Convert ORDER BY a ASC to ORDER BY a
			expression.Set("desc", nil)
		}
	}

	return expression
}

// canonicalizeCoerceDate casts a text operand used with a temporal operand (in either order) to
// the temporal type, or to the type inferred from the text when promoteToInferredDatetimeType
// allows it. An interval operand first coerces the other operand by its unit.
func canonicalizeCoerceDate(a, b *Expr, promoteToInferredDatetimeType bool) {
	// Both operand orders are taken from the original nodes, before any replacement.
	pairs := [][2]*Expr{{a, b}, {b, a}}
	for _, pair := range pairs {
		a, b := pair[0], pair[1]
		if b.IsA(KInterval) {
			a = canonicalizeCoerceTimeunitArg(a, b.ArgE("unit"))
		}

		aType := a.Type()
		if aType == nil ||
			!DataType_TEMPORAL_TYPES.Has(aType.DTypeOf()) ||
			b.Type() == nil ||
			!DataType_TEXT_TYPES.Has(b.Type().DTypeOf()) {
			continue
		}

		// targetType is either aType (a DataType expression) or a promoted DType; only a promoted
		// type also casts a.
		var targetType any = aType
		targetIsAType := true
		if promoteToInferredDatetimeType {
			var bType DType
			if b.IsString() {
				dateText := b.Name()
				if optxIsISODate(dateText) {
					bType = DT_DATE
				} else if optxIsISODatetime(dateText) {
					bType = DT_DATETIME
				} else {
					bType = aType.DTypeOf()
				}
			} else {
				// If b is not a datetime string, we conservatively promote it to a DATETIME,
				// in order to ensure there are no surprising truncations due to downcasting
				bType = DT_DATETIME
			}

			if canonicalizeCoercesTo(aType.DTypeOf(), bType) {
				targetType = bType
				targetIsAType = false
			}
		}

		if !targetIsAType {
			canonicalizeReplaceCast(a, targetType)
		}

		canonicalizeReplaceCast(b, targetType)
	}
}

// canonicalizeCoercesTo reports whether aType coerces to bType in the base coercion table.
func canonicalizeCoercesTo(aType, bType DType) bool {
	targets, ok := typeAnnotatorCoercesTo()[aType]
	if !ok {
		return false
	}
	return targets.Has(bType)
}

// canonicalizeCoerceTimeunitArg casts a text argument of a unit-based date function to DATE (an
// ISO date with a date unit) or DATETIME (an ISO datetime), and a DATE argument with a non-date
// unit to DATETIME. It returns the argument or its replacement.
func canonicalizeCoerceTimeunitArg(arg *Expr, unit *Expr) *Expr {
	if arg.Type() == nil {
		return arg
	}

	if DataType_TEXT_TYPES.Has(arg.Type().DTypeOf()) {
		dateText := arg.Name()
		isISODate := optxIsISODate(dateText)

		if isISODate && optxIsDateUnit(unit) {
			return arg.Replace(CastExpr(arg.Copy(), DT_DATE, true, nil))
		}

		// An ISO date is also an ISO datetime, but not vice versa
		if isISODate || optxIsISODatetime(dateText) {
			return arg.Replace(CastExpr(arg.Copy(), DT_DATETIME, true, nil))
		}
	} else if arg.Type().Arg("this") == DT_DATE && !optxIsDateUnit(unit) {
		return arg.Replace(CastExpr(arg.Copy(), DT_DATETIME, true, nil))
	}

	return arg
}

func canonicalizeCoerceDatediffArgs(node *Expr) {
	for _, e := range []*Expr{node.This(), node.Expression()} {
		if e.Type() == nil || !DataType_TEMPORAL_TYPES.Has(e.Type().DTypeOf()) {
			e.Replace(CastExpr(e.Copy(), DT_DATETIME, true, nil))
		}
	}
}

// canonicalizeReplaceCast replaces node with a cast of it to the type to, a DType or a DataType.
func canonicalizeReplaceCast(node *Expr, to any) {
	node.Replace(CastExpr(node.Copy(), to, true, nil))
}

// canonicalizeReplaceIntPredicate rewrites an integer-typed predicate x as x <> 0, recursing into
// COALESCE.
//
// This was originally designed for Presto. There is a similar transform for T-SQL; this one only
// operates on int types because Presto has a boolean type whereas T-SQL doesn't (people use bits):
// with y as (select true as x) select x = 0 FROM y -- illegal presto query.
func canonicalizeReplaceIntPredicate(expression *Expr) {
	if expression.IsA(KCoalesce) {
		for _, child := range expression.IterExpressions(false) {
			canonicalizeReplaceIntPredicate(child)
		}
	} else if expression.Type() != nil && DataType_INTEGER_TYPES.Has(expression.Type().DTypeOf()) {
		expression.Replace(optxNeq(expression, LiteralInt(0)))
	}
}

func optxNeq(e *Expr, other *Expr) *Expr {
	this := e.Copy()
	other = other.Copy()
	if !this.IsA(KNEQ) && !other.IsA(KNEQ) {
		this = wrapIfKind(this, KBinary)
		other = wrapIfKind(other, KBinary)
	}
	return New(KNEQ, "this", this, "expression", other)
}

func optxIsType(e *Expr, dtypes ...any) bool {
	if e.IsA(KDataType) {
		return DataTypeIsType(e, dtypes, false)
	}
	if e.IsA(KCast) {
		return optxIsType(e.ArgE("to"), dtypes...)
	}
	t := e.RawType()
	return t != nil && DataTypeIsType(t, dtypes, false)
}

func optxDTypes(s DTypeSet) []any {
	items := s.Items()
	out := make([]any, len(items))
	for i, t := range items {
		out[i] = t
	}
	return out
}

var optxDateUnits = newStrSet("day", "week", "month", "quarter", "year", "year_month")

func optxIsDateUnit(expression *Expr) bool {
	return expression != nil && optxDateUnits.Has(pyLower(expression.Name()))
}

// optxIsISODate reports whether text is a valid ISO 8601 date of 7, 8 or 10 bytes: YYYY-MM-DD,
// YYYYMMDD or a week date (YYYY-Www[-D], YYYYWww[D]).
func optxIsISODate(text string) bool {
	n := len(text)
	if n != 7 && n != 8 && n != 10 {
		return false
	}
	year, month, day, ok := optxParseISOFormatDate(text, n)
	return ok && optxValidDate(year, month, day)
}

// optxIsISODatetime reports whether text is a valid ISO 8601 date, optionally followed by a
// one-character separator and a time with an optional fraction and UTC offset.
func optxIsISODatetime(text string) bool {
	n := len(text)
	if len([]rune(text)) < 7 {
		return false
	}
	separatorLocation, ok := optxFindISOFormatDatetimeSeparator(text, n)
	if !ok {
		return false
	}

	year, month, day, ok := optxParseISOFormatDate(text, separatorLocation)
	if !ok {
		return false
	}
	hour, minute, second, microsecond := 0, 0, 0, 0
	if n > separatorLocation {
		// We need to skip the separator character, which is either one or
		// multiple bytes long in UTF-8 (the length is encoded in the MSB).
		p := separatorLocation
		sepLen := 1
		if c := optxCharAt(text, p); c&0x80 != 0 {
			switch c & 0xf0 {
			case 0xe0:
				sepLen = 3
			case 0xf0:
				sepLen = 4
			default:
				sepLen = 2
			}
		}
		p = min(p+sepLen, n)

		var tzOffset, tzMicrosecond int
		var rv int
		hour, minute, second, microsecond, tzOffset, tzMicrosecond, rv = optxParseISOFormatTime(text[p:])
		if rv < 0 {
			return false
		}
		if rv == 1 {
			// The tzinfo offset must be strictly between -24h and 24h.
			total := int64(tzOffset)*1_000_000 + int64(tzMicrosecond)
			if total <= -86_400_000_000 || total >= 86_400_000_000 {
				return false
			}
		}
	}

	return optxValidDate(year, month, day) &&
		hour >= 0 && hour <= 23 &&
		minute >= 0 && minute <= 59 &&
		second >= 0 && second <= 59 &&
		microsecond >= 0 && microsecond <= 999999
}

// optxCharAt returns s[i], or 0 (NUL) outside s, so the parsers can read past the end as if s
// were NUL-terminated.
func optxCharAt(s string, i int) byte {
	if i >= 0 && i < len(s) {
		return s[i]
	}
	return 0
}

func optxIsDigit(c byte) bool { return c >= '0' && c <= '9' }

// optxParseDigits parses numDigits decimal digits at p, returning the value, the new position and
// success.
func optxParseDigits(s string, p int, numDigits int) (int, int, bool) {
	v := 0
	for range numDigits {
		c := optxCharAt(s, p)
		p++
		if !optxIsDigit(c) {
			return 0, p, false
		}
		v = v*10 + int(c-'0')
	}
	return v, p, true
}

// optxFindISOFormatDatetimeSeparator returns the byte position of the separator between date and
// time, inferred from the date format; false for a malformed week date.
func optxFindISOFormatDatetimeSeparator(dtstr string, n int) (int, bool) {
	const dateSeparator = '-'
	const weekIndicator = 'W'

	if n == 7 {
		return 7, true
	}

	if optxCharAt(dtstr, 4) == dateSeparator {
		// YYYY-???
		if optxCharAt(dtstr, 5) == weekIndicator {
			// YYYY-W??
			if n < 8 {
				return -1, false
			}

			if n > 8 && optxCharAt(dtstr, 8) == dateSeparator {
				// YYYY-Www-D (10) or YYYY-Www-HH (8)
				if n == 9 {
					return -1, false
				}
				if n > 10 && optxIsDigit(optxCharAt(dtstr, 10)) {
					// This is as far as we'll try to go to resolve the
					// ambiguity for the moment — if we have YYYY-Www-##, the
					// separator is either a hyphen at 8 or a number at 10.
					return 8, true
				}
				return 10, true
			}
			// YYYY-Www (8)
			return 8, true
		}
		// YYYY-MM-DD (10)
		return 10, true
	}

	// YYYY???
	if optxCharAt(dtstr, 4) == weekIndicator {
		// YYYYWww (7) or YYYYWwwd (8)
		idx := 7
		for ; idx < n; idx++ {
			if !optxIsDigit(dtstr[idx]) {
				break
			}
		}

		if idx < 9 {
			return idx, true
		}

		if idx%2 == 0 {
			// If the index of the last number is even, it's YYYYWww
			return 7, true
		}
		return 8, true
	}
	// YYYYMMDD (8)
	return 8, true
}

// optxParseISOFormatDate parses a calendar or week date at the start of dtstr. n is the date
// length; digits may be read past it.
func optxParseISOFormatDate(dtstr string, n int) (year, month, day int, ok bool) {
	p := 0
	year, p, ok = optxParseDigits(dtstr, p, 4)
	if !ok {
		return 0, 0, 0, false
	}

	usesSeparator := optxCharAt(dtstr, p) == '-'
	if usesSeparator {
		p++
	}

	if optxCharAt(dtstr, p) == 'W' {
		// This is an isocalendar-style date string
		p++
		isoWeek, isoDay := 0, 0

		isoWeek, p, ok = optxParseDigits(dtstr, p, 2)
		if !ok {
			return 0, 0, 0, false
		}

		if p < n {
			if usesSeparator {
				c := optxCharAt(dtstr, p)
				p++
				if c != '-' {
					return 0, 0, 0, false
				}
			}

			isoDay, _, ok = optxParseDigits(dtstr, p, 1)
			if !ok {
				return 0, 0, 0, false
			}
		} else {
			isoDay = 1
		}

		return optxISOToYMD(year, isoWeek, isoDay)
	}

	month, p, ok = optxParseDigits(dtstr, p, 2)
	if !ok {
		return 0, 0, 0, false
	}

	if usesSeparator {
		c := optxCharAt(dtstr, p)
		p++
		if c != '-' {
			return 0, 0, 0, false
		}
	}
	day, _, ok = optxParseDigits(dtstr, p, 2)
	if !ok {
		return 0, 0, 0, false
	}
	return year, month, day, true
}

// optxParseHHMMSSFF parses HH[:MM[:SS[.ffffff]]] (the colons may be omitted together). tstr is
// the whole remaining string, which may be read to its end, and end the logical end. rv is 0 at
// the end of the string, 1 if more text follows and negative on error.
func optxParseHHMMSSFF(tstr string, end int) (hour, minute, second, microsecond, rv int) {
	vals := [3]int{}
	p := 0
	hasSeparator := true

	// Parse [HH[:?MM[:?SS]]]
	for i := range 3 {
		var ok bool
		vals[i], p, ok = optxParseDigits(tstr, p, 2)
		if !ok {
			return vals[0], vals[1], vals[2], 0, -3
		}

		c := optxCharAt(tstr, p)
		p++
		if i == 0 {
			hasSeparator = c == ':'
		}

		if p >= end {
			if c != 0 {
				return vals[0], vals[1], vals[2], 0, 1
			}
			return vals[0], vals[1], vals[2], 0, 0
		} else if hasSeparator && c == ':' {
			continue
		} else if c == '.' || c == ',' {
			break
		} else if !hasSeparator {
			p--
		} else {
			return vals[0], vals[1], vals[2], 0, -4 // Malformed time separator
		}
	}

	lenRemains := end - p
	toParse := lenRemains
	if lenRemains >= 6 {
		toParse = 6
	}

	var ok bool
	microsecond, p, ok = optxParseDigits(tstr, p, toParse)
	if !ok {
		return vals[0], vals[1], vals[2], microsecond, -3
	}

	correction := []int{100000, 10000, 1000, 100, 10}
	if toParse < 6 && toParse > 0 {
		microsecond *= correction[toParse-1]
	}

	for optxIsDigit(optxCharAt(tstr, p)) {
		p++ // skip truncated digits
	}

	if optxCharAt(tstr, p) != 0 {
		return vals[0], vals[1], vals[2], microsecond, 1
	}
	return vals[0], vals[1], vals[2], microsecond, 0
}

// optxParseISOFormatTime parses a time with an optional Z or +/-HH[:MM[:SS[.ffffff]]] UTC offset.
// Return codes: 0 success (no tzoffset), 1 success (with tzoffset), negative on failure.
func optxParseISOFormatTime(dtstr string) (hour, minute, second, microsecond, tzOffset, tzMicrosecond, rv int) {
	pEnd := len(dtstr)

	tzinfoPos := 0
	for {
		c := optxCharAt(dtstr, tzinfoPos)
		if c == 'Z' || c == '+' || c == '-' {
			break
		}
		tzinfoPos++
		if tzinfoPos >= pEnd {
			break
		}
	}

	hour, minute, second, microsecond, rv = optxParseHHMMSSFF(dtstr, tzinfoPos)

	if rv < 0 {
		return
	} else if tzinfoPos == pEnd {
		// We know that there's no time zone, so if there's stuff at the
		// end of the string it's an error.
		if rv == 1 {
			rv = -5
		} else {
			rv = 0
		}
		return
	}

	if optxCharAt(dtstr, tzinfoPos) == 'Z' {
		if optxCharAt(dtstr, tzinfoPos+1) != 0 {
			rv = -5
		} else {
			rv = 1
		}
		return
	}

	tzSign := 1
	if optxCharAt(dtstr, tzinfoPos) == '-' {
		tzSign = -1
	}
	tzinfoPos++
	tzHour, tzMinute, tzSecond, tzUsec, tzRv := optxParseHHMMSSFF(dtstr[min(tzinfoPos, len(dtstr)):], pEnd-tzinfoPos)

	tzOffset = tzSign * ((tzHour * 3600) + (tzMinute * 60) + tzSecond)
	tzMicrosecond = tzUsec * tzSign

	if tzRv != 0 {
		rv = -5
	} else {
		rv = 1
	}
	return
}

// optxISOToYMD converts an ISO year, week and weekday into a calendar date; false when one of
// them is out of range.
func optxISOToYMD(isoYear, isoWeek, isoDay int) (int, int, int, bool) {
	// Year is bounded to 0 < year < 10000 because 9999-12-31 is (9999, 52, 5)
	if isoYear < 1 || isoYear > 9999 {
		return 0, 0, 0, false
	}
	if isoWeek <= 0 || isoWeek >= 53 {
		outOfRange := true
		if isoWeek == 53 {
			// ISO years have 53 weeks in it on years starting with a Thursday
			// and on leap years starting on Wednesday
			firstWeekday := optxYMDToOrd(isoYear, 1, 1) % 7
			if firstWeekday == 4 || (firstWeekday == 3 && optxIsLeap(isoYear)) {
				outOfRange = false
			}
		}

		if outOfRange {
			return 0, 0, 0, false
		}
	}

	if isoDay <= 0 || isoDay >= 8 {
		return 0, 0, 0, false
	}

	day1 := optxISOWeek1Monday(isoYear)
	dayOffset := (isoWeek-1)*7 + isoDay - 1

	y, m, d := optxOrdToYMD(day1 + dayOffset)
	return y, m, d, true
}

func optxIsLeap(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func optxYMDToOrd(year, month, day int) int {
	y := year - 1
	daysBeforeYear := y*365 + y/4 - y/100 + y/400
	daysBeforeMonth := []int{0, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}[month]
	if month > 2 && optxIsLeap(year) {
		daysBeforeMonth++
	}
	return daysBeforeYear + daysBeforeMonth + day
}

func optxISOWeek1Monday(year int) int {
	firstDay := optxYMDToOrd(year, 1, 1) // ord of 1/1
	// 0 if 1/1 is a Monday, 1 if a Tue, etc.
	firstWeekday := (firstDay + 6) % 7
	// ordinal of closest Monday at or before 1/1
	week1Monday := firstDay - firstWeekday

	if firstWeekday > 3 { // if 1/1 was Fri, Sat, Sun
		week1Monday += 7
	}
	return week1Monday
}

func optxOrdToYMD(ordinal int) (int, int, int) {
	t := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, ordinal-1)
	return t.Year(), int(t.Month()), t.Day()
}

// optxValidDate reports whether year-month-day is a valid date in years 1 through 9999.
func optxValidDate(year, month, day int) bool {
	if year < 1 || year > 9999 || month < 1 || month > 12 {
		return false
	}
	dim := []int{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}[month]
	if month == 2 && optxIsLeap(year) {
		dim = 29
	}
	return day >= 1 && day <= dim
}
