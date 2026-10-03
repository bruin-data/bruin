package sqlengine

// Python date/datetime/timedelta and dateutil.relativedelta semantics needed by the port of
// sqlglot/optimizer/simplify.py (CPython 3.13 `_datetime`, python-dateutil 2.9).

import (
	"fmt"
	"math"
	"math/big"
	"unicode/utf8"
)

// smpDT mirrors a Python datetime.date (isDatetime=false) or datetime.datetime value.
// tz is the fixed UTC offset in microseconds of an aware datetime (nil when naive).
type smpDT struct {
	isDatetime                 bool
	year, month, day           int
	hour, minute, second, usec int
	tz                         *int64
}

const (
	smpMinYear    = 1
	smpMaxYear    = 9999
	smpMaxOrdinal = 3652059
	smpUsPerDay   = int64(86400) * 1000000
)

var (
	smpDaysInMonth     = [13]int{-1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}
	smpDaysBeforeMonth = [13]int{-1, 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334}
)

func smpIsLeap(year int64) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}

func smpDaysInMonthOf(year int64, month int) int {
	if month == 2 && smpIsLeap(year) {
		return 29
	}
	return smpDaysInMonth[month]
}

func smpDaysBeforeYear(year int) int {
	y := year - 1
	return y*365 + y/4 - y/100 + y/400
}

// smpYmd2Ord mirrors datetime._ymd2ord.
func smpYmd2Ord(year, month, day int) int {
	dbm := smpDaysBeforeMonth[month]
	if month > 2 && smpIsLeap(int64(year)) {
		dbm++
	}
	return smpDaysBeforeYear(year) + dbm + day
}

// smpOrd2Ymd mirrors datetime._ord2ymd.
func smpOrd2Ymd(n int) (int, int, int) {
	n--
	n400, n := n/146097, n%146097
	year := n400*400 + 1
	n100, n := n/36524, n%36524
	n4, n := n/1461, n%1461
	n1, n := n/365, n%365
	year += n100*100 + n4*4 + n1
	if n1 == 4 || n100 == 4 {
		return year - 1, 12, 31
	}
	leapyear := n1 == 3 && (n4 != 24 || n100 == 3)
	month := (n + 50) >> 5
	preceding := smpDaysBeforeMonth[month]
	if month > 2 && leapyear {
		preceding++
	}
	if preceding > n {
		month--
		preceding -= smpDaysInMonth[month]
		if month == 2 && leapyear {
			preceding--
		}
	}
	n -= preceding
	return year, month, n + 1
}

func (d smpDT) toordinal() int { return smpYmd2Ord(d.year, d.month, d.day) }

// weekday mirrors date.weekday() (Monday == 0).
func (d smpDT) weekday() int { return (d.toordinal() + 6) % 7 }

// date mirrors datetime.date().
func (d smpDT) date() smpDT { return smpDT{year: d.year, month: d.month, day: d.day} }

// smpDatetimeFromDate mirrors datetime(year=d.year, month=d.month, day=d.day).
func smpDatetimeFromDate(d smpDT) smpDT {
	return smpDT{isDatetime: true, year: d.year, month: d.month, day: d.day}
}

// String mirrors str(date) / str(datetime) (isoformat with a " " separator).
func (d smpDT) String() string {
	s := fmt.Sprintf("%04d-%02d-%02d", d.year, d.month, d.day)
	if !d.isDatetime {
		return s
	}
	s += fmt.Sprintf(" %02d:%02d:%02d", d.hour, d.minute, d.second)
	if d.usec != 0 {
		s += fmt.Sprintf(".%06d", d.usec)
	}
	if d.tz != nil {
		off := *d.tz
		sign := "+"
		if off < 0 {
			sign = "-"
			off = -off
		}
		hh := off / 3600000000
		mm := off % 3600000000 / 60000000
		rem := off % 60000000
		s += fmt.Sprintf("%s%02d:%02d", sign, hh, mm)
		if rem != 0 {
			s += fmt.Sprintf(":%02d", rem/1000000)
			if rem%1000000 != 0 {
				s += fmt.Sprintf(".%06d", rem%1000000)
			}
		}
	}
	return s
}

// smpCheckDate mirrors the range checks of the date constructor / date.replace.
func smpCheckDate(year int64, month, day int) {
	if year < smpMinYear || year > smpMaxYear {
		if year > math.MaxInt32 || year < math.MinInt32 {
			smpRaise("OverflowError", "signed integer is greater than maximum")
		}
		panic(&ValueError{Msg: fmt.Sprintf("year %d is out of range", year)})
	}
	if month < 1 || month > 12 {
		panic(&ValueError{Msg: "month must be in 1..12"})
	}
	if day < 1 || day > smpDaysInMonthOf(year, month) {
		panic(&ValueError{Msg: "day is out of range for month"})
	}
}

// replaceYMD mirrors d.replace(year=..., month=..., day=...).
func (d smpDT) replaceYMD(year int64, month, day int) smpDT {
	smpCheckDate(year, month, day)
	d.year, d.month, d.day = int(year), month, day
	return d
}

// smpTimedelta mirrors a normalized datetime.timedelta.
type smpTimedelta struct {
	days    int64
	seconds int64 // 0 <= seconds < 86400
	usec    int64 // 0 <= usec < 1000000
}

func smpFloorDivMod(a, b int64) (int64, int64) {
	q, r := a/b, a%b
	if r != 0 && (r < 0) != (b < 0) {
		q--
		r += b
	}
	return q, r
}

// smpNewTimedelta mirrors timedelta(days=, hours=, minutes=, seconds=, microseconds=) for ints.
func smpNewTimedelta(days, hours, minutes, seconds, usec int64) smpTimedelta {
	if days > 2*999999999 || days < -2*999999999 {
		smpRaise("OverflowError", fmt.Sprintf("days=%d; must have magnitude <= 999999999", days))
	}
	s := hours*3600 + minutes*60 + seconds
	q, us := smpFloorDivMod(usec, 1000000)
	s += q
	q, s = smpFloorDivMod(s, 86400)
	d := days + q
	if d > 999999999 || d < -999999999 {
		smpRaise("OverflowError", fmt.Sprintf("days=%d; must have magnitude <= 999999999", d))
	}
	return smpTimedelta{days: d, seconds: s, usec: us}
}

func (t smpTimedelta) neg() smpTimedelta {
	return smpNewTimedelta(-t.days, 0, 0, -t.seconds, -t.usec)
}

// smpAddTimedelta mirrors date + timedelta / datetime + timedelta.
func smpAddTimedelta(d smpDT, t smpTimedelta) smpDT {
	if !d.isDatetime {
		o := int64(d.toordinal()) + t.days
		if o > 0 && o <= smpMaxOrdinal {
			y, m, dd := smpOrd2Ymd(int(o))
			return smpDT{year: y, month: m, day: dd}
		}
		smpRaise("OverflowError", "date value out of range")
	}
	delta := smpNewTimedelta(int64(d.toordinal()), int64(d.hour), int64(d.minute), int64(d.second), int64(d.usec))
	delta = smpNewTimedelta(delta.days+t.days, 0, 0, delta.seconds+t.seconds, delta.usec+t.usec)
	hour, rem := delta.seconds/3600, delta.seconds%3600
	minute, second := rem/60, rem%60
	if delta.days > 0 && delta.days <= smpMaxOrdinal {
		y, m, dd := smpOrd2Ymd(int(delta.days))
		return smpDT{
			isDatetime: true, year: y, month: m, day: dd, hour: int(hour), minute: int(minute),
			second: int(second), usec: int(delta.usec), tz: d.tz,
		}
	}
	smpRaise("OverflowError", "date value out of range")
	return smpDT{}
}

// smpDTEq mirrors Python's == between dates/datetimes.
func smpDTEq(a, b smpDT) bool {
	if a.isDatetime != b.isDatetime {
		return false
	}
	if !a.isDatetime {
		return a.year == b.year && a.month == b.month && a.day == b.day
	}
	if (a.tz == nil) != (b.tz == nil) {
		return false
	}
	return a.utcMicros() == b.utcMicros()
}

func (d smpDT) utcMicros() int64 {
	v := int64(d.toordinal())*smpUsPerDay + int64(d.hour)*3600000000 + int64(d.minute)*60000000 +
		int64(d.second)*1000000 + int64(d.usec)
	if d.tz != nil {
		v -= *d.tz
	}
	return v
}

// smpDTCmp mirrors Python's ordering comparisons between dates/datetimes.
func smpDTCmp(a, b smpDT, op string) int {
	if a.isDatetime != b.isDatetime {
		an, bn := "datetime.date", "datetime.date"
		if a.isDatetime {
			an = "datetime.datetime"
		}
		if b.isDatetime {
			bn = "datetime.datetime"
		}
		smpRaise("TypeError", "'"+op+"' not supported between instances of '"+an+"' and '"+bn+"'")
	}
	if !a.isDatetime {
		return smpCmpInts(a.toordinal(), b.toordinal())
	}
	if (a.tz == nil) != (b.tz == nil) {
		smpRaise("TypeError", "can't compare offset-naive and offset-aware datetimes")
	}
	x, y := a.utcMicros(), b.utcMicros()
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	}
	return 0
}

func smpCmpInts(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// ---------------------------------------------------------------------------------------------
// datetime.fromisoformat (CPython 3.13 Modules/_datetimemodule.c)
// ---------------------------------------------------------------------------------------------

type smpISOBuf []byte

// at mirrors reading a NUL-terminated C string (0 past the end).
func (b smpISOBuf) at(i int) byte {
	if i < 0 || i >= len(b) {
		return 0
	}
	return b[i]
}

func smpIsDigitByte(c byte) bool { return c >= '0' && c <= '9' }

// parseDigits mirrors parse_digits: returns the new position, or -1 on failure.
func (b smpISOBuf) parseDigits(p int, v *int, n int) int {
	for i := 0; i < n; i++ {
		c := b.at(p)
		p++
		if !smpIsDigitByte(c) {
			return -1
		}
		*v = *v*10 + int(c-'0')
	}
	return p
}

func smpIsoWeek1Monday(year int) int {
	firstday := smpYmd2Ord(year, 1, 1)
	firstweekday := (firstday + 6) % 7
	week1monday := firstday - firstweekday
	if firstweekday > 3 {
		week1monday += 7
	}
	return week1monday
}

// smpIsoToYmd mirrors iso_to_ymd.
func smpIsoToYmd(isoYear, isoWeek, isoDay int) (int, int, int, int) {
	if isoYear < smpMinYear || isoYear > smpMaxYear {
		return -4, 0, 0, 0
	}
	if isoWeek <= 0 || isoWeek >= 53 {
		outOfRange := true
		if isoWeek == 53 {
			firstWeekday := smpDT{year: isoYear, month: 1, day: 1}.weekday()
			if firstWeekday == 3 || (firstWeekday == 2 && smpIsLeap(int64(isoYear))) {
				outOfRange = false
			}
		}
		if outOfRange {
			return -2, 0, 0, 0
		}
	}
	if isoDay <= 0 || isoDay >= 8 {
		return -3, 0, 0, 0
	}
	day1 := smpIsoWeek1Monday(isoYear)
	dayOffset := (isoWeek-1)*7 + isoDay - 1
	y, m, d := smpOrd2Ymd(day1 + dayOffset)
	return 0, y, m, d
}

// parseIsoformatDate mirrors parse_isoformat_date.
func (b smpISOBuf) parseIsoformatDate(length int, year, month, day *int) int {
	p := b.parseDigits(0, year, 4)
	if p < 0 {
		return -1
	}
	usesSeparator := b.at(p) == '-'
	if usesSeparator {
		p++
	}
	if b.at(p) == 'W' {
		p++
		isoWeek, isoDay := 0, 0
		p = b.parseDigits(p, &isoWeek, 2)
		if p < 0 {
			return -3
		}
		if length < 0 || p < length {
			if usesSeparator {
				c := b.at(p)
				p++
				if c != '-' {
					return -2
				}
			}
			p = b.parseDigits(p, &isoDay, 1)
			if p < 0 {
				return -4
			}
		} else {
			isoDay = 1
		}
		rv, y, m, d := smpIsoToYmd(*year, isoWeek, isoDay)
		if rv != 0 {
			return -3 + rv
		}
		*year, *month, *day = y, m, d
		return 0
	}
	p = b.parseDigits(p, month, 2)
	if p < 0 {
		return -1
	}
	if usesSeparator {
		c := b.at(p)
		p++
		if c != '-' {
			return -2
		}
	}
	p = b.parseDigits(p, day, 2)
	if p < 0 {
		return -1
	}
	return 0
}

// parseHhMmSsFf mirrors parse_hh_mm_ss_ff over b[p:pEnd].
func (b smpISOBuf) parseHhMmSsFf(p, pEnd int, hour, minute, second, usec *int) int {
	*hour, *minute, *second, *usec = 0, 0, 0, 0
	vals := []*int{hour, minute, second}
	hasSeparator := true
	for i := 0; i < 3; i++ {
		p = b.parseDigits(p, vals[i], 2)
		if p < 0 {
			return -3
		}
		c := b.at(p)
		p++
		if i == 0 {
			hasSeparator = c == ':'
		}
		if p >= pEnd {
			if c != 0 {
				return 1
			}
			return 0
		} else if hasSeparator && c == ':' {
			continue
		} else if c == '.' || c == ',' {
			break
		} else if !hasSeparator {
			p--
		} else {
			return -4
		}
	}
	toParse := pEnd - p
	if toParse >= 6 {
		toParse = 6
	}
	p = b.parseDigits(p, usec, toParse)
	if p < 0 {
		return -3
	}
	correction := []int{100000, 10000, 1000, 100, 10}
	if toParse < 6 && toParse >= 1 {
		*usec *= correction[toParse-1]
	}
	for smpIsDigitByte(b.at(p)) {
		p++
	}
	if b.at(p) != 0 {
		return 1
	}
	return 0
}

// parseIsoformatTime mirrors parse_isoformat_time for the bytes b[start:start+dtlen].
func (b smpISOBuf) parseIsoformatTime(start, dtlen int, hour, minute, second, usec *int, tzoffset, tzusec *int) int {
	pEnd := start + dtlen
	tzPos := start
	for {
		c := b.at(tzPos)
		if c == 'Z' || c == '+' || c == '-' {
			break
		}
		tzPos++
		if tzPos >= pEnd {
			break
		}
	}
	rv := b.parseHhMmSsFf(start, tzPos, hour, minute, second, usec)
	if rv < 0 {
		return rv
	} else if tzPos == pEnd {
		if rv == 1 {
			return -5
		}
		return 0
	}
	if b.at(tzPos) == 'Z' {
		*tzoffset = 0
		*tzusec = 0
		if b.at(tzPos+1) != 0 {
			return -5
		}
		return 1
	}
	tzsign := 1
	if b.at(tzPos) == '-' {
		tzsign = -1
	}
	tzPos++
	tzhour, tzminute, tzsecond := 0, 0, 0
	rv = b.parseHhMmSsFf(tzPos, pEnd, &tzhour, &tzminute, &tzsecond, tzusec)
	*tzoffset = tzsign * ((tzhour * 3600) + (tzminute * 60) + tzsecond)
	*tzusec *= tzsign
	if rv != 0 {
		return -5
	}
	return 1
}

// findIsoformatDatetimeSeparator mirrors _find_isoformat_datetime_separator.
func (b smpISOBuf) findIsoformatDatetimeSeparator() int {
	n := len(b)
	if n == 7 {
		return 7
	}
	if b.at(4) == '-' {
		if b.at(5) == 'W' {
			if n < 8 {
				return -1
			}
			if n > 8 && b.at(8) == '-' {
				if n == 9 {
					return -1
				}
				if n > 10 && smpIsDigitByte(b.at(10)) {
					return 8
				}
				return 10
			}
			return 8
		}
		return 10
	}
	if b.at(4) == 'W' {
		idx := 7
		for ; idx < n; idx++ {
			if !smpIsDigitByte(b.at(idx)) {
				break
			}
		}
		if idx < 9 {
			return idx
		}
		if idx%2 == 0 {
			return 7
		}
		return 8
	}
	return 8
}

// smpFromISOFormat mirrors datetime.fromisoformat(s). ok=false means ValueError.
func smpFromISOFormat(s string) (dt smpDT, ok bool) {
	if utf8.RuneCountInString(s) < 7 {
		return smpDT{}, false
	}
	b := smpISOBuf(s)
	sep := b.findIsoformatDatetimeSeparator()
	if sep < 0 {
		return smpDT{}, false
	}
	year, month, day := 0, 0, 0
	hour, minute, second, usec := 0, 0, 0, 0
	tzoffset, tzusec := 0, 0
	rv := b.parseIsoformatDate(sep, &year, &month, &day)
	n := len(b)
	if rv == 0 && n > sep {
		p := sep
		c := b.at(p)
		if c&0x80 == 0 {
			p++
		} else {
			switch c & 0xf0 {
			case 0xe0:
				p += 3
			case 0xf0:
				p += 4
			default:
				p += 2
			}
		}
		rv = b.parseIsoformatTime(p, n-p, &hour, &minute, &second, &usec, &tzoffset, &tzusec)
	}
	if rv < 0 {
		return smpDT{}, false
	}
	var tz *int64
	if rv == 1 {
		if tzoffset == 0 {
			z := int64(0)
			tz = &z
		} else {
			td := smpNewTimedelta(0, 0, 0, int64(tzoffset), int64(tzusec))
			if (td.days == -1 && td.seconds == 0 && td.usec < 1) || td.days < -1 || td.days >= 1 {
				return smpDT{}, false
			}
			off := td.days*smpUsPerDay + td.seconds*1000000 + td.usec
			tz = &off
		}
	}
	if year < smpMinYear || year > smpMaxYear || month < 1 || month > 12 || day < 1 ||
		day > smpDaysInMonthOf(int64(year), month) {
		return smpDT{}, false
	}
	if hour > 23 || minute > 59 || second > 59 || usec > 999999 {
		return smpDT{}, false
	}
	return smpDT{
		isDatetime: true, year: year, month: month, day: day, hour: hour, minute: minute,
		second: second, usec: usec, tz: tz,
	}, true
}

// ---------------------------------------------------------------------------------------------
// dateutil.relativedelta (relative fields only)
// ---------------------------------------------------------------------------------------------

// smpRelDelta mirrors dateutil.relativedelta.relativedelta with relative information only.
// huge marks a value too large for int64: any date arithmetic with it overflows in Python too.
type smpRelDelta struct {
	years, months, days, hours, minutes, seconds, microseconds int64
	huge                                                       bool
}

func smpSign(x int64) int64 {
	if x < 0 {
		return -1
	}
	return 1
}

// fix mirrors relativedelta._fix.
func (r *smpRelDelta) fix() {
	if r.microseconds > 999999 || r.microseconds < -999999 {
		s := smpSign(r.microseconds)
		div, mod := (r.microseconds*s)/1000000, (r.microseconds*s)%1000000
		r.microseconds = mod * s
		r.seconds += div * s
	}
	if r.seconds > 59 || r.seconds < -59 {
		s := smpSign(r.seconds)
		div, mod := (r.seconds*s)/60, (r.seconds*s)%60
		r.seconds = mod * s
		r.minutes += div * s
	}
	if r.minutes > 59 || r.minutes < -59 {
		s := smpSign(r.minutes)
		div, mod := (r.minutes*s)/60, (r.minutes*s)%60
		r.minutes = mod * s
		r.hours += div * s
	}
	if r.hours > 23 || r.hours < -23 {
		s := smpSign(r.hours)
		div, mod := (r.hours*s)/24, (r.hours*s)%24
		r.hours = mod * s
		r.days += div * s
	}
	if r.months > 11 || r.months < -11 {
		s := smpSign(r.months)
		div, mod := (r.months*s)/12, (r.months*s)%12
		r.months = mod * s
		r.years += div * s
	}
}

func (r smpRelDelta) hasTime() bool {
	return r.hours != 0 || r.minutes != 0 || r.seconds != 0 || r.microseconds != 0
}

// truthy mirrors relativedelta.__bool__.
func (r smpRelDelta) truthy() bool {
	return r.huge || r.years != 0 || r.months != 0 || r.days != 0 || r.hours != 0 || r.minutes != 0 ||
		r.seconds != 0 || r.microseconds != 0
}

// neg mirrors relativedelta.__neg__.
func (r smpRelDelta) neg() smpRelDelta {
	n := smpRelDelta{
		years: -r.years, months: -r.months, days: -r.days, hours: -r.hours, minutes: -r.minutes,
		seconds: -r.seconds, microseconds: -r.microseconds, huge: r.huge,
	}
	n.fix()
	return n
}

// addTo mirrors relativedelta.__add__(date) (also used for date + relativedelta).
func (r smpRelDelta) addTo(other smpDT) smpDT {
	if r.huge {
		smpRaise("OverflowError", "date value out of range")
	}
	if r.hasTime() && !other.isDatetime {
		other = smpDatetimeFromDate(other)
	}
	year := int64(other.year) + r.years
	month := int64(other.month)
	if r.months != 0 {
		month += r.months
		if month > 12 {
			year++
			month -= 12
		} else if month < 1 {
			year--
			month += 12
		}
	}
	day := smpDaysInMonthOf(year, int(month))
	if other.day < day {
		day = other.day
	}
	ret := other.replaceYMD(year, int(month), day)
	return smpAddTimedelta(ret, smpNewTimedelta(r.days, r.hours, r.minutes, r.seconds, r.microseconds))
}

// subFrom mirrors date - relativedelta.
func (r smpRelDelta) subFrom(other smpDT) smpDT { return r.neg().addTo(other) }

// smpCheckedMul multiplies n by k, reporting int64 overflow.
func smpCheckedMul(n *big.Int, k int64) (int64, bool) {
	v := new(big.Int).Mul(n, big.NewInt(k))
	if !v.IsInt64() {
		return 0, false
	}
	return v.Int64(), true
}

// smpInterval mirrors simplify.interval(unit, n).
func smpInterval(unit string, n *big.Int) smpRelDelta {
	var r smpRelDelta
	var field *int64
	var k int64 = 1
	switch unit {
	case "year":
		field = &r.years
	case "quarter":
		field, k = &r.months, 3
	case "month":
		field = &r.months
	case "week":
		field, k = &r.days, 7
	case "day":
		field = &r.days
	case "hour":
		field = &r.hours
	case "minute":
		field = &r.minutes
	case "second":
		field = &r.seconds
	case "millisecond":
		field, k = &r.microseconds, 1000
	case "microsecond":
		field = &r.microseconds
	default:
		panic(&smpUnsupportedUnit{msg: "Unsupported unit: " + unit})
	}
	v, ok := smpCheckedMul(n, k)
	if !ok || v > math.MaxInt64/4 || v < math.MinInt64/4 {
		return smpRelDelta{huge: true}
	}
	*field = v
	r.fix()
	return r
}

func smpIntervalOne(unit string) smpRelDelta { return smpInterval(unit, big.NewInt(1)) }
