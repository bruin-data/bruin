package cmd

import (
	"database/sql/driver"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"
)

// formatPostgresValue renders the pgx value types that %v and encoding/json would
// print as raw struct fields or byte arrays, e.g. {7203500000 1 0 true} for an
// interval, as the text Postgres itself prints. It returns false for any other
// value. NULL values format as nil.
func formatPostgresValue(cell interface{}) (interface{}, bool) {
	switch v := cell.(type) {
	case pgtype.Interval:
		if !v.Valid {
			return nil, true
		}
		return formatPgInterval(v), true
	case pgtype.Time:
		if !v.Valid {
			return nil, true
		}
		return formatPgClock(v.Microseconds), true
	case pgtype.Range[interface{}]:
		return formatPgRange(v), true
	case pgtype.Multirange[pgtype.Range[interface{}]]:
		ranges := make([]string, len(v))
		for i, r := range v {
			ranges[i] = fmt.Sprintf("%v", formatPgRange(r))
		}
		return "{" + strings.Join(ranges, ",") + "}", true
	case pgtype.InfinityModifier:
		// pgx returns 'infinity' and '-infinity' dates and timestamps as this enum.
		return v.String(), true
	case [16]byte:
		// pgx returns uuid columns as a bare [16]byte.
		return fmt.Sprintf("%x-%x-%x-%x-%x", v[0:4], v[4:6], v[6:8], v[8:10], v[10:16]), true
	case pgtype.Point, pgtype.Line, pgtype.Lseg, pgtype.Box, pgtype.Path, pgtype.Polygon,
		pgtype.Circle, pgtype.Bits, pgtype.TID, pgtype.TSVector:
		// For these types pgx's Value() is already the Postgres text form.
		value, err := cell.(driver.Valuer).Value()
		if err != nil {
			return nil, false
		}
		return value, true
	default:
		return nil, false
	}
}

// formatPgNumeric renders a Postgres NUMERIC as Postgres prints it, e.g. 42.86,
// NaN or Infinity, and returns nil for NULL. pgx returns NUMERIC columns as
// pgtype.Numeric, which %v would print as its raw struct fields.
func formatPgNumeric(n pgtype.Numeric) interface{} {
	value, err := n.Value()
	if err != nil {
		return n
	}
	return value
}

// formatPgInterval matches Postgres' default IntervalStyle, e.g. "1 year 2 mons
// 3 days 04:05:06.5" or "-1 years -2 mons +3 days -04:05:06". pgx's own text
// encoding differs ("14 mon 3 day 04:05:06.500000").
func formatPgInterval(interval pgtype.Interval) string {
	// Postgres 17 stores infinite intervals as every field at its limit.
	switch {
	case interval.Months == math.MaxInt32 && interval.Days == math.MaxInt32 && interval.Microseconds == math.MaxInt64:
		return "infinity"
	case interval.Months == math.MinInt32 && interval.Days == math.MinInt32 && interval.Microseconds == math.MinInt64:
		return "-infinity"
	}

	var b strings.Builder
	// A field after a negative one shows an explicit + sign, as Postgres prints it.
	isZero, afterNegative := true, false
	addPart := func(value int64, unit string) {
		if value == 0 {
			return
		}
		if !isZero {
			b.WriteByte(' ')
		}
		if afterNegative && value > 0 {
			b.WriteByte('+')
		}
		fmt.Fprintf(&b, "%d %s", value, unit)
		if value != 1 {
			b.WriteByte('s')
		}
		isZero, afterNegative = false, value < 0
	}
	addPart(int64(interval.Months/12), "year")
	addPart(int64(interval.Months%12), "mon")
	addPart(int64(interval.Days), "day")

	usec := interval.Microseconds
	if isZero || usec != 0 {
		if !isZero {
			b.WriteByte(' ')
		}
		if usec < 0 {
			b.WriteByte('-')
			usec = -usec
		} else if afterNegative {
			b.WriteByte('+')
		}
		b.WriteString(formatPgClock(usec))
	}
	return b.String()
}

// formatPgClock renders a non-negative number of microseconds as hh:mm:ss with
// the fractional seconds Postgres prints, e.g. 02:00:03.5. Hours can exceed 24.
func formatPgClock(usec int64) string {
	const usecPerSecond = 1_000_000
	seconds, fraction := usec/usecPerSecond, usec%usecPerSecond
	text := fmt.Sprintf("%02d:%02d:%02d", seconds/3600, seconds/60%60, seconds%60)
	if fraction != 0 {
		text += "." + strings.TrimRight(fmt.Sprintf("%06d", fraction), "0")
	}
	return text
}

// formatPgRange renders a range as Postgres prints it, e.g. [1,10), (1.5,2.25],
// [5,) or empty, and returns nil for NULL.
func formatPgRange(r pgtype.Range[interface{}]) interface{} {
	if !r.Valid {
		return nil
	}
	if r.LowerType == pgtype.Empty {
		return "empty"
	}

	var b strings.Builder
	if r.LowerType == pgtype.Inclusive {
		b.WriteByte('[')
	} else {
		b.WriteByte('(')
	}
	if r.LowerType != pgtype.Unbounded {
		b.WriteString(formatPgRangeBound(r.Lower))
	}
	b.WriteByte(',')
	if r.UpperType != pgtype.Unbounded {
		b.WriteString(formatPgRangeBound(r.Upper))
	}
	if r.UpperType == pgtype.Inclusive {
		b.WriteByte(']')
	} else {
		b.WriteByte(')')
	}
	return b.String()
}

// formatPgRangeBound quotes a bound the way Postgres does when it holds a
// delimiter or whitespace, e.g. a timestamp.
func formatPgRangeBound(bound interface{}) string {
	text := fmt.Sprintf("%v", formatQueryCellForDisplay(bound))
	if text != "" && !strings.ContainsAny(text, "\"\\()[], \t\n\r\v\f") {
		return text
	}
	escaped := strings.NewReplacer(`"`, `""`, `\`, `\\`).Replace(text)
	return `"` + escaped + `"`
}
