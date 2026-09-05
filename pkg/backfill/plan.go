// Package backfill plans and records resumable local partitioned runs.
package backfill

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"iter"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/bruin-data/bruin/pkg/date"
)

// Plan contains the immutable inputs needed to regenerate a backfill. Intervals
// are generated lazily; neither planning nor execution retains the whole range.
type Plan struct {
	Target           string              `json:"target"`
	WorkingDirectory string              `json:"working_directory"`
	Environment      string              `json:"environment"`
	RunFlags         map[string][]string `json:"run_flags"`
	Start            time.Time           `json:"start"`
	End              time.Time           `json:"end"` // exclusive
	Timezone         string              `json:"timezone"`
	Partition        string              `json:"partition"`
}

type Interval struct {
	ID    string    `json:"id"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end"` // exclusive
}

// ParseRange treats date-only ends as inclusive calendar dates and timestamp
// ends as exclusive instants. Unzoned inputs use the explicit IANA timezone.
func ParseRange(start, end, zone string) (time.Time, time.Time, error) {
	loc, err := time.LoadLocation(zone)
	if err != nil || zone == "" || zone == "Local" {
		return time.Time{}, time.Time{}, fmt.Errorf("timezone must be UTC or an IANA timezone: %q", zone)
	}
	parse := func(value string) (time.Time, error) {
		t, format, err := date.ParseTimeWithFormat(value)
		if err != nil {
			return t, err
		}
		if format == "2006-01-02" || format == "02 Jan 2006" {
			t = calendarStart(t.Year(), t.Month(), t.Day(), loc)
		} else if !strings.Contains(format, "Z07") {
			t, err = time.ParseInLocation(format, value, loc)
			if err == nil && t.Format(format) != value {
				return t, fmt.Errorf("nonexistent local time %q; use a timestamp with an explicit offset", value)
			}
		}
		if t.Nanosecond()%1000 != 0 {
			return t, fmt.Errorf("timestamps must have at most microsecond precision")
		}
		return t.In(loc), err
	}
	a, err := parse(start)
	if err != nil {
		return a, time.Time{}, fmt.Errorf("invalid start date: %w", err)
	}
	b, err := parse(end)
	if err != nil {
		return a, b, fmt.Errorf("invalid end date: %w", err)
	}
	if len(end) == len("2006-01-02") || len(end) == len("02 Jan 2006") {
		original, _ := date.ParseTime(end)
		next := original.AddDate(0, 0, 1)
		loc, _ := time.LoadLocation(zone)
		b = calendarStart(next.Year(), next.Month(), next.Day(), loc)
	}
	if !a.Before(b) {
		return a, b, fmt.Errorf("start date must be before the exclusive end date")
	}
	return a, b, nil
}

func (p Plan) Validate() error {
	if p.Target == "" {
		return fmt.Errorf("backfill target is required")
	}
	if !p.Start.Before(p.End) || p.Start.Year() < 1 || p.End.Year() > 9999 {
		return fmt.Errorf("invalid backfill date range")
	}
	if p.Start.Nanosecond()%1000 != 0 || p.End.Nanosecond()%1000 != 0 {
		return fmt.Errorf("timestamps must have at most microsecond precision")
	}
	if p.Timezone == "" || p.Timezone == "Local" {
		return fmt.Errorf("an explicit timezone is required")
	}
	if _, err := time.LoadLocation(p.Timezone); err != nil {
		return err
	}
	switch p.Partition {
	case "hourly", "daily", "weekly", "monthly", "yearly":
		return nil
	default:
		d, err := time.ParseDuration(p.Partition)
		if err != nil || d < time.Microsecond || d%time.Microsecond != 0 {
			return fmt.Errorf("partition must be hourly, daily, weekly, monthly, yearly, or a positive duration in whole microseconds")
		}
	}
	return nil
}

func digest(value any) string {
	data, _ := json.Marshal(value) // only JSON-native structs are passed here
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Intervals yields the same partitions in either order. Calendar partitions
// align to local boundaries (Monday for weeks); duration partitions anchor at Start.
// Call Validate before iterating an externally supplied plan.
func (p Plan) Intervals(reverse bool) iter.Seq[Interval] {
	return func(yield func(Interval) bool) {
		loc, _ := time.LoadLocation(p.Timezone)
		start, end := p.Start.In(loc), p.End.In(loc)
		duration, _ := time.ParseDuration(p.Partition)
		if p.Partition == "hourly" {
			duration = time.Hour
		}
		identity := p.identity()
		cursor := start
		if reverse {
			cursor = end
		}
		for (!reverse && cursor.Before(end)) || (reverse && cursor.After(start)) {
			var a, b time.Time
			if reverse {
				b = cursor
				if duration > 0 {
					n := (b.UnixMicro() - start.UnixMicro() - 1) / duration.Microseconds()
					a = time.UnixMicro(start.UnixMicro() + n*duration.Microseconds()).In(loc)
				} else {
					a = floor(cursor.Add(-time.Microsecond), p.Partition)
				}
				if a.Before(start) {
					a = start
				}
				cursor = a
			} else {
				a = cursor
				if duration > 0 {
					b = a.Add(duration)
				} else {
					b = advance(floor(a, p.Partition), p.Partition)
				}
				if b.After(end) {
					b = end
				}
				cursor = b
			}
			interval := Interval{Start: a, End: b}
			interval.ID = digest([]string{identity, a.UTC().Format(time.RFC3339Nano), b.UTC().Format(time.RFC3339Nano)})
			if !yield(interval) {
				return
			}
		}
	}
}

func floor(t time.Time, partition string) time.Time {
	if partition == "hourly" {
		return t.Add(-time.Duration(t.Minute())*time.Minute - time.Duration(t.Second())*time.Second - time.Duration(t.Nanosecond()))
	}
	y, m, d := t.Date()
	switch partition {
	case "weekly":
		// Noon avoids date arithmetic across a DST transition at midnight.
		n := time.Date(y, m, d, 12, 0, 0, 0, t.Location()).AddDate(0, 0, -(int(t.Weekday())+6)%7)
		y, m, d = n.Date()
	case "monthly":
		d = 1
	case "yearly":
		m, d = time.January, 1
	}
	return calendarStart(y, m, d, t.Location())
}

// calendarStart returns the first instant of a local calendar date. Some zones
// skip midnight or repeat it; time.Date alone may select the previous date or
// the second occurrence. A skipped date (e.g. Apia 2011-12-30) maps to the next
// existing date, which naturally omits the empty interval.
func calendarStart(y int, m time.Month, d int, loc *time.Location) time.Time {
	wanted := time.Date(y, m, d, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	t := time.Date(y, m, d, 0, 0, 0, 0, loc)
	for t.Format("2006-01-02") < wanted {
		t = t.Add(time.Minute)
	}
	for t.Add(-time.Minute).Format("2006-01-02") == wanted {
		t = t.Add(-time.Minute)
	}
	return t
}

func advance(t time.Time, partition string) time.Time {
	y, m, d := t.Date()
	date := time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	switch partition {
	case "hourly":
		return t.Add(time.Hour)
	case "daily":
		date = date.AddDate(0, 0, 1)
	case "weekly":
		date = date.AddDate(0, 0, 7)
	case "monthly":
		date = date.AddDate(0, 1, 0)
	default:
		date = date.AddDate(1, 0, 0)
	}
	return calendarStart(date.Year(), date.Month(), date.Day(), t.Location())
}

func (p Plan) identity() string {
	return digest(struct {
		Target, Environment string
		Flags               map[string][]string
	}{p.Target, p.Environment, p.RunFlags})
}

// Count calculates elapsed-time partition counts directly and walks calendar
// boundaries without creating partition IDs or reading queued records.
func (p Plan) Count(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := p.Validate(); err != nil {
		return 0, err
	}
	d, _ := time.ParseDuration(p.Partition)
	if p.Partition == "hourly" {
		d = time.Hour
	}
	if d > 0 {
		n := 1 + (p.End.UnixMicro()-p.Start.UnixMicro()-1)/d.Microseconds()
		if int64(int(n)) != n {
			return 0, fmt.Errorf("partition count exceeds this platform's integer capacity")
		}
		return int(n), nil
	}
	loc, _ := time.LoadLocation(p.Timezone)
	count := 0
	for cursor := p.Start.In(loc); cursor.Before(p.End); {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		next := advance(floor(cursor, p.Partition), p.Partition)
		if !next.After(cursor) {
			return 0, fmt.Errorf("partition boundary did not advance at %s", cursor)
		}
		cursor = next
		count++
	}
	return count, nil
}

// validateInterval rejects unrelated or damaged records when scanning the store.
func (p Plan) validateInterval(i Interval) error {
	loc, _ := time.LoadLocation(p.Timezone)
	start := i.Start.In(loc)
	if start.Nanosecond()%1000 != 0 {
		return fmt.Errorf("partition timestamp exceeds microsecond precision")
	}
	if start.Before(p.Start) || !start.Before(p.End) {
		return fmt.Errorf("partition is outside the plan")
	}
	d, _ := time.ParseDuration(p.Partition)
	if p.Partition == "hourly" {
		d = time.Hour
	}
	var end time.Time
	if d > 0 {
		if (start.UnixMicro()-p.Start.UnixMicro())%d.Microseconds() != 0 {
			return fmt.Errorf("partition is not aligned with the plan")
		}
		end = start.Add(d)
	} else {
		if !start.Equal(p.Start) && !floor(start, p.Partition).Equal(start) {
			return fmt.Errorf("partition is not aligned with the plan")
		}
		end = advance(floor(start, p.Partition), p.Partition)
	}
	if end.After(p.End) {
		end = p.End
	}
	id := digest([]string{p.identity(), start.UTC().Format(time.RFC3339Nano), end.UTC().Format(time.RFC3339Nano)})
	if i.ID != id || !i.End.Equal(end) {
		return fmt.Errorf("partition record does not match plan: %s", i.ID)
	}
	return nil
}
