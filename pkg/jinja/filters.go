package jinja

import (
	"strconv"
	"time"

	"github.com/bruin-data/bruin/pkg/date"
	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"github.com/pkg/errors"
)

var Filters *exec.FilterSet

func init() { //nolint:gochecknoinits
	Filters = gonja.DefaultEnvironment.Filters
	filterMap := map[string]exec.FilterFunction{
		"add_days":         addDays,
		"add_hours":        addHours,
		"add_minutes":      addMinutes,
		"add_seconds":      addSeconds,
		"add_milliseconds": addMilliseconds,
		"add_months":       addMonths,
		"add_years":        addYears,
		"date_add":         addDays,
		"date_format":      formatDate,
		"truncate_year":    truncateYear,
		"truncate_month":   truncateMonth,
		"truncate_day":     truncateDay,
		"truncate_hour":    truncateHour,
	}

	for name, filter := range filterMap {
		err := Filters.Register(name, filter)
		if err != nil {
			panic(err)
		}
	}
}

func dateModifier(_ *exec.Evaluator, in *exec.Value, params *exec.VarArgs, modifierFunc func(time.Time) time.Time) *exec.Value {
	if in.IsError() {
		return in
	}
	if p := params.ExpectArgs(1); p.IsError() {
		return exec.AsValue(errors.Wrap(p, "'add_days' accept only a single argument"))
	}

	// parse the input to a date object
	parsed, format, err := date.ParseTimeWithFormat(in.String())
	if err != nil {
		return exec.AsValue(errors.Wrap(err, "invalid date format"))
	}

	parsed = modifierFunc(parsed)
	return exec.AsValue(parsed.Format(format))
}

func addDays(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	// add the days
	days := params.Args[0].String()
	daysInt, err := strconv.Atoi(days)
	if err != nil {
		return exec.AsValue(errors.Errorf("invalid number of days for add_days, it must be a valid integer, '%s' given", days))
	}

	return dateModifier(e, in, params, func(t time.Time) time.Time {
		return t.AddDate(0, 0, daysInt)
	})
}

func addHours(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	// add the days
	hours := params.Args[0].String()
	hoursInt, err := strconv.Atoi(hours)
	if err != nil {
		return exec.AsValue(errors.Errorf("invalid number of hours for add_hours, it must be a valid integer, '%s' given", hours))
	}

	return dateModifier(e, in, params, func(t time.Time) time.Time {
		return t.Add(time.Duration(hoursInt) * time.Hour)
	})
}

func addMinutes(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	minutes := params.Args[0].String()
	minutesInt, err := strconv.Atoi(minutes)
	if err != nil {
		return exec.AsValue(errors.Errorf("invalid number of minutes for add_minutes, it must be a valid integer, '%s' given", minutes))
	}

	return dateModifier(e, in, params, func(t time.Time) time.Time {
		return t.Add(time.Duration(minutesInt) * time.Minute)
	})
}

func addSeconds(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	seconds := params.Args[0].String()
	secondsInt, err := strconv.Atoi(seconds)
	if err != nil {
		return exec.AsValue(errors.Errorf("invalid number of seconds for add_seconds, it must be a valid integer, '%s' given", seconds))
	}

	return dateModifier(e, in, params, func(t time.Time) time.Time {
		return t.Add(time.Duration(secondsInt) * time.Second)
	})
}

func addMilliseconds(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	milliseconds := params.Args[0].String()
	millisecondsInt, err := strconv.Atoi(milliseconds)
	if err != nil {
		return exec.AsValue(errors.Errorf("invalid number of milliseconds for add_milliseconds, it must be a valid integer, '%s' given", milliseconds))
	}

	return dateModifier(e, in, params, func(t time.Time) time.Time {
		return t.Add(time.Duration(millisecondsInt) * time.Millisecond)
	})
}

func addMonths(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	months := params.Args[0].String()
	monthsInt, err := strconv.Atoi(months)
	if err != nil {
		return exec.AsValue(errors.Errorf("invalid number of months for add_months, it must be a valid integer, '%s' given", months))
	}

	return dateModifier(e, in, params, func(t time.Time) time.Time {
		return t.AddDate(0, monthsInt, 0)
	})
}

func addYears(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	years := params.Args[0].String()
	yearsInt, err := strconv.Atoi(years)
	if err != nil {
		return exec.AsValue(errors.Errorf("invalid number of years for add_years, it must be a valid integer, '%s' given", years))
	}

	return dateModifier(e, in, params, func(t time.Time) time.Time {
		return t.AddDate(yearsInt, 0, 0)
	})
}

func formatDate(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	if in.IsError() {
		return in
	}
	if p := params.ExpectArgs(1); p.IsError() {
		return exec.AsValue(errors.Wrap(p, "'add_days' accept only a single argument"))
	}

	// parse the input to a date object
	stringInput := in.String()
	parsed, err := date.ParseTime(stringInput)
	if err != nil {
		return exec.AsValue(errors.Errorf("invalid date format, %s given", stringInput))
	}

	// add the days
	format := params.Args[0].String()

	return exec.AsValue(parsed.Format(date.ConvertPythonDateFormatToGolang(format)))
}

func truncateYear(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	if in.IsError() {
		return in
	}

	parsed, format, err := date.ParseTimeWithFormat(in.String())
	if err != nil {
		return exec.AsValue(errors.Wrap(err, "invalid date format"))
	}

	// Set to January 1st 00:00:00
	truncated := time.Date(parsed.Year(), 1, 1, 0, 0, 0, 0, parsed.Location())
	return exec.AsValue(truncated.Format(format))
}

func truncateMonth(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	if in.IsError() {
		return in
	}

	parsed, format, err := date.ParseTimeWithFormat(in.String())
	if err != nil {
		return exec.AsValue(errors.Wrap(err, "invalid date format"))
	}

	// Set to 1st day of current month 00:00:00
	truncated := time.Date(parsed.Year(), parsed.Month(), 1, 0, 0, 0, 0, parsed.Location())
	return exec.AsValue(truncated.Format(format))
}

func truncateDay(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	if in.IsError() {
		return in
	}

	parsed, format, err := date.ParseTimeWithFormat(in.String())
	if err != nil {
		return exec.AsValue(errors.Wrap(err, "invalid date format"))
	}

	// Set to beginning of current day 00:00:00
	truncated := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, parsed.Location())
	return exec.AsValue(truncated.Format(format))
}

func truncateHour(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
	if in.IsError() {
		return in
	}

	parsed, format, err := date.ParseTimeWithFormat(in.String())
	if err != nil {
		return exec.AsValue(errors.Wrap(err, "invalid date format"))
	}

	// Set to beginning of current hour with 00:00
	truncated := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), parsed.Hour(), 0, 0, 0, parsed.Location())
	return exec.AsValue(truncated.Format(format))
}
