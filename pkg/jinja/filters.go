package jinja

import (
	"strconv"

	"github.com/bruin-data/bruin/pkg/date"
	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"github.com/pkg/errors"
)

var Filters *exec.FilterSet

func init() { //nolint:gochecknoinits
	Filters = gonja.DefaultEnvironment.Filters
	err := Filters.Register("add_days", addDays)
	if err != nil {
		panic(err)
	}

	err = Filters.Register("date_add", addDays)
	if err != nil {
		panic(err)
	}

	err = Filters.Register("date_format", formatDate)
	if err != nil {
		panic(err)
	}
}

func addDays(e *exec.Evaluator, in *exec.Value, params *exec.VarArgs) *exec.Value {
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

	// add the days
	days := params.Args[0].String()
	daysInt, err := strconv.Atoi(days)
	if err != nil {
		return exec.AsValue(errors.Errorf("invalid number of days for add_days, it must be a valid integer, '%s' given", days))
	}

	parsed = parsed.AddDate(0, 0, daysInt)
	return exec.AsValue(parsed.Format(format))
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
