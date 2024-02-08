package jinja

import (
	"time"

	"github.com/bruin-data/bruin/pkg/date"
)

// this func uses variadic arguments because I couldn't find a nicer way of supporting optional arguments
// if anyone has a better idea that won't break the API, I am open for contributions.
// the contract is: date_add(date, days, output_format, input_format).
func dateAdd(args ...interface{}) string {
	if len(args) < 2 {
		return "at least 2 arguments needed for date_add"
	}

	inputFormat := ""
	if len(args) > 3 {
		inputFormatCasted, ok := args[3].(string)
		if !ok {
			return "invalid input format"
		}
		inputFormat = date.ConvertPythonDateFormatToGolang(inputFormatCasted)
	}

	var inputDate time.Time
	var err error

	inputDateString, ok := args[0].(string)
	if !ok {
		return "invalid date"
	}

	if inputFormat == "" {
		inputDate, err = date.ParseTime(inputDateString)
		if err != nil {
			return "invalid date format:" + inputDateString
		}
	} else {
		inputDate, err = time.Parse(inputFormat, inputDateString)
		if err != nil {
			return "invalid date format:" + inputDateString
		}
	}

	days, ok := args[1].(int)
	if !ok {
		return "invalid days for date_add"
	}

	outputFormat := "2006-01-02"
	if len(args) > 2 {
		outputFormatString, ok := args[2].(string)
		if !ok {
			return "invalid output format"
		}

		outputFormat = date.ConvertPythonDateFormatToGolang(outputFormatString)
	}

	format := inputDate.AddDate(0, 0, days).Format(outputFormat)
	return format
}

// this func uses variadic arguments because I couldn't find a nicer way of supporting optional arguments
// if anyone has a better idea that won't break the API, I am open for contributions.
// the contract is: date_format(date, output_format, input_format).
func dateFormat(args ...interface{}) string {
	if len(args) < 2 || len(args) > 3 {
		return "invalid arguments for date_format"
	}

	inputFormat := ""
	if len(args) > 2 {
		inputFormatCasted, ok := args[2].(string)
		if !ok {
			return "invalid input format"
		}
		inputFormat = date.ConvertPythonDateFormatToGolang(inputFormatCasted)
	}

	var inputDate time.Time
	var err error

	inputDateString, ok := args[0].(string)
	if !ok {
		return "invalid date"
	}

	if inputFormat == "" {
		inputDate, err = date.ParseTime(inputDateString)
		if err != nil {
			return "invalid date format:" + inputDateString
		}
	} else {
		inputDate, err = time.Parse(inputFormat, inputDateString)
		if err != nil {
			return "invalid date format:" + inputFormat
		}
	}

	outputFormatString, ok := args[1].(string)
	if !ok {
		return "invalid output format"
	}

	outputFormat := date.ConvertPythonDateFormatToGolang(outputFormatString)
	format := inputDate.Format(outputFormat)
	return format
}
