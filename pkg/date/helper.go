package date

import (
	"errors"
	"strings"
	"time"
)

func ParseTime(input string) (time.Time, error) {
	t, _, err := ParseTimeWithFormat(input)
	return t, err
}

func ParseTimeWithFormat(input string) (time.Time, string, error) {
	allowedFormats := []string{
		"2006-01-02 15:04:05.000000Z07:00",
		"2006-01-02T15:04:05.000000Z07:00",
		"2006-01-02 15:04:05.000000",
		"2006-01-02T15:04:05.000000",
		"2006-01-02 15:04:05.000Z07:00",
		"2006-01-02T15:04:05.000Z07:00",
		"2006-01-02 15:04:05.000",
		"2006-01-02T15:04:05.000",
		"2006-01-02 15:04:05Z07:00",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04Z07:00",
		"2006-01-02T15:04Z07:00",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006-01-02",
		"02 Jan 2006 15:04:05.000Z07:00",
		"02 Jan 2006 15:04:05Z07:00",
		"02 Jan 2006 15:04Z07:00",
		"02 Jan 2006",
	}

	for _, format := range allowedFormats {
		t, err := time.Parse(format, input)
		if err == nil {
			return t, format, nil
		}
	}

	return time.Time{}, "", errors.New("invalid datetime format")
}

func ConvertPythonDateFormatToGolang(pythonFormat string) string {
	replacements := map[string]string{
		"%Y": "2006",
		"%y": "06",
		"%m": "01",
		"%d": "02",
		"%H": "15",
		"%M": "04",
		"%S": "05",
		"%z": "MST",
		"%Z": "MST",
		"%a": "Mon",
		"%A": "Monday",
		"%b": "Jan",
		"%B": "January",
	}
	goFormat := pythonFormat
	for python, goStr := range replacements {
		goFormat = strings.ReplaceAll(goFormat, python, goStr)
	}

	if strings.Contains(goFormat, "MST") {
		loc, err := time.LoadLocation("")
		if err == nil {
			z := time.Now().In(loc).Format("Z")
			goFormat = strings.ReplaceAll(goFormat, "MST", z)
		}
	}

	return goFormat
}
