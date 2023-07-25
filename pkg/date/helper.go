package date

import (
	"errors"
	"strings"
	"time"
)

func ParseTime(input string) (time.Time, error) {
	allowedFormats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04",
		"2006-01-02T15:04",
		"2006-01-02",
	}

	for _, format := range allowedFormats {
		t, err := time.Parse(format, input)
		if err == nil {
			return t, nil
		}
	}

	return time.Time{}, errors.New("invalid datetime format")
}

func ConvertPythonDateFormatToGolang(pythonFormat string) string {
	replacements := map[string]string{
		"%Y": "2006",
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
