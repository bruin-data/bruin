package jinja

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"

	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"github.com/pkg/errors"
)

type Renderer struct {
	context         *exec.Context
	queryRenderLock *sync.Mutex
}

func init() { //nolint: gochecknoinits
	gonja.DefaultConfig.StrictUndefined = true
}

var (
	missingVariableRegex = regexp.MustCompile(`name\s+"([^"]+)"`)
	locationRegex        = regexp.MustCompile(`\(Line: \d+ Col: \d+, near ".*?"\)`)
)

type Context map[string]any

func NewRenderer(context Context) *Renderer {
	return &Renderer{
		context:         exec.NewContext(context),
		queryRenderLock: &sync.Mutex{},
	}
}

func PythonEnvVariables(startDate, endDate *time.Time, pipelineName, runID string, fullRefresh bool) map[string]string {
	vars := map[string]string{
		"BRUIN_START_DATE":      startDate.Format("2006-01-02"),
		"BRUIN_START_DATETIME":  startDate.Format("2006-01-02T15:04:05"),
		"BRUIN_START_TIMESTAMP": startDate.Format("2006-01-02T15:04:05.000000Z07:00"),
		"BRUIN_END_DATE":        endDate.Format("2006-01-02"),
		"BRUIN_END_DATETIME":    endDate.Format("2006-01-02T15:04:05"),
		"BRUIN_END_TIMESTAMP":   endDate.Format("2006-01-02T15:04:05.000000Z07:00"),
		"BRUIN_RUN_ID":          runID,
		"BRUIN_PIPELINE":        pipelineName,
		"BRUIN_FULL_REFRESH":    "",
	}

	if fullRefresh {
		vars["BRUIN_FULL_REFRESH"] = "1"
	}

	return vars
}

func NewRendererWithStartEndDates(startDate, endDate *time.Time, pipelineName, runID string) *Renderer {
	ctx := exec.NewContext(map[string]any{
		"start_date":        startDate.Format("2006-01-02"),
		"start_date_nodash": startDate.Format("20060102"),
		"start_datetime":    startDate.Format("2006-01-02T15:04:05"),
		"start_timestamp":   startDate.Format("2006-01-02T15:04:05.000000Z07:00"),
		"end_date":          endDate.Format("2006-01-02"),
		"end_date_nodash":   endDate.Format("20060102"),
		"end_datetime":      endDate.Format("2006-01-02T15:04:05"),
		"end_timestamp":     endDate.Format("2006-01-02T15:04:05.000000Z07:00"),
		"pipeline":          pipelineName,
		"run_id":            runID,
	})

	return &Renderer{
		context:         ctx,
		queryRenderLock: &sync.Mutex{},
	}
}

func NewRendererWithYesterday(pipelineName, runID string) *Renderer {
	yesterday := time.Now().AddDate(0, 0, -1)
	startDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	endDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, time.UTC)
	return NewRendererWithStartEndDates(&startDate, &endDate, pipelineName, runID)
}

func UpdateContextWithIntervalModifiers(ctx *exec.Context, modifiers pipeline.IntervalModifiers) error {
	modifyAndFormat := func(key string, format string, modifier string) error {
		value, exists := ctx.Get(key)
		if !exists {
			return fmt.Errorf("key %s not found in context", key)
		}
		originalTime, ok := value.(time.Time)
		if !ok {
			return fmt.Errorf("invalid time value for %s", key)
		}

		modifiedTime, err := pipeline.ModifyDate(originalTime, modifier)
		if err != nil {
			return fmt.Errorf("failed to modify date for %s: %w", key, err)
		}

		ctx.Set(key, modifiedTime.Format(format))
		return nil
	}

	if modifiers.Start != "" {
		dateFormats := map[string]string{
			"start_date":        "2006-01-02",
			"start_date_nodash": "20060102",
			"start_datetime":    "2006-01-02T15:04:05",
			"start_timestamp":   "2006-01-02T15:04:05.000000Z07:00",
		}

		for key, format := range dateFormats {
			if err := modifyAndFormat(key, format, modifiers.Start); err != nil {
				return err
			}
		}
	}

	if modifiers.End != "" {
		dateFormats := map[string]string{
			"end_date":        "2006-01-02",
			"end_date_nodash": "20060102",
			"end_datetime":    "2006-01-02T15:04:05",
			"end_timestamp":   "2006-01-02T15:04:05.000000Z07:00",
		}

		for key, format := range dateFormats {
			if err := modifyAndFormat(key, format, modifiers.End); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *Renderer) Render(query string, modifiers pipeline.IntervalModifiers) (string, error) {
	r.queryRenderLock.Lock()
	tpl, err := gonja.FromString(query)
	if err != nil {
		r.queryRenderLock.Unlock()
		customError := findParserErrorType(err)
		if customError == "" {
			return "", errors.Wrap(err, "you have found a bug in the jinja parser, please report it")
		}

		return "", errors.New(customError)
	}
	r.queryRenderLock.Unlock()

	// Now you can render the template with the given
	// gonja.context how often you want to.
	if err := UpdateContextWithIntervalModifiers(r.context, modifiers); err != nil {
		return "", err
	}
	
	out, err := tpl.ExecuteToString(r.context)
	if err != nil {
		customError := findRenderErrorType(err)
		if customError == "" {
			return "", errors.Wrap(err, "you have found a bug in the jinja renderer, please report it")
		}

		return "", errors.New(customError)
	}

	return out, nil
}

func findRenderErrorType(err error) string {
	message := err.Error()
	errorBits := strings.Split(message, ": ")
	innermostErr := errorBits[len(errorBits)-1]

	if strings.HasPrefix(innermostErr, "filter '") && strings.HasSuffix(innermostErr, "' not found") {
		return innermostErr
	} else if strings.HasPrefix(innermostErr, "Unable to evaluate name ") {
		match := missingVariableRegex.FindStringSubmatch(innermostErr)
		if len(match) <= 2 {
			return "missing variable '" + match[1] + "'"
		}

		return innermostErr
	}

	return ""
}

func findParserErrorType(err error) string {
	message := err.Error()

	if strings.Contains(message, "Unexpected EOF, expected tag else or endfor") {
		match := locationRegex.FindString(message)
		return "missing 'endfor' at " + match
	} else if strings.Contains(message, "Unexpected EOF, expected tag elif or else or endif") {
		match := locationRegex.FindString(message)
		return "missing end of the 'if' condition at " + match + ", did you forget to add 'endif'?"
	}

	return ""
}
