package jinja

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/date"

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
		"PYTHONUNBUFFERED":      "1",
	}

	if fullRefresh {
		vars["BRUIN_FULL_REFRESH"] = "1"
	}

	return vars
}

func NewRendererWithStartEndDates(startDate, endDate *time.Time, pipelineName, runID string, vars Context) *Renderer {
	ctx := defaultContext(startDate, endDate, pipelineName, runID)
	ctx["var"] = vars
	return &Renderer{
		context:         exec.NewContext(ctx),
		queryRenderLock: &sync.Mutex{},
	}
}

func defaultContext(startDate, endDate *time.Time, pipelineName, runID string) map[string]any {
	return map[string]any{
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
	}
}

func NewRendererWithYesterday(pipelineName, runID string) *Renderer {
	yesterday := time.Now().AddDate(0, 0, -1)
	startDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	endDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, time.UTC)
	return NewRendererWithStartEndDates(&startDate, &endDate, pipelineName, runID, nil)
}

func (r *Renderer) Render(query string) (string, error) {
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

//nolint:ireturn
func (r *Renderer) CloneForAsset(ctx context.Context, pipe *pipeline.Pipeline, asset *pipeline.Asset) RendererInterface {
	startDate, ok := ctx.Value(pipeline.RunConfigStartDate).(time.Time)
	if !ok {
		return r
	}

	endDate, ok := ctx.Value(pipeline.RunConfigEndDate).(time.Time)
	if !ok {
		return r
	}

	fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
	if fullRefresh {
		if asset.StartDate != "" {
			if parsed, err := date.ParseTime(asset.StartDate); err == nil {
				startDate = parsed
			}
		} else if pipe.StartDate != "" {
			if parsed, err := date.ParseTime(pipe.StartDate); err == nil {
				startDate = parsed
			}
		}
	}

	applyModifiers, ok := ctx.Value(pipeline.RunConfigApplyIntervalModifiers).(bool)
	if ok && applyModifiers && !fullRefresh {
		startDate = pipeline.ModifyDate(startDate, asset.IntervalModifiers.Start)
		endDate = pipeline.ModifyDate(endDate, asset.IntervalModifiers.End)
	}

	jinjaContext := defaultContext(&startDate, &endDate, pipe.Name, ctx.Value(pipeline.RunConfigRunID).(string))
	jinjaContext["this"] = asset.Name
	jinjaContext["var"] = pipe.Variables.Value()

	return &Renderer{
		context:         exec.NewContext(jinjaContext),
		queryRenderLock: &sync.Mutex{},
	}
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

// this ugly interface is needed to avoid circular dependencies and the ability to create different renderer instances per asset.
type RendererInterface interface {
	Render(query string) (string, error)
	CloneForAsset(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) RendererInterface
}
