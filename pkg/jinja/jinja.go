package jinja

import (
	"sync"
	"time"

	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"github.com/pkg/errors"
)

type Renderer struct {
	context         *exec.Context
	queryRenderLock *sync.Mutex
}

type Context map[string]any

func NewRenderer(context Context) *Renderer {
	return &Renderer{
		context:         exec.NewContext(context),
		queryRenderLock: &sync.Mutex{},
	}
}

func PythonEnvVariables(startDate, endDate *time.Time, runID string) map[string]string {
	return map[string]string{
		"BRUIN_START_DATE":        startDate.Format("2006-01-02"),
		"BRUIN_START_DATE_NODASH": startDate.Format("20060102"),
		"BRUIN_START_DATETIME":    startDate.Format("2006-01-02T15:04:05"),
		"BRUIN_START_TIMESTAMP":   startDate.Format("2006-01-02T15:04:05.000000Z07:00"),
		"BRUIN_END_DATE":          endDate.Format("2006-01-02"),
		"BRUIN_END_DATE_NODASH":   endDate.Format("20060102"),
		"BRUIN_END_DATETIME":      endDate.Format("2006-01-02T15:04:05"),
		"BRUIN_END_TIMESTAMP":     endDate.Format("2006-01-02T15:04:05.000000Z07:00"),
		"BRUIN_RUN_ID":            runID,
	}
}

func NewRendererWithStartEndDates(startDate, endDate *time.Time) *Renderer {
	ctx := exec.NewContext(map[string]any{
		"start_date":        startDate.Format("2006-01-02"),
		"start_date_nodash": startDate.Format("20060102"),
		"start_datetime":    startDate.Format("2006-01-02T15:04:05"),
		"start_timestamp":   startDate.Format("2006-01-02T15:04:05.000000Z07:00"),
		"end_date":          endDate.Format("2006-01-02"),
		"end_date_nodash":   endDate.Format("20060102"),
		"end_datetime":      endDate.Format("2006-01-02T15:04:05"),
		"end_timestamp":     endDate.Format("2006-01-02T15:04:05.000000Z07:00"),
	})

	return &Renderer{
		context:         ctx,
		queryRenderLock: &sync.Mutex{},
	}
}

func NewRendererWithYesterday() *Renderer {
	yesterday := time.Now().AddDate(0, 0, -1)
	startDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	endDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, time.UTC)
	return NewRendererWithStartEndDates(&startDate, &endDate)
}

func (r *Renderer) Render(query string) (string, error) {
	r.queryRenderLock.Lock()

	tpl, err := gonja.FromString(query)
	if err != nil {
		r.queryRenderLock.Unlock()
		return "", errors.Wrap(err, "you have found a bug in the jinja parser, please report it")
	}
	r.queryRenderLock.Unlock()

	// Now you can render the template with the given
	// gonja.context how often you want to.
	out, err := tpl.ExecuteToString(r.context)
	if err != nil {
		return "", errors.Wrap(err, "you have found a bug in the jinja renderer, please report it")
	}

	return out, nil
}
