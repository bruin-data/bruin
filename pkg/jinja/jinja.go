package jinja

import (
	"sync"
	"time"

	"github.com/noirbizarre/gonja"
	"github.com/pkg/errors"
)

type Renderer struct {
	context         gonja.Context
	queryRenderLock *sync.Mutex
}

type Context map[string]any

func NewRenderer(context Context) *Renderer {
	return &Renderer{
		context:         gonja.Context(context),
		queryRenderLock: &sync.Mutex{},
	}
}

func NewRendererWithStartEndDates(startDate, endDate *time.Time) *Renderer {
	ctx := gonja.Context{
		"start_date":             startDate.Format("2006-01-02"),
		"start_date_nodash":      startDate.Format("20060102"),
		"start_datetime":         startDate.Format("2006-01-02T15:04:05"),
		"start_datetime_with_tz": startDate.Format(time.RFC3339),
		"end_date":               endDate.Format("2006-01-02"),
		"end_date_nodash":        endDate.Format("20060102"),
		"end_datetime":           endDate.Format("2006-01-02T15:04:05"),
		"end_datetime_with_tz":   endDate.Format(time.RFC3339),

		"utils": map[string]interface{}{
			"date_add":    dateAdd,
			"date_format": dateFormat,
		},
	}

	cfg := gonja.NewConfig()
	cfg.KeepTrailingNewline = true
	env := gonja.NewEnvironment(cfg, loader)

	return &Renderer{
		context:         ctx,
		queryRenderLock: &sync.Mutex{},
	}
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
	out, err := tpl.Execute(r.context)
	if err != nil {
		return "", errors.Wrap(err, "you have found a bug in the jinja renderer, please report it")
	}

	return out, nil
}
