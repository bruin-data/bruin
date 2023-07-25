package query

import (
	"regexp"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/jinja"
)

var DefaultJinjaRenderer = jinja.NewRenderer(jinja.Context{
	"ds":                  time.Now().Format("2006-01-02"),
	"ds_nodash":           time.Now().Format("20060102"),
	"data_interval_start": time.Now().AddDate(0, 0, -1).Format(time.RFC3339),
	"data_interval_end":   time.Now().Format(time.RFC3339),
	"utils": map[string]interface{}{
		"date_add": func(str string, days int) string {
			return str
		},
		"date_format": func(str, inputFormat, outputFormat string) string {
			return str
		},
	},
})

type Renderer struct {
	Args map[string]string
}

var reIdentifiers = regexp.MustCompile(`(?s){{(([^}][^}]?|[^}]}?)*)}}`)

func (r Renderer) Render(query string) string {
	matchedVariables := reIdentifiers.FindAllString(query, -1)
	if len(matchedVariables) == 0 {
		return query
	}

	for _, variable := range matchedVariables {
		referencedRenderVariable := strings.Trim(variable[2:len(variable)-2], " ")
		if value, ok := r.Args[referencedRenderVariable]; ok {
			query = strings.ReplaceAll(query, variable, value)
		}
	}

	return query
}
