package jinja

import (
	"context"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/nikolalohinski/gonja/v2"
	"github.com/nikolalohinski/gonja/v2/exec"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type Renderer struct {
	context         *exec.Context
	queryRenderLock *sync.Mutex
	macroContent    string
}

func init() { //nolint: gochecknoinits
	gonja.DefaultConfig.StrictUndefined = true
}

var (
	missingVariableRegex = regexp.MustCompile(`name\s+"([^"]+)"`)
	locationRegex        = regexp.MustCompile(`\(Line: \d+ Col: \d+, near ".*?"\)`)
)

type Context map[string]any

// LoadMacros loads all macro files from the given directory and returns them as a single string.
func LoadMacros(fs afero.Fs, macrosPath string) (string, error) {
	exists, err := afero.DirExists(fs, macrosPath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to check if macros directory exists: %s", macrosPath)
	}
	if !exists {
		return "", nil
	}

	entries, err := afero.ReadDir(fs, macrosPath)
	if err != nil {
		return "", errors.Wrapf(err, "failed to read macros directory: %s", macrosPath)
	}

	var macroContent strings.Builder
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}

		filePath := filepath.Join(macrosPath, entry.Name())
		content, err := afero.ReadFile(fs, filePath)
		if err != nil {
			return "", errors.Wrapf(err, "failed to read macro file: %s", filePath)
		}

		macroContent.Write(content)
		macroContent.WriteString("\n")
	}

	return macroContent.String(), nil
}

func NewRenderer(context Context) *Renderer {
	return &Renderer{
		context:         exec.NewContext(context),
		queryRenderLock: &sync.Mutex{},
		macroContent:    "",
	}
}

// NewRendererWithMacros creates a new Renderer with the given context and macro content.
func NewRendererWithMacros(context Context, macroContent string) *Renderer {
	return &Renderer{
		context:         exec.NewContext(context),
		queryRenderLock: &sync.Mutex{},
		macroContent:    macroContent,
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
	ctx := defaultContext(startDate, endDate, pipelineName, runID, false)
	ctx["var"] = vars
	return &Renderer{
		context:         exec.NewContext(ctx),
		queryRenderLock: &sync.Mutex{},
		macroContent:    "",
	}
}

// NewRendererWithStartEndDatesAndMacros creates a new Renderer with the given dates, context, and macro content.
func NewRendererWithStartEndDatesAndMacros(startDate, endDate *time.Time, pipelineName, runID string, vars Context, macroContent string) *Renderer {
	ctx := defaultContext(startDate, endDate, pipelineName, runID, false)
	ctx["var"] = vars
	return &Renderer{
		context:         exec.NewContext(ctx),
		queryRenderLock: &sync.Mutex{},
		macroContent:    macroContent,
	}
}

func defaultContext(startDate, endDate *time.Time, pipelineName, runID string, fullRefresh bool) map[string]any {
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
		"full_refresh":      fullRefresh,
	}
}

func NewRendererWithYesterday(pipelineName, runID string) *Renderer {
	yesterday := time.Now().AddDate(0, 0, -1)
	startDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 0, 0, 0, 0, time.UTC)
	endDate := time.Date(yesterday.Year(), yesterday.Month(), yesterday.Day(), 23, 59, 59, 999999999, time.UTC)
	ctx := defaultContext(&startDate, &endDate, pipelineName, runID, false)
	ctx["var"] = nil
	return &Renderer{
		context:         exec.NewContext(ctx),
		queryRenderLock: &sync.Mutex{},
	}
}

func (r *Renderer) Render(query string) (string, error) {
	r.queryRenderLock.Lock()

	// Prepend macro content to the query if macros are loaded
	fullQuery := query
	if r.macroContent != "" {
		fullQuery = r.macroContent + "\n" + query
	}

	tpl, err := gonja.FromString(fullQuery)
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

	if r.macroContent != "" {
		out = cleanupExcessiveNewlines(out)
	}

	return out, nil
}

func cleanupExcessiveNewlines(s string) string {
	lines := strings.Split(s, "\n")
	var result []string
	consecutiveEmpty := 0

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			consecutiveEmpty++
			if consecutiveEmpty <= 2 {
				result = append(result, line)
			}
		} else {
			consecutiveEmpty = 0
			result = append(result, line)
		}
	}

	return strings.Join(result, "\n")
}

//nolint:ireturn
func (r *Renderer) CloneForAsset(ctx context.Context, pipe *pipeline.Pipeline, asset *pipeline.Asset) (RendererInterface, error) {
	startDate, ok := ctx.Value(pipeline.RunConfigStartDate).(time.Time)
	if !ok {
		return r, nil
	}

	endDate, ok := ctx.Value(pipeline.RunConfigEndDate).(time.Time)
	if !ok {
		return r, nil
	}

	fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)

	applyModifiers, ok := ctx.Value(pipeline.RunConfigApplyIntervalModifiers).(bool)
	if ok && applyModifiers {
		tempContext := defaultContext(&startDate, &endDate, pipe.Name, ctx.Value(pipeline.RunConfigRunID).(string), fullRefresh)
		tempContext["this"] = asset.Name
		tempContext["var"] = pipe.Variables.Value()
		tempRenderer := &Renderer{
			context:         exec.NewContext(tempContext),
			queryRenderLock: &sync.Mutex{},
		}
		// Use non-mutating template resolution to avoid modifying the original asset
		resolvedStartModifier, err := asset.IntervalModifiers.Start.ResolveTemplateToNew(tempRenderer)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to resolve start interval modifier template for asset %s", asset.Name)
		}
		startDate = pipeline.ModifyDate(startDate, resolvedStartModifier)

		resolvedEndModifier, err := asset.IntervalModifiers.End.ResolveTemplateToNew(tempRenderer)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to resolve end interval modifier template for asset %s", asset.Name)
		}
		endDate = pipeline.ModifyDate(endDate, resolvedEndModifier)
	}

	jinjaContext := defaultContext(&startDate, &endDate, pipe.Name, ctx.Value(pipeline.RunConfigRunID).(string), fullRefresh)
	jinjaContext["this"] = asset.Name
	jinjaContext["var"] = pipe.Variables.Value()

	return &Renderer{
		context:         exec.NewContext(jinjaContext),
		queryRenderLock: &sync.Mutex{},
		macroContent:    r.macroContent, // Preserve macro content when cloning
	}, nil
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
	CloneForAsset(ctx context.Context, pipeline *pipeline.Pipeline, asset *pipeline.Asset) (RendererInterface, error)
}
