package env

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/bruin-data/bruin/pkg/date"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// SetupVariables prepares the environment variables for a pipeline run.
// It is meant for use in python operators.
func SetupVariables(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset, env map[string]string) (map[string]string, error) {
	env, err := envMutateIntervals(ctx, p, t, env)
	if err != nil {
		return nil, err
	}

	env, err = envInjectVariables(env, p.Variables.Value())
	if err != nil {
		return nil, err
	}

	return env, nil
}

func envMutateIntervals(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset, env map[string]string) (map[string]string, error) {
	if val := ctx.Value(pipeline.RunConfigApplyIntervalModifiers); val != nil {
		if apply, ok := val.(bool); !ok || !apply {
			return env, nil
		}
	}
	startDate, ok := ctx.Value(pipeline.RunConfigStartDate).(time.Time)
	if !ok {
		return nil, errors.New("start date is required - please provide a valid date")
	}

	endDate, ok := ctx.Value(pipeline.RunConfigEndDate).(time.Time)
	if !ok {
		return nil, errors.New("end date is required - please provide a valid date")
	}
	runID, ok := ctx.Value(pipeline.RunConfigRunID).(string)
	if !ok {
		return nil, errors.New("run ID not found - please check if the run exists")
	}
	fullRefresh, ok := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
	if !ok {
		return nil, errors.New("invalid or missing full refresh setting - must be true or false")
	}

	if fullRefresh {
		if t.StartDate != "" {
			if parsed, err := date.ParseTime(t.StartDate); err == nil {
				startDate = parsed
			}
		} else if p.StartDate != "" {
			if parsed, err := date.ParseTime(p.StartDate); err == nil {
				startDate = parsed
			}
		}
	} else {
		startDate = pipeline.ModifyDate(startDate, t.IntervalModifiers.Start)
		endDate = pipeline.ModifyDate(endDate, t.IntervalModifiers.End)
	}

	modifiedStartDate := startDate
	modifiedEndDate := endDate

	return jinja.PythonEnvVariables(&modifiedStartDate, &modifiedEndDate, p.Name, runID, fullRefresh), nil
}

func envInjectVariables(env map[string]string, variables map[string]any) (map[string]string, error) {
	if len(variables) == 0 {
		return env, nil
	}
	doc, err := json.Marshal(variables)
	if err != nil {
		return nil, fmt.Errorf("error marshalling variables to JSON: %w", err)
	}
	env["BRUIN_VARS"] = string(doc)
	return env, nil
}
