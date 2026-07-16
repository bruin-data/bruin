package executor

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type Operator interface {
	Run(ctx context.Context, ti scheduler.TaskInstance) error
}

type (
	OperatorMap map[pipeline.AssetType]Operator
)

type Sequential struct {
	TaskTypeMap map[pipeline.AssetType]Config
}

// AssetTimeoutError reports that an asset exceeded its configured timeout.
type AssetTimeoutError struct {
	AssetName string
	Timeout   time.Duration
}

func (e *AssetTimeoutError) Error() string {
	return fmt.Sprintf("asset %q timed out after %s", e.AssetName, e.Timeout)
}

func (e *AssetTimeoutError) Unwrap() error {
	return context.DeadlineExceeded
}

func (s Sequential) RunSingleTask(ctx context.Context, instance scheduler.TaskInstance) error {
	task := instance.GetAsset()
	fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
	if fullRefresh && task.RefreshRestricted != nil && *task.RefreshRestricted {
		ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)
		if instance.GetType() == scheduler.TaskInstanceTypeMain {
			if printer, ok := ctx.Value(KeyPrinter).(io.Writer); ok {
				_, _ = fmt.Fprintf(printer, "Warning: full refresh is restricted for asset %q; running incrementally.\n", task.Name)
			}
		}
	}

	// check if task type exists in map
	executors, ok := s.TaskTypeMap[task.Type]
	if !ok {
		return errors.New("there is no executor configured for the task type, task cannot be run: " + string(task.Type))
	}

	executor, ok := executors[instance.GetType()]
	if !ok {
		return errors.New("there is no executor configured for the asset class: " + instance.GetType().String())
	}

	if task.Timeout == 0 {
		return executor.Run(ctx, instance)
	}

	timeoutErr := &AssetTimeoutError{
		AssetName: task.Name,
		Timeout:   task.Timeout.Duration(),
	}
	timeoutCtx, cancel := context.WithTimeoutCause(ctx, timeoutErr.Timeout, timeoutErr)
	defer cancel()

	err := executor.Run(timeoutCtx, instance)
	if errors.Is(context.Cause(timeoutCtx), timeoutErr) {
		return timeoutErr
	}

	return err
}
