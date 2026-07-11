package executor

import (
	"context"
	"fmt"
	"io"

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

	return executor.Run(ctx, instance)
}
