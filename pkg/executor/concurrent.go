package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/fatih/color"
	"go.uber.org/zap"
)

var (
	colors = []color.Attribute{
		color.FgBlue,
		color.FgMagenta,
		color.FgCyan,
		color.FgWhite,
		color.FgGreen + color.Faint,
		color.FgYellow,
	}
	faint        = color.New(color.Faint).SprintFunc()
	whitePrinter = color.New(color.FgWhite, color.Faint).SprintfFunc()
)

type contextKey int

const (
	KeyPrinter contextKey = iota
	KeyIsDebug contextKey = iota
	ContextLogger

	timeFormat = "2006-01-02 15:04:05"
)

type Concurrent struct {
	workerCount int
	workers     []*worker
}

func NewConcurrent(
	logger *zap.SugaredLogger,
	taskTypeMap map[pipeline.AssetType]Config,
	workerCount int, doNotLogTimeStamp bool,
) (*Concurrent, error) {
	executor := &Sequential{
		TaskTypeMap: taskTypeMap,
	}

	var printLock sync.Mutex

	workers := make([]*worker, workerCount)
	for i := range workerCount {
		workers[i] = &worker{
			id:                fmt.Sprintf("worker-%d", i),
			executor:          executor,
			logger:            logger,
			printer:           color.New(colors[i%len(colors)]),
			printLock:         &printLock,
			doNotLogTimestamp: doNotLogTimeStamp,
		}
	}

	return &Concurrent{
		workerCount: workerCount,
		workers:     workers,
	}, nil
}

func (c Concurrent) Start(ctx context.Context, input chan scheduler.TaskInstance, result chan<- *scheduler.TaskExecutionResult) {
	for i := range c.workerCount {
		go c.workers[i].run(ctx, input, result)
	}
}

type worker struct {
	id                string
	executor          *Sequential
	logger            *zap.SugaredLogger
	printer           *color.Color
	printLock         *sync.Mutex
	doNotLogTimestamp bool
}

func (w worker) run(ctx context.Context, taskChannel <-chan scheduler.TaskInstance, results chan<- *scheduler.TaskExecutionResult) {
	for task := range taskChannel {
		w.printLock.Lock()

		timestampStr := whitePrinter("[%s]", time.Now().Format(timeFormat))
		if w.doNotLogTimestamp {
			fmt.Printf("%s\n", w.printer.Sprintf("Running:  %s", task.GetHumanID()))
		} else {
			fmt.Printf("%s %s\n", timestampStr, w.printer.Sprintf("Running:  %s", task.GetHumanID()))
		}
		w.printLock.Unlock()

		start := time.Now()

		printer := &workerWriter{
			w:           os.Stdout,
			task:        task.GetAsset(),
			sprintfFunc: w.printer.SprintfFunc(),
			worker:      w.id,
		}

		executionCtx := context.WithValue(ctx, KeyPrinter, printer)
		executionCtx = context.WithValue(executionCtx, ContextLogger, w.logger)
		err := w.executor.RunSingleTask(executionCtx, task)

		duration := time.Since(start)
		durationString := fmt.Sprintf("(%s)", duration.Truncate(time.Millisecond).String())
		w.printLock.Lock()

		res := "Finished"
		if err != nil {
			res = "Failed"
		}
		if w.doNotLogTimestamp {
			fmt.Printf("%s\n", w.printer.Sprintf("%s: %s %s", res, task.GetHumanID(), faint(durationString)))
		} else {
			timestampStr = whitePrinter("[%s]", time.Now().Format(timeFormat))
			fmt.Printf("%s %s\n", timestampStr, w.printer.Sprintf("%s: %s %s", res, task.GetHumanID(), faint(durationString)))
		}
		w.printLock.Unlock()
		results <- &scheduler.TaskExecutionResult{
			Instance: task,
			Error:    err,
		}
	}
}

type workerWriter struct {
	w           io.Writer
	task        *pipeline.Asset
	sprintfFunc func(format string, a ...interface{}) string
	worker      string
}

func (w *workerWriter) Write(p []byte) (int, error) {
	timestampStr := whitePrinter("[%s]", time.Now().Format(timeFormat))
	formatted := fmt.Sprintf("%s %s", timestampStr, w.sprintfFunc("[%s] %s", w.task.Name, string(p)))

	n, err := w.w.Write([]byte(formatted))
	if err != nil {
		return n, err
	}
	if n != len(formatted) {
		return n, io.ErrShortWrite
	}
	return len(p), nil
}
