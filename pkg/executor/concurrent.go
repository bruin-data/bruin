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
		color.FgHiMagenta,
		color.FgHiBlue,
		color.FgHiCyan,
	}
	faint = color.New(color.Faint).SprintFunc()
)

type contextKey int

const (
	KeyPrinter contextKey = iota

	timeFormat = "2006-01-02 15:04:05"
)

type Concurrent struct {
	workerCount int
	workers     []*worker
}

func NewConcurrent(
	logger *zap.SugaredLogger,
	taskTypeMap map[pipeline.AssetType]Config,
	workerCount int,
) *Concurrent {
	executor := &Sequential{
		TaskTypeMap: taskTypeMap,
	}

	var printLock sync.Mutex

	workers := make([]*worker, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = &worker{
			id:        fmt.Sprintf("worker-%d", i),
			executor:  executor,
			logger:    logger,
			printer:   color.New(colors[i%len(colors)]),
			printLock: &printLock,
		}
	}

	return &Concurrent{
		workerCount: workerCount,
		workers:     workers,
	}
}

func (c Concurrent) Start(input chan scheduler.TaskInstance, result chan<- *scheduler.TaskExecutionResult) {
	for i := 0; i < c.workerCount; i++ {
		go c.workers[i].run(input, result)
	}
}

type worker struct {
	id        string
	executor  *Sequential
	logger    *zap.SugaredLogger
	printer   *color.Color
	printLock *sync.Mutex
}

func (w worker) run(taskChannel <-chan scheduler.TaskInstance, results chan<- *scheduler.TaskExecutionResult) {
	for task := range taskChannel {
		w.printLock.Lock()
		w.printer.Printf("[%s] Starting: %s\n", time.Now().Format(timeFormat), task.GetHumanID())
		w.printLock.Unlock()

		start := time.Now()

		printer := &workerWriter{
			w:           os.Stdout,
			task:        task.GetAsset(),
			sprintfFunc: w.printer.SprintfFunc(),
			worker:      w.id,
		}

		ctx := context.WithValue(context.Background(), KeyPrinter, printer)
		err := w.executor.RunSingleTask(ctx, task)

		duration := time.Since(start)
		durationString := fmt.Sprintf("(%s)", duration.Truncate(time.Millisecond).String())
		w.printLock.Lock()
		w.printer.Printf("[%s] Finished: %s %s\n", time.Now().Format(timeFormat), task.GetHumanID(), faint(durationString))
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
	formatted := w.sprintfFunc("[%s] [%s] %s", time.Now().Format(timeFormat), w.task.Name, string(p))

	n, err := w.w.Write([]byte(formatted))
	if err != nil {
		return n, err
	}
	if n != len(formatted) {
		return n, io.ErrShortWrite
	}
	return len(p), nil
}
