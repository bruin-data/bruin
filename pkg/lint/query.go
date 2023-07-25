package lint

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"go.uber.org/zap"
)

type queryValidator interface {
	IsValid(ctx context.Context, query *query.Query) (bool, error)
}

type connectionManager interface {
	GetConnection(conn string) (interface{}, error)
}

type queryExtractor interface {
	ExtractQueriesFromFile(filepath string) ([]*query.Query, error)
}

type QueryValidatorRule struct {
	Identifier  string
	TaskType    pipeline.AssetType
	Connections connectionManager
	Extractor   queryExtractor
	WorkerCount int
	Logger      *zap.SugaredLogger
}

func (q QueryValidatorRule) Name() string {
	return q.Identifier
}

func (q QueryValidatorRule) validateTask(p *pipeline.Pipeline, task *pipeline.Asset, done chan<- []*Issue) {
	issues := make([]*Issue, 0)

	queries, err := q.Extractor.ExtractQueriesFromFile(task.ExecutableFile.Path)
	if err != nil {
		issues = append(issues, &Issue{
			Task:        task,
			Description: fmt.Sprintf("Cannot read executable file '%s': %+v", task.ExecutableFile.Path, err),
		})

		done <- issues
		return
	}

	q.Logger.Debugf("Found %d queries in file '%s'", len(queries), task.ExecutableFile.Path)

	if len(queries) == 0 {
		issues = append(issues, &Issue{
			Task:        task,
			Description: fmt.Sprintf("No queries found in executable file '%s'", task.ExecutableFile.Path),
		})

		done <- issues
		return
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	for index, foundQuery := range queries {
		wg.Add(1)
		go func(index int, foundQuery *query.Query) {
			defer wg.Done()

			q.Logger.Debugw("Checking if a query is valid", "path", task.ExecutableFile.Path)
			start := time.Now()

			assetConnectionName := p.GetConnectionNameForAsset(task)
			q.Logger.Debugw("The connection will be used for asset", "asset", task.Name, "connection", assetConnectionName)

			validator, err := q.Connections.GetConnection(assetConnectionName)
			if err != nil {
				mu.Lock()
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Cannot get connection for task '%s': %+v", task.Name, err),
				})
				mu.Unlock()

				return
			}
			valll, ok := validator.(queryValidator)
			if !ok {
				mu.Lock()
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Query validator '%s' is not a valid instance", assetConnectionName),
				})
				mu.Unlock()

				return
			}
			valid, err := valll.IsValid(context.Background(), foundQuery)
			if err != nil {
				mu.Lock()
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Invalid query found at index %d: %s", index, err),
					Context: []string{
						"The failing query is as follows:",
						foundQuery.Query,
					},
				})
				mu.Unlock()
			} else if !valid {
				mu.Lock()
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Query '%s' is invalid", foundQuery.Query),
					Context: []string{
						"The failing query is as follows:",
						foundQuery.Query,
					},
				})
				mu.Unlock()
			}

			duration := time.Since(start)
			q.Logger.Debugw("Finished with query checking", "path", task.ExecutableFile.Path, "duration", duration)
		}(index, foundQuery)
	}

	wg.Wait()
	done <- issues
}

func (q QueryValidatorRule) bufferSize() int {
	return 256
}

func (q *QueryValidatorRule) Validate(p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	// skip if there are no workers defined
	if q.WorkerCount == 0 {
		return issues, nil
	}

	q.Logger.Debugf("Starting validation with %d workers for task type '%s'", q.WorkerCount, q.TaskType)

	taskChannel := make(chan *pipeline.Asset, q.bufferSize())
	results := make(chan []*Issue, q.bufferSize())

	// start the workers
	for i := 0; i < q.WorkerCount; i++ {
		go func(taskChannel <-chan *pipeline.Asset, results chan<- []*Issue) {
			for task := range taskChannel {
				q.validateTask(p, task, results)
			}
		}(taskChannel, results)
	}

	processedTaskCount := 0
	for _, task := range p.Tasks {
		if task.Type != q.TaskType {
			q.Logger.Debug("Skipping task, task type not matched")
			continue
		}
		q.Logger.Debug("Processing task type")

		processedTaskCount++
		taskChannel <- task
		q.Logger.Debugf("Pushed a task to the taskChannel")
	}
	q.Logger.Debugf("Processed %d tasks at path '%s', closing channel", processedTaskCount, p.DefinitionFile.Path)
	close(taskChannel)
	q.Logger.Debugf("Closed the channel")

	for i := 0; i < processedTaskCount; i++ {
		foundIssues := <-results
		q.Logger.Debugf("Received issues: %d/%d", i+1, processedTaskCount)
		issues = append(issues, foundIssues...)
	}

	return issues, nil
}
