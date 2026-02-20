package lint

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
)

type queryValidator interface {
	IsValid(ctx context.Context, query *query.Query) (bool, error)
}

type connectionManager interface {
	GetConnection(conn string) any
}

type queryExtractor interface {
	ExtractQueriesFromString(content string) ([]*query.Query, error)
	CloneForAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) (query.QueryExtractor, error)
}

type materializer interface {
	Render(task *pipeline.Asset, query string) (string, error)
}

type QueryValidatorRule struct {
	Identifier   string
	Fast         bool
	TaskType     pipeline.AssetType
	Connections  connectionManager
	Extractor    queryExtractor
	Materializer materializer
	WorkerCount  int
	Logger       logger.Logger
}

func (q *QueryValidatorRule) Name() string {
	return q.Identifier
}

func (q *QueryValidatorRule) IsFast() bool {
	return false
}

func (q *QueryValidatorRule) GetApplicableLevels() []Level {
	// we need to implement this rule for asset level as well
	return []Level{LevelPipeline, LevelAsset}
}

func (q *QueryValidatorRule) GetSeverity() ValidatorSeverity {
	return ValidatorSeverityCritical
}

func (q *QueryValidatorRule) ValidateAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) ([]*Issue, error) {
	issues := make([]*Issue, 0)
	if asset.Type != q.TaskType {
		q.Logger.Debug("Skipping task, task type not matched")
		return issues, nil
	}

	extractor, err := q.Extractor.CloneForAsset(ctx, p, asset)
	if err != nil {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("failed to extract queries from asset '%s': %s", asset.Name, err),
			Context: []string{
				err.Error(),
			},
		})
		return issues, nil
	}

	queries, err := extractor.ExtractQueriesFromString(asset.ExecutableFile.Content)
	if err != nil {
		q.Logger.Debugf("failed to extract the queries from pipeline '%s' task '%s'", p.Name, asset.Name)
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Cannot read executable file '%s'", asset.ExecutableFile.Path),
			Context: []string{
				err.Error(),
			},
		})

		return issues, nil //nolint:nilerr
	}

	if len(queries) == 0 {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("No queries found in executable file '%s'", asset.ExecutableFile.Path),
		})

		return issues, nil
	} else if len(queries) > 1 {
		// this has to be the case until we find a nicer solution for no-dry-run databases
		// remember that BigQuery or others that support dry-run already return a single query as part of the queryExtractor implementations
		// which means the only scenario where there are multiple queries returned is the no-dry-run databases
		q.Logger.Warnf("Found %d queries in file '%s', we cannot validate script assets, skipping query validation", len(queries), asset.ExecutableFile.Path)
		return issues, nil
	}

	foundQuery := queries[0]

	if q.Materializer != nil {
		materialized, err := q.Materializer.Render(asset, foundQuery.Query)
		if err != nil {
			issues = append(issues, &Issue{
				Task:        asset,
				Description: fmt.Sprintf("Failed to materialize query: %s", err),
			})
		}
		foundQuery.Query = materialized
	}

	assetConnectionName, err := p.GetConnectionNameForAsset(asset)
	if err != nil {
		q.Logger.Debugf("failed to get connection name for asset '%s'", asset.Name)
		return issues, nil //nolint:nilerr
	}

	q.Logger.Debugw("The connection will be used for asset", "asset", asset.Name, "connection", assetConnectionName)

	validator := q.Connections.GetConnection(assetConnectionName)
	if validator == nil {
		q.Logger.Debugf("failed to get connection instance for asset '%s'", asset.Name)
		return issues, nil //nolint:nilerr
	}
	validatorInstance, ok := validator.(queryValidator)
	if !ok {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Query validator '%s' is not a valid instance", assetConnectionName),
		})

		return issues, nil
	}
	valid, err := validatorInstance.IsValid(ctx, foundQuery)
	if err != nil {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: fmt.Sprintf("Failed to validate query: %s", err),
		})
	} else if !valid {
		issues = append(issues, &Issue{
			Task:        asset,
			Description: "Query is invalid: " + foundQuery.Query,
		})
	}

	return issues, nil
}

func (q *QueryValidatorRule) validateTask(ctx context.Context, p *pipeline.Pipeline, task *pipeline.Asset, done chan<- []*Issue) {
	issues := make([]*Issue, 0)

	extractor, err := q.Extractor.CloneForAsset(ctx, p, task)
	if err != nil {
		issues = append(issues, &Issue{
			Task:        task,
			Description: fmt.Sprintf("failed to extract queries from asset '%s': %s", task.Name, err),
			Context: []string{
				err.Error(),
			},
		})
		done <- issues
		return
	}
	q.Logger.Debugf("validating pipeline '%s' task '%s'", p.Name, task.Name)
	queries, err := extractor.ExtractQueriesFromString(task.ExecutableFile.Content)
	q.Logger.Debugf("got the query extract results from file for pipeline '%s' task '%s'", p.Name, task.Name)

	if err != nil {
		q.Logger.Debugf("failed to extract the queries from pipeline '%s' task '%s'", p.Name, task.Name)
		issues = append(issues, &Issue{
			Task:        task,
			Description: fmt.Sprintf("Cannot read executable file '%s'", task.ExecutableFile.Path),
			Context: []string{
				err.Error(),
			},
		})

		done <- issues
		q.Logger.Debugf("pushed the error for failed to extract the queries from pipeline '%s' task '%s'", p.Name, task.Name)
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
	} else if len(queries) > 1 {
		// this has to be the case until we find a nicer solution for no-dry-run databases
		q.Logger.Warnf("Found %d queries in file '%s', we cannot validate script assets, skipping query validation", len(queries), task.ExecutableFile.Path)
		done <- issues
		return
	}

	var mu sync.Mutex
	var wg sync.WaitGroup

	q.Logger.Debugf("Going into the query loop for asset '%s'", task.Name)

	for index, foundQuery := range queries {
		wg.Add(1)
		q.Logger.Debugf("Spawning the %dth index validation for asset '%s'", index, task.Name)
		go func(foundQuery *query.Query) {
			defer wg.Done()

			q.Logger.Debugw("Checking if a query is valid", "path", task.ExecutableFile.Path)
			start := time.Now()

			if q.Materializer != nil {
				materialized, err := q.Materializer.Render(task, foundQuery.Query)
				if err != nil {
					issues = append(issues, &Issue{
						Task:        task,
						Description: fmt.Sprintf("Failed to materialize query: %s", err),
					})
				}
				foundQuery.Query = materialized
			}

			assetConnectionName, err := p.GetConnectionNameForAsset(task)
			if err != nil {
				q.Logger.Debugf("failed to get connection name for asset '%s'", task.Name)
				return
			}

			q.Logger.Debugw("The connection will be used for asset", "asset", task.Name, "connection", assetConnectionName)

			validator := q.Connections.GetConnection(assetConnectionName)
			if validator == nil {
				q.Logger.Debugf("failed to get connection instance for asset '%s'", task.Name)
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
			valid, err := valll.IsValid(ctx, foundQuery)
			if err != nil {
				mu.Lock()
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Invalid query found: %s", err),
				})
				mu.Unlock()
			} else if !valid {
				mu.Lock()
				issues = append(issues, &Issue{
					Task:        task,
					Description: fmt.Sprintf("Query '%s' is invalid", foundQuery.Query),
				})
				mu.Unlock()
			}

			duration := time.Since(start)
			q.Logger.Debugw("Finished with query checking", "path", task.ExecutableFile.Path, "duration", duration)
		}(foundQuery)
	}
	q.Logger.Debugf("Waiting for all workers to finish for task '%s'", task.Name)
	wg.Wait()
	q.Logger.Debugf("Workers finished for task '%s'", task.Name)
	done <- issues
	q.Logger.Debugf("Pushed issues to the done channel for task '%s'", task.Name)
}

func (q *QueryValidatorRule) bufferSize() int {
	return 256
}

func (q *QueryValidatorRule) Validate(ctx context.Context, p *pipeline.Pipeline) ([]*Issue, error) {
	issues := make([]*Issue, 0)

	excludeTag, ok := ctx.Value(excludeTagKey).(string)
	if !ok {
		excludeTag = ""
	}

	// skip if there are no workers defined
	if q.WorkerCount == 0 {
		return issues, nil
	}

	q.Logger.Debugf("Starting validation with %d workers for task type '%s'", q.WorkerCount, q.TaskType)

	taskChannel := make(chan *pipeline.Asset, q.bufferSize())
	results := make(chan []*Issue, q.bufferSize())

	// start the workers
	for range q.WorkerCount {
		go func(taskChannel <-chan *pipeline.Asset, results chan<- []*Issue) {
			for task := range taskChannel {
				q.validateTask(ctx, p, task, results)
			}
		}(taskChannel, results)
	}

	processedTaskCount := 0
	for _, task := range p.Assets {
		if task.Type != q.TaskType {
			q.Logger.Debug("Skipping task, task type not matched")
			continue
		}
		if ContainsTag(task.Tags, excludeTag) {
			continue
		}
		q.Logger.Debugf("Processing task type: %s", task.Type)

		processedTaskCount++
		taskChannel <- task
		q.Logger.Debugf("Pushed a task to the taskChannel")
	}
	q.Logger.Debugf("Processed %d tasks at path '%s', closing channel", processedTaskCount, p.DefinitionFile.Path)

	for i := range processedTaskCount {
		q.Logger.Debugf("Waiting for results for i = %d", i)
		foundIssues := <-results
		q.Logger.Debugf("Received issues: %d/%d", i+1, processedTaskCount)
		issues = append(issues, foundIssues...)
	}

	close(taskChannel)
	q.Logger.Debugf("Closed the channel")

	return issues, nil
}

func (q *QueryValidatorRule) ValidateCrossPipeline(ctx context.Context, pipelines []*pipeline.Pipeline) ([]*Issue, error) {
	// This rule doesn't need cross-pipeline validation
	return []*Issue{}, nil
}
