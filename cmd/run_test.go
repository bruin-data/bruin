package cmd

import (
	"fmt"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"go.uber.org/zap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClean(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		s    string
		want string
	}{
		{
			name: "empty",
			s:    "\u001B[0m\u001B[94m[2023-12-05 19:11:25] [hello_python] >> [2023-12-05 19:11:25 - INFO] Starting extractor: gcs_bucket_files",
			want: "[2023-12-05 19:11:25] [hello_python] >> [2023-12-05 19:11:25 - INFO] Starting extractor: gcs_bucket_files",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tt.want, Clean(tt.s), "Clean(%v)", tt.s)
		})
	}
}

func BenchmarkClean(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Clean("\u001B[0m\u001B[94m[2023-12-05 19:11:25] [hello_python] >> [2023-12-05 19:11:25 - INFO] Starting extractor: gcs_bucket_files")
		Clean("\u001B[0m\u001B[94m[2023-12-05 19:11:26] [hello_python] >>   File \"/Users/burak/.bruin/virtualenvs/77ef9663d804ac96afe6fb2a10d2b2b817a07fd82875759af8910b4fe31a7149/lib/python3.9/site-packages/google/api_core/page_iterator.py\", line 208, in _items_iter")
	}
}

func printMarkedTasks(tasks []scheduler.TaskInstance) {
	fmt.Println("Marked Tasks:")
	for _, task := range tasks {
		fmt.Printf(" d:Task Name: %s, Status: %s\n", task.GetAsset().Name, task.GetStatus())
	}
}

func TestApplyFiltersWithSingleTask(t *testing.T) {
	// Arrange: Create a real Pipeline
	pipeline := &pipeline.Pipeline{
		Name: "TestPipeline",
		Assets: []*pipeline.Asset{
			{
				Name: "Task1",
				Type: pipeline.AssetTypeBigqueryQuery,
				Tags: []string{"tag1"},
			},
		},
		MetadataPush: pipeline.MetadataPush{
			Global:   false,
			BigQuery: false,
		},
	}

	logger := zap.NewNop().Sugar() // Replace with a real logger if needed
	s := scheduler.NewScheduler(logger, pipeline)

	filter := &Filter{
		IncludeDownstream: true,
		IncludeTag:        "",
	}

	task := pipeline.Assets[0] // Single task: "Task1"

	// Act: Apply filters and mark assets
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, task)

	// Assert: Validate the task was marked correctly
	assert.NoError(t, err)

	// Validate that "Task1" is marked as Pending in the scheduler
	markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
	assert.Len(t, markedTasks, 1)
	printMarkedTasks(markedTasks) // Debug print
	assert.Equal(t, "Task1", markedTasks[0].GetAsset().Name)
}

func TestApplyFiltersWithIncludeTag_MatchesAssets(t *testing.T) {
	// Arrange: Create a pipeline with tagged assets
	pipeline := &pipeline.Pipeline{
		Name: "TestPipeline",
		Assets: []*pipeline.Asset{
			{
				Name: "Task1",
				Type: pipeline.AssetTypeBigqueryQuery,
				Tags: []string{"tag1", "tag2"},
			},
			{
				Name: "Task2",
				Type: pipeline.AssetTypePython,
				Tags: []string{"tag2"},
			},
		},
		MetadataPush: pipeline.MetadataPush{
			Global:   false, // Disable metadata push for clarity
			BigQuery: false,
		},
	}

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, pipeline)

	filter := &Filter{
		IncludeDownstream: false, // Focus on matching tags only
		IncludeTag:        "tag1",
	}

	// Act: Apply filters
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, nil)

	// Assert
	assert.NoError(t, err)

	// Validate that only "Task1" is marked as Pending
	markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
	assert.Len(t, markedTasks, 1)
	assert.Equal(t, "Task1", markedTasks[0].GetAsset().Name)
}

func TestApplyFiltersWithIncludeTag_NoMatches(t *testing.T) {
	// Arrange: Create a pipeline with tagged assets
	pipeline := &pipeline.Pipeline{
		Name: "TestPipeline",
		Assets: []*pipeline.Asset{
			{
				Name: "Task1",
				Type: pipeline.AssetTypeBigqueryQuery,
				Tags: []string{"tag1", "tag2"},
			},
			{
				Name: "Task2",
				Type: pipeline.AssetTypePython,
				Tags: []string{"tag2"},
			},
		},
		MetadataPush: pipeline.MetadataPush{
			Global:   false,
			BigQuery: false,
		},
	}

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, pipeline)

	filter := &Filter{
		IncludeDownstream: false,  // Focus on matching tags only
		IncludeTag:        "tag3", // No asset has "tag3"
	}

	// Act: Apply filters
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, nil)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, "no assets found with include tag 'tag3'")

}

func TestApplyFiltersWithIncludeTag_AndDownstream(t *testing.T) {
	task3 := &pipeline.Asset{
		Name:      "Task3",
		Type:      pipeline.AssetTypePython,
		Tags:      []string{},
		Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task2"}}, // Depends on Task2
	}
	task2 := &pipeline.Asset{
		Name:      "Task2",
		Type:      pipeline.AssetTypeBigqueryQuery,
		Tags:      []string{},
		Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task1"}}, // Depends on Task1
	}
	task1 := &pipeline.Asset{
		Name:      "Task1",
		Type:      pipeline.AssetTypePython,
		Tags:      []string{"tag1"},
		Upstreams: []pipeline.Upstream{}, // Task1 is the root
	}

	pipeline := &pipeline.Pipeline{
		Name:   "TestPipeline",
		Assets: []*pipeline.Asset{task1, task2, task3},
		MetadataPush: pipeline.MetadataPush{
			Global:   false,
			BigQuery: false,
		},
	}

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, pipeline)

	filter := &Filter{
		IncludeDownstream: true,
		IncludeTag:        "tag1",
	}

	// Act: Apply filters
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, nil)

	// Assert
	assert.NoError(t, err)

	// Validate that all tasks in the downstream path are marked
	markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
	assert.Len(t, markedTasks, 3)
	taskNames := []string{markedTasks[0].GetAsset().Name, markedTasks[1].GetAsset().Name, markedTasks[2].GetAsset().Name}
	assert.Contains(t, taskNames, "Task1")
	assert.Contains(t, taskNames, "Task2")
	assert.Contains(t, taskNames, "Task3")
}

func TestApplyFiltersWithOnlyCheckTaskType(t *testing.T) {
	// Arrange: Create a pipeline with tasks of different types
	pipeline := &pipeline.Pipeline{
		Name: "TestPipeline",
		Assets: []*pipeline.Asset{
			{Name: "MainTask", Type: pipeline.AssetTypePython},         // Treated as "main"
			{Name: "MainTask2", Type: pipeline.AssetTypeBigqueryQuery}, // Treated as "checks"
		},
		MetadataPush: pipeline.MetadataPush{
			Global:   false,
			BigQuery: false,
		},
	}

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, pipeline)

	filter := &Filter{
		OnlyTaskTypes: []string{"checks"},
	}

	// Act: Apply filters
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, nil)

	// Assert
	assert.NoError(t, err)

	// Validate that only "main" tasks are marked
	markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
	printMarkedTasks(markedTasks)
	assert.Len(t, markedTasks, 0)

}

func TestApplyFiltersWithInvalidTaskType(t *testing.T) {
	// Arrange: Create a pipeline
	pipeline := &pipeline.Pipeline{
		Name: "TestPipeline",
		Assets: []*pipeline.Asset{
			{Name: "MainTask", Type: pipeline.AssetTypePython},
			{Name: "CheckTask", Type: pipeline.AssetTypeBigqueryQuery},
		},
		MetadataPush: pipeline.MetadataPush{
			Global:   false,
			BigQuery: false,
		},
	}

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, pipeline)

	filter := &Filter{
		OnlyTaskTypes: []string{"bigquery-query"}, // Invalid value
	}

	// Act: Apply filters
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, nil)

	// Assert
	assert.Error(t, err)
	assert.EqualError(t, err, "invalid value for '--only' flag: 'bigquery-query', available values are 'main', 'checks', and 'push-metadata'")

}

func TestApplyFiltersWithOnlyMetadataPush(t *testing.T) {
	pipeline := &pipeline.Pipeline{
		Name: "TestPipeline",
		Assets: []*pipeline.Asset{
			{Name: "Task1", Type: pipeline.AssetTypeBigqueryQuery},
		},
		MetadataPush: pipeline.MetadataPush{
			Global:   true, // Enable metadata push
			BigQuery: true,
		},
	}

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, pipeline)

	filter := &Filter{
		OnlyTaskTypes: []string{"push-metadata"},
	}

	// Act
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, nil)

	// Assert
	assert.NoError(t, err)
	markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
	printMarkedTasks(markedTasks)
	assert.Len(t, markedTasks, 1)
	assert.Equal(t, "Task1:metadata-push", markedTasks[0].GetHumanID())
}

func TestApplyFiltersWithEmptyPipeline(t *testing.T) {
	pipeline := &pipeline.Pipeline{
		Name:   "EmptyPipeline",
		Assets: []*pipeline.Asset{},
	}

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, pipeline)

	filter := &Filter{}

	// Act
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, nil)

	// Assert
	assert.NoError(t, err)
	markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
	assert.Len(t, markedTasks, 0)
}

func TestApplyFiltersWithEmptyFilter(t *testing.T) {
	// Arrange: Create a pipeline with multiple assets
	pipeline := &pipeline.Pipeline{
		Name: "TestPipeline",
		Assets: []*pipeline.Asset{
			{Name: "Task1", Type: pipeline.AssetTypePython, Tags: []string{"tag1"}},
			{Name: "Task2", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag2"}},
			{Name: "Task3", Type: pipeline.AssetTypePython, Tags: []string{}},
			{Name: "Task4", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag1", "tag3"}},
		},
		MetadataPush: pipeline.MetadataPush{
			Global:   false, // Metadata push disabled
			BigQuery: false,
		},
	}

	// Set up the scheduler and logger
	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, pipeline)

	// Create an "empty" filter where no flags are specified
	filter := &Filter{
		IncludeTag:        "",    // No include tag
		ExcludeTag:        "",    // No exclude tag
		OnlyTaskTypes:     nil,   // No task types specified
		IncludeDownstream: false, // Do not include downstream tasks
		PushMetaData:      false, // No metadata push
	}

	// Act: Apply filters
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, nil)

	// Assert
	assert.NoError(t, err)

	// Validate that all tasks are marked as Pending
	markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
	assert.Len(t, markedTasks, len(pipeline.Assets)) // All tasks should be included

	// Ensure all task names are in the marked tasks
	markedTaskNames := []string{}
	for _, task := range markedTasks {
		markedTaskNames = append(markedTaskNames, task.GetAsset().Name)
	}

	for _, asset := range pipeline.Assets {
		assert.Contains(t, markedTaskNames, asset.Name)
	}
}

func TestApplyFiltersWithIncludeTagAndOnlyChecks(t *testing.T) {
	// Arrange: Create a pipeline with multiple assets, some sharing the same tag
	pipeline := &pipeline.Pipeline{
		Name: "ComplexPipeline",
		Assets: []*pipeline.Asset{
			{Name: "Task1", Type: pipeline.AssetTypePython, Tags: []string{"common-tag"}},        // main
			{Name: "Task2", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"common-tag"}}, // checks
			{Name: "Task3", Type: pipeline.AssetTypePython, Tags: []string{"unique-tag"}},        // main
			{Name: "Task4", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"common-tag"}}, // checks
		},
		MetadataPush: pipeline.MetadataPush{
			Global:   false, // Metadata push disabled
			BigQuery: false,
		},
	}

	// Set up the scheduler and logger
	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, pipeline)

	// Create a filter that specifies:
	// - Include only tasks with "common-tag"
	// - Restrict to task type "checks" (BigqueryQuery)
	filter := &Filter{
		IncludeTag:        "common-tag",
		ExcludeTag:        "", // No exclusions
		OnlyTaskTypes:     []string{"checks"},
		IncludeDownstream: false, // No downstream tasks
		PushMetaData:      false, // No metadata push
	}

	// Act: Apply filters
	err := filter.ApplyFiltersAndMarkAssets(pipeline, s, nil)

	// Assert
	assert.NoError(t, err)

	// Validate that only the "checks" tasks with "common-tag" are marked as Pending
	markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
	assert.Len(t, markedTasks, 0)

}
