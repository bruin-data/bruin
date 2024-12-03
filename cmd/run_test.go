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
func TestApplyFilters(t *testing.T) {
	t.Parallel() // Enable parallel execution for the top-level test

	tests := []struct {
		name            string
		pipeline        *pipeline.Pipeline
		filter          *Filter
		expectedPending []string
		expectError     bool
		expectedError   string
	}{
		{
			name: "Single Task",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					{Name: "Task1", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag1"}},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:          &Filter{IncludeDownstream: true},
			expectedPending: []string{"Task1"},
			expectError:     false,
		},
		{
			name: "Include Tag Matches",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					{Name: "Task1", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag1", "tag2"}},
					{Name: "Task2", Type: pipeline.AssetTypePython, Tags: []string{"tag2"}},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:          &Filter{IncludeTag: "tag1"},
			expectedPending: []string{"Task1"},
			expectError:     false,
		},
		{
			name: "Include Tag No Matches",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					{Name: "Task1", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag1", "tag2"}},
					{Name: "Task2", Type: pipeline.AssetTypePython, Tags: []string{"tag2"}},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:          &Filter{IncludeTag: "tag3"},
			expectedPending: nil,
			expectError:     true,
			expectedError:   "no assets found with include tag 'tag3'",
		},
		{
			name: "Include Tag And Downstream",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					{
						Name:      "Task1",
						Type:      pipeline.AssetTypePython,
						Tags:      []string{"tag1"},
						Upstreams: []pipeline.Upstream{},
					},
					{
						Name:      "Task2",
						Type:      pipeline.AssetTypeBigqueryQuery,
						Tags:      []string{},
						Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task1"}},
					},
					{
						Name:      "Task3",
						Type:      pipeline.AssetTypePython,
						Tags:      []string{},
						Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task2"}},
					},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:          &Filter{IncludeDownstream: true, IncludeTag: "tag1"},
			expectedPending: []string{"Task1", "Task2", "Task3"},
			expectError:     false,
		},
		{
			name: "Only Check Task Type",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					{Name: "MainTask", Type: pipeline.AssetTypePython},
					{Name: "CheckTask", Type: pipeline.AssetTypeBigqueryQuery},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:          &Filter{OnlyTaskTypes: []string{"checks"}},
			expectedPending: []string{},
			expectError:     false,
		},
		{
			name: "Invalid Task Type",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					{Name: "MainTask", Type: pipeline.AssetTypePython},
					{Name: "CheckTask", Type: pipeline.AssetTypeBigqueryQuery},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:        &Filter{OnlyTaskTypes: []string{"bigquery-query"}},
			expectedError: "invalid value for '--only' flag: 'bigquery-query', available values are 'main', 'checks', and 'push-metadata'",
			expectError:   true,
		},
		{
			name: "Empty Pipeline",
			pipeline: &pipeline.Pipeline{
				Name:   "EmptyPipeline",
				Assets: []*pipeline.Asset{},
			},
			filter:          &Filter{},
			expectedPending: []string{},
			expectError:     false,
		},
		{
			name: "Empty Filter - Regular workflow",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					{Name: "Task1", Type: pipeline.AssetTypePython, Tags: []string{"tag1"}},
					{Name: "Task2", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag2"}},
					{Name: "Task3", Type: pipeline.AssetTypePython, Tags: []string{}},
					{Name: "Task4", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag1", "tag3"}},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:          &Filter{},
			expectedPending: []string{"Task1", "Task2", "Task3", "Task4"},
			expectError:     false,
		},
		{
			name: "Include Tag And Only Checks",
			pipeline: &pipeline.Pipeline{
				Name: "ComplexPipeline",
				Assets: []*pipeline.Asset{
					{Name: "Task1", Type: pipeline.AssetTypePython, Tags: []string{"common-tag"}},
					{Name: "Task2", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"common-tag"}},
					{Name: "Task3", Type: pipeline.AssetTypePython, Tags: []string{"unique-tag"}},
					{Name: "Task4", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"common-tag"}},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:          &Filter{IncludeTag: "common-tag", OnlyTaskTypes: []string{"checks"}},
			expectedPending: []string{},
			expectError:     false,
		},
		{
			name: "Multiple Tags - Mixed Matches",
			pipeline: &pipeline.Pipeline{
				Name: "MultiTagPipeline",
				Assets: []*pipeline.Asset{
					{Name: "Task1", Type: pipeline.AssetTypePython, Tags: []string{"tag1", "tag2"}},
					{Name: "Task2", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag2", "tag3"}},
					{Name: "Task3", Type: pipeline.AssetTypePython, Tags: []string{"tag3", "tag4"}},
					{Name: "Task4", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag1", "tag4"}},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:          &Filter{IncludeTag: "tag1"},
			expectedPending: []string{"Task1", "Task4"}, // Only tasks with "tag1" should match
			expectError:     false,
		},
		{
			name: "Empty Tags",
			pipeline: &pipeline.Pipeline{
				Name: "EmptyTagsPipeline",
				Assets: []*pipeline.Asset{
					{Name: "Task1", Type: pipeline.AssetTypePython, Tags: []string{}},
					{Name: "Task2", Type: pipeline.AssetTypeBigqueryQuery, Tags: []string{"tag2"}},
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter:          &Filter{IncludeTag: "tag1"},
			expectedPending: []string{}, // No matches since "tag1" isn't present
			expectError:     true,
			expectedError:   "no assets found with include tag 'tag1'",
		},

		{
			name: "Metadata Push Enabled",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					{Name: "Task1", Type: pipeline.AssetTypeBigqueryQuery},
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: true},
			},
			filter:          &Filter{PushMetaData: true},
			expectedPending: []string{"Task1", "Task1:metadata-push"}, // Metadata push instance should be included
			expectError:     false,
		},

		{
			name: "Metadata Push Enabled with only metadatapush",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					{Name: "Task1", Type: pipeline.AssetTypeBigqueryQuery},
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: true},
			},
			filter:          &Filter{PushMetaData: true, OnlyTaskTypes: []string{"push-metadata"}},
			expectedPending: []string{"Task1:metadata-push"}, // Metadata push instance should be included
			expectError:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel() // Enable parallel execution for individual test cases
			logger := zap.NewNop().Sugar()
			s := scheduler.NewScheduler(logger, tt.pipeline)

			// Act
			err := tt.filter.ApplyFiltersAndMarkAssets(tt.pipeline, s, nil)

			// Assert
			if tt.expectError {
				assert.Error(t, err)
				assert.EqualError(t, err, tt.expectedError)
			} else {
				assert.NoError(t, err)
				markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
				assert.Len(t, markedTasks, len(tt.expectedPending))

				// Collect task HumanIDs
				taskNames := []string{}
				for _, task := range markedTasks {
					taskNames = append(taskNames, task.GetHumanID()) // Use HumanID for accurate matching
				}

				// Check if each expected task is present in the marked tasks
				for _, name := range tt.expectedPending {
					assert.Contains(t, taskNames, name)
				}
			}
		})
	}
}
