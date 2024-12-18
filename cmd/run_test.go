package cmd

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/bruin-data/bruin/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

var (
	task0 = &pipeline.Asset{
		Name: "Task0",
		Type: pipeline.AssetTypeBigqueryQuery,
		Tags: []string{"tag1"},
		Columns: []pipeline.Column{
			{
				Name:        "Column0",
				Type:        "STRING",
				Description: "A sample column",
				PrimaryKey:  true,
				Checks: []pipeline.ColumnCheck{
					{Name: "Check0"},
				},
			},
		},
		CustomChecks: []pipeline.CustomCheck{
			{Name: "CustomCheck0", Description: "some custom check"},
		},
	}

	task1 = &pipeline.Asset{
		Name: "Task1",
		Type: pipeline.AssetTypePython,
		Tags: []string{"tag1", "x"},
	}

	task2 = &pipeline.Asset{
		Name: "Task2",
		Type: pipeline.AssetTypeBigqueryQuery,
		Tags: []string{"tag2"},
	}

	task3 = &pipeline.Asset{
		Name: "Task3", Type: pipeline.AssetTypePython,
		Tags:      []string{"tag4"},
		Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task9"}},
	}

	task4 = &pipeline.Asset{
		Name:      "Task4",
		Type:      pipeline.AssetTypeBigqueryQuery,
		Tags:      []string{"tag1", "tag3"},
		Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task9"}},
	}

	task5 = &pipeline.Asset{
		Name:      "Task5",
		Type:      pipeline.AssetTypeBigqueryQuery,
		Tags:      []string{"tag1"},
		Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task8"}}, // Task8 is upstream
	}

	task6 = &pipeline.Asset{
		Name:      "Task6",
		Type:      pipeline.AssetTypeBigqueryQuery,
		Tags:      []string{},
		Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task8"}}, // Task8 is upstream
	}

	task7 = &pipeline.Asset{
		Name:      "Task7",
		Type:      pipeline.AssetTypePython,
		Tags:      []string{""},
		Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task8"}}, // Task8 is upstream
	}

	task8 = &pipeline.Asset{
		Name:      "Task8",
		Type:      pipeline.AssetTypeBigqueryQuery,
		Tags:      []string{"tag1", "exclude"},
		Upstreams: []pipeline.Upstream{}, // No upstreams for Task8
	}

	task9 = &pipeline.Asset{
		Name:      "Task9",
		Type:      pipeline.AssetTypeBigqueryQuery,
		Tags:      []string{"tag4"},
		Upstreams: []pipeline.Upstream{},
	}

	task10 = &pipeline.Asset{
		Name: "Task10",
		Type: pipeline.AssetTypeBigqueryQuery,
		Tags: []string{"tag1"},
		Columns: []pipeline.Column{
			{
				Name:        "Column10",
				Type:        "STRING",
				Description: "A sample column",
				PrimaryKey:  true,
				Checks: []pipeline.ColumnCheck{
					{Name: "Check10"},
				},
			},
		},
		CustomChecks: []pipeline.CustomCheck{
			{Name: "CustomCheck10", Description: "some custom check"},
		},
	}
	task11 = &pipeline.Asset{
		Name: "Task11",
		Type: pipeline.AssetTypeBigqueryQuery,
		Tags: []string{"tag1", "tag4"},
		Columns: []pipeline.Column{
			{
				Name:        "Column11",
				Type:        "STRING",
				Description: "A sample column",
				PrimaryKey:  true,
				Checks: []pipeline.ColumnCheck{
					{Name: "Check11"},
				},
			},
		},
		CustomChecks: []pipeline.CustomCheck{
			{Name: "CustomCheck11", Description: "some custom check"},
		},
		Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task9"}},
	}
	task12 = &pipeline.Asset{
		Name: "Task12",
		Type: pipeline.AssetTypeBigqueryQuery,
		Tags: []string{},
		Columns: []pipeline.Column{
			{
				Name:        "Column12",
				Type:        "STRING",
				Description: "A sample column",
				PrimaryKey:  true,
				Checks: []pipeline.ColumnCheck{
					{Name: "Check12"},
				},
			},
		},
		CustomChecks: []pipeline.CustomCheck{
			{Name: "CustomCheck12", Description: "some custom check"},
		},
		Upstreams: []pipeline.Upstream{{Type: "asset", Value: "Task9"}},
	}
)

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
			name: " Run single Task in a full pipeline",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				SingleTask: task4,
			},
			expectedPending: []string{"Task4"},
			expectError:     false,
		},
		{
			name: "Single Task with a Tag",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				SingleTask: task4,
				IncludeTag: "tag1",
			},
			expectedPending: []string{},
			expectError:     true,
			expectedError:   "you cannot use the '--tag' flag when running a single asset",
		},

		{
			name: "Run single Task with downstream",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				SingleTask:        task8,
				IncludeDownstream: true,
			},
			expectedPending: []string{"Task8", "Task7", "Task6", "Task5"},
			expectError:     false,
		},
		{
			name: "Run single Task with downstream and a tag",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task5,
					task6,
					task7,
					task8,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				SingleTask:        task8,
				IncludeDownstream: true,
				IncludeTag:        "tag1",
			},
			expectedPending: []string{},
			expectError:     true,
			expectedError:   "you cannot use the '--tag' flag when running a single asset",
		},
		{
			name: "Filter By Tag and Only at the same time",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task4,
					task5,
					task6,
					task7,
					task8,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: true},
			},
			filter: &Filter{
				IncludeDownstream: false,
				IncludeTag:        "tag1",
				OnlyTaskTypes:     []string{"push-metadata"},
				PushMetaData:      true,
			},
			expectedPending: []string{"Task8:metadata-push", "Task5:metadata-push", "Task4:metadata-push"},
			expectError:     false,
		},
		{
			name: "Filter By Tag and run its downstream",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				IncludeDownstream: true,
				IncludeTag:        "tag1",
			},
			expectedPending: []string{
				"Task0", "Task1", "Task4", "Task5", "Task6", "Task7", "Task8",
				"Task0:Column0:Check0", "Task0:custom-check:customcheck0",
			}, // task 2 and 3 shouldnt run
			expectError: false,
		},
		{
			name: "Filter by Tag and Run Matching Downstream Tasks by Type",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
					task9,
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: true},
			},
			filter: &Filter{
				IncludeDownstream: true,
				IncludeTag:        "tag4",
				PushMetaData:      true,
				OnlyTaskTypes:     []string{"push-metadata"},
			},
			expectedPending: []string{"Task3:metadata-push", "Task4:metadata-push", "Task9:metadata-push"},
			expectError:     false,
		},

		{
			name: "Run single asset with PushmetaData",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
					task9,
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: true},
			},
			filter: &Filter{
				SingleTask:        task4,
				IncludeDownstream: true,
				PushMetaData:      true,
			},
			expectedPending: []string{"Task4", "Task4:metadata-push"},
			expectError:     false,
		},

		{
			name: " Run single asset with PushmetaData and Only flag",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
					task9,
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: true},
			},
			filter: &Filter{
				SingleTask:        task4,
				IncludeDownstream: true,
				PushMetaData:      true,
				OnlyTaskTypes:     []string{"push-metadata"},
			},
			expectedPending: []string{"Task4:metadata-push"},
			expectError:     false,
		},
		{
			name: " Run only Checks",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
					task9,
					task10,
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: true},
			},
			filter: &Filter{
				IncludeDownstream: true,
				PushMetaData:      true,
				OnlyTaskTypes:     []string{"checks"},
			},
			expectedPending: []string{"Task0:Column0:Check0", "Task0:custom-check:customcheck0", "Task10:Column10:Check10", "Task10:custom-check:customcheck10"},
			expectError:     false,
		},

		{
			name: " Run only Main",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
					task9,
					task10,
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: true},
			},
			filter: &Filter{
				IncludeDownstream: true,
				PushMetaData:      true,
				OnlyTaskTypes:     []string{"main"}, // checks and metadata should be excluded
			},
			expectedPending: []string{"Task0", "Task1", "Task2", "Task3", "Task4", "Task5", "Task6", "Task7", "Task8", "Task9", "Task10"},
			expectError:     false,
		},

		{
			name: " Run full pipeline",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
					task9,
					task10,
				},
			},
			filter: &Filter{},
			expectedPending: []string{
				"Task0", "Task1", "Task2", "Task3", "Task4", "Task5", "Task6", "Task7", "Task8", "Task9", "Task10",
				"Task10:Column10:Check10", "Task10:custom-check:customcheck10", "Task0:Column0:Check0", "Task0:custom-check:customcheck0",
			},
			expectError: false,
		},
		{
			name: " Run single asset with only Checks",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task6,
					task7,
					task8,
					task9,
					task10,
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: true},
			},
			filter: &Filter{
				SingleTask:        task10,
				IncludeDownstream: true,
				PushMetaData:      true,
				OnlyTaskTypes:     []string{"checks"},
			},
			expectedPending: []string{"Task10:Column10:Check10", "Task10:custom-check:customcheck10"},
			expectError:     false,
		},
		{
			name: "Only flag with multiple arguments",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task6,
					task7,
					task8,
					task9,
					task10,
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: true},
			},
			filter: &Filter{
				SingleTask:        task10,
				IncludeDownstream: true,
				PushMetaData:      true,
				OnlyTaskTypes:     []string{"checks", "push-metadata"},
			},
			expectedPending: []string{"Task10:Column10:Check10", "Task10:custom-check:customcheck10", "Task10:metadata-push"},
			expectError:     false,
		},

		{
			name: "Single Task with a exclude-tag",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				SingleTask: task4,
				ExcludeTag: "tag1",
			},
			expectedPending: []string{},
			expectError:     true,
			expectedError:   "you cannot use the '--exclude-tag' flag when running a single asset",
		},

		{
			name: "Full pipeline with exclude-tag",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag: "tag1",
			},
			expectedPending: []string{"Task2", "Task3"},
			expectError:     false,
		},

		{
			name: "Full pipeline with exclude-tag",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag: "non-existent-tag",
			},
			expectedPending: []string{},
			expectError:     true,
			expectedError:   "no assets found with exclude tag 'non-existent-tag'",
		},

		{
			name: "Exclude Tag and the downstreams",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag:        "exclude",
				IncludeDownstream: true,
			},
			expectedPending: []string{"Task1", "Task2", "Task3", "Task4"},
		},
		{
			name: "Exclude Tag and the downstreams",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag:        "exclude",
				IncludeDownstream: true,
			},
			expectedPending: []string{"Task1", "Task2", "Task3", "Task4"},
		},

		{
			name: "Exclude and Include Tag",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task4,
					task5,
					task6,
					task7,
					task8,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag:        "tag3",
				IncludeTag:        "tag1",
				IncludeDownstream: false,
			},
			expectedPending: []string{"Task5", "Task8"},
		},

		{
			name: "Exclude and Include Tag (exclude tag non existent)",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task4,
					task5,
					task6,
					task7,
					task8,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag:        "non-existent-tag",
				IncludeTag:        "tag1",
				IncludeDownstream: false,
			},
			expectedPending: []string{},
			expectError:     true,
			expectedError:   "no assets found with exclude tag 'non-existent-tag'",
		},

		{
			name: "Exclude ,Include Tag with downstream",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task2,
					task4,
					task5,
					task6,
					task7,
					task8,
					task9,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag:        "tag3",
				IncludeTag:        "tag1",
				IncludeDownstream: true,
			},
			expectedPending: []string{"Task5", "Task6", "Task7", "Task8"},
		},

		{
			name: "Exclude with only checks ",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag:    "x",
				OnlyTaskTypes: []string{"checks"},
			},
			expectedPending: []string{"Task0:Column0:Check0", "Task0:custom-check:customcheck0"},
		},
		{
			name: "Exclude with only checks 2",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag:    "x",
				OnlyTaskTypes: []string{"checks"},
				PushMetaData:  true,
			},
			expectedPending: []string{"Task0:Column0:Check0", "Task0:custom-check:customcheck0"},
		},

		{
			name: "Exclude with only checks and metadapush",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
				},
				MetadataPush: pipeline.MetadataPush{Global: true, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag:    "x",
				OnlyTaskTypes: []string{"checks", "push-metadata"},
				PushMetaData:  true,
			},
			expectedPending: []string{"Task0:Column0:Check0", "Task0:custom-check:customcheck0", "Task0:metadata-push"},
		},

		{
			name: "Exclude ,Include Tag with downstream and only flag",
			pipeline: &pipeline.Pipeline{
				Name: "TestPipeline",
				Assets: []*pipeline.Asset{
					task0,
					task1,
					task2,
					task3,
					task4,
					task5,
					task6,
					task7,
					task8,
					task9,
					task10,
					task11,
					task12,
				},
				MetadataPush: pipeline.MetadataPush{Global: false, BigQuery: false},
			},
			filter: &Filter{
				ExcludeTag:        "tag1",
				IncludeTag:        "tag4",
				IncludeDownstream: true,
				OnlyTaskTypes:     []string{"checks"},
			},
			expectedPending: []string{"Task12:Column12:Check12", "Task12:custom-check:customcheck12"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel() // Enable parallel execution for individual test cases
			logger := zap.NewNop().Sugar()
			s := scheduler.NewScheduler(logger, tt.pipeline, state.NewState("test", map[string]string{}, "test"))

			// Act
			err := tt.filter.ApplyFiltersAndMarkAssets(tt.pipeline, s)

			// Assert
			if tt.expectError {
				require.Error(t, err)
				assert.EqualError(t, err, tt.expectedError)
			} else {
				require.NoError(t, err)
				markedTasks := s.GetTaskInstancesByStatus(scheduler.Pending)
				assert.Len(t, markedTasks, len(tt.expectedPending))

				// Collect task HumanIDs
				taskNames := []string{}
				for _, task := range markedTasks {
					taskNames = append(taskNames, task.GetHumanID()) // Use HumanID for accurate matching
					print(task.GetHumanID())
				}

				// Check if each expected task is present in the marked tasks
				for _, name := range tt.expectedPending {
					assert.Contains(t, taskNames, name)
				}
			}
		})
	}
}
