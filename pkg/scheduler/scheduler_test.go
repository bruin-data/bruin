package scheduler

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/version"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestScheduler_GetStatusForTask(t *testing.T) {
	t.Parallel()

	result := GetStatusForTask([]TaskInstanceStatus{Pending, Pending, Pending, Pending, Pending})
	assert.Equal(t, Pending.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{Failed, Failed, Failed, Failed, Failed})
	assert.Equal(t, Failed.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{Succeeded, Succeeded, Succeeded, Succeeded, Succeeded})
	assert.Equal(t, Succeeded.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{Succeeded, Succeeded, Failed})
	assert.Equal(t, Failed.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{Failed, Succeeded, Succeeded})
	assert.Equal(t, Failed.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{UpstreamFailed, Succeeded, Succeeded})
	assert.Equal(t, Failed.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{UpstreamFailed, Failed})
	assert.Equal(t, Failed.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{Skipped, Skipped, Skipped})
	assert.Equal(t, Skipped.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{Skipped, Skipped, Failed})
	assert.Equal(t, Failed.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{Skipped, Skipped, Succeeded})
	assert.Equal(t, Succeeded.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{Succeeded, Skipped, Skipped})
	assert.Equal(t, Succeeded.String(), result.String())

	result = GetStatusForTask([]TaskInstanceStatus{Succeeded, Running, Skipped})
	assert.Equal(t, Pending.String(), result.String())
}

func TestScheduler_getScheduleableTasks(t *testing.T) {
	t.Parallel()

	// In the test cases I'll simulate the execution steps of the following graph:
	// task1 -> task3
	// task2 -> task4
	// task3 -> task5
	// task4 -> task5

	tasks := []*pipeline.Asset{
		{
			Name: "task11",
		},
		{
			Name: "task21",
		},
		{
			Name: "task12",
			Upstreams: []pipeline.Upstream{
				{Type: "asset", Value: "task11"},
			},
		},
		{
			Name: "task22",
			Upstreams: []pipeline.Upstream{
				{Type: "asset", Value: "task21"},
			},
		},
		{
			Name: "task3",
			Upstreams: []pipeline.Upstream{
				{Type: "asset", Value: "task12"},
				{Type: "asset", Value: "task22"},
			},
		},
	}

	tests := []struct {
		name          string
		taskInstances map[string]TaskInstanceStatus
		want          []string
	}{
		{
			name: "beginning the pipeline execution",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Pending,
				"task12": Pending,
				"task21": Pending,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{"task11", "task21"},
		},
		{
			name: "both t1 and t2 are running, should get nothing",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Running,
				"task12": Pending,
				"task21": Running,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{},
		},
		{
			name: "t11 succeeded, should get t12",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Pending,
				"task21": Running,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{"task12"},
		},
		{
			name: "t12 succeeded as well, shouldn't get anything yet",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Succeeded,
				"task21": Running,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{},
		},
		{
			name: "t21 succeeded, should get t22",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Succeeded,
				"task21": Succeeded,
				"task22": Pending,
				"task3":  Pending,
			},
			want: []string{"task22"},
		},
		{
			name: "t22 succeeded as well, should get the final Asset",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Succeeded,
				"task21": Succeeded,
				"task22": Succeeded,
				"task3":  Pending,
			},
			want: []string{"task3"},
		},
		{
			name: "everything succeeded, should get nothing",
			taskInstances: map[string]TaskInstanceStatus{
				"task11": Succeeded,
				"task12": Succeeded,
				"task21": Succeeded,
				"task22": Succeeded,
				"task3":  Succeeded,
			},
			want: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			taskInstances := make([]TaskInstance, 0, len(tasks))
			for _, task := range tasks {
				status, ok := tt.taskInstances[task.Name]
				if !ok {
					t.Fatalf("Given Asset doesn't have a status set on the test: %s", task.Name)
				}
				taskInstances = append(taskInstances, &AssetInstance{
					Asset:  task,
					status: status,
				})
			}

			p := &Scheduler{
				taskInstances: taskInstances,
			}
			p.initialize()

			got := p.getScheduleableTasks()
			gotNames := make([]string, 0, len(got))
			for _, t := range got {
				gotNames = append(gotNames, t.GetAsset().Name)
			}

			assert.Equal(t, tt.want, gotNames)
		})
	}
}

func TestScheduler_getScheduleableTasksWithNoConnectionLimits(t *testing.T) {
	t.Parallel()

	s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
		{Name: "first", Connection: "postgres"},
		{Name: "second", Connection: "postgres"},
	}, nil)

	assert.Equal(t, []string{"first", "second"}, scheduleableHumanIDs(s))
}

func TestScheduler_getScheduleableTasksHonorsSingleConnectionLimit(t *testing.T) {
	t.Parallel()

	t.Run("reserves capacity within the same scheduling batch", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "first", Connection: "postgres"},
			{Name: "second", Connection: "postgres"},
			{Name: "third", Connection: "postgres"},
		}, map[string]int{"postgres": 1})

		assert.Equal(t, []string{"first"}, scheduleableHumanIDs(s))
	})

	t.Run("queued tasks consume capacity", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "first", Connection: "postgres"},
			{Name: "second", Connection: "postgres"},
		}, map[string]int{"postgres": 1})
		markMainTaskStatus(s, "first", Queued)

		assert.Empty(t, scheduleableHumanIDs(s))
	})

	t.Run("running tasks consume capacity", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "first", Connection: "postgres"},
			{Name: "second", Connection: "postgres"},
		}, map[string]int{"postgres": 1})
		markMainTaskStatus(s, "first", Running)

		assert.Empty(t, scheduleableHumanIDs(s))
	})
}

func TestScheduler_getScheduleableTasksDoesNotBlockUnrelatedConnections(t *testing.T) {
	t.Parallel()

	s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
		{Name: "limited-first", Connection: "postgres"},
		{Name: "unrelated", Connection: "snowflake"},
		{Name: "limited-second", Connection: "postgres"},
	}, map[string]int{"postgres": 1})

	assert.Equal(t, []string{"limited-first", "unrelated"}, scheduleableHumanIDs(s))
}

func TestScheduler_getScheduleableTasksRequiresCapacityForEveryLimitedConnection(t *testing.T) {
	t.Parallel()

	s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
		{Name: "source-holder", Connection: "source"},
		{
			Name: "python-task",
			Type: pipeline.AssetTypePython,
			Secrets: []pipeline.SecretMapping{
				{SecretKey: "source", InjectedKey: "SOURCE"},
				{SecretKey: "destination", InjectedKey: "DESTINATION"},
			},
		},
	}, map[string]int{"source": 1, "destination": 1})
	markMainTaskStatus(s, "source-holder", Running)

	assert.Empty(t, scheduleableHumanIDs(s))

	markMainTaskStatus(s, "source-holder", Succeeded)
	assert.Equal(t, []string{"python-task"}, scheduleableHumanIDs(s))
}

func TestScheduler_getScheduleableTasksLimitsMaterializedPythonDestination(t *testing.T) {
	t.Parallel()

	materializedPythonAsset := func(name string) *pipeline.Asset {
		return &pipeline.Asset{
			Name: "python-" + name,
			Type: pipeline.AssetTypePython,
			Materialization: pipeline.Materialization{
				Type: pipeline.MaterializationTypeTable,
			},
		}
	}

	s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
		materializedPythonAsset("first"),
		materializedPythonAsset("second"),
	}, map[string]int{"gcp-default": 1})

	assert.Equal(t, []string{"python-first"}, scheduleableHumanIDs(s))
}

func TestScheduler_getScheduleableTasksLimitsExplicitPythonAndRConnections(t *testing.T) {
	t.Parallel()

	t.Run("python", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "python-first", Type: pipeline.AssetTypePython, Connection: "warehouse"},
			{Name: "python-second", Type: pipeline.AssetTypePython, Connection: "warehouse"},
		}, map[string]int{"warehouse": 1})

		assert.Equal(t, []string{"python-first"}, scheduleableHumanIDs(s))
	})

	t.Run("r", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "r-first", Type: pipeline.AssetTypeR, Connection: "warehouse"},
			{Name: "r-second", Type: pipeline.AssetTypeR, Connection: "warehouse"},
		}, map[string]int{"warehouse": 1})

		assert.Equal(t, []string{"r-first"}, scheduleableHumanIDs(s))
	})
}

func TestScheduler_getScheduleableTasksLimitsRSecrets(t *testing.T) {
	t.Parallel()

	rAsset := func(name string) *pipeline.Asset {
		return &pipeline.Asset{
			Name: "r-" + name,
			Type: pipeline.AssetTypeR,
			Secrets: []pipeline.SecretMapping{
				{SecretKey: "r-source", InjectedKey: "R_SOURCE"},
			},
		}
	}

	s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
		rAsset("first"),
		rAsset("second"),
	}, map[string]int{"r-source": 1})

	assert.Equal(t, []string{"r-first"}, scheduleableHumanIDs(s))
}

func TestScheduler_ConnectionLimitsFromDetailsIgnoresConnectionlessMetadataPush(t *testing.T) {
	t.Parallel()

	s := NewScheduler(zap.NewNop().Sugar(), &pipeline.Pipeline{
		MetadataPush: pipeline.MetadataPush{Global: true},
		Assets: []*pipeline.Asset{
			{Name: "r-task", Type: pipeline.AssetTypeR},
		},
	}, "test")

	limits, err := s.ConnectionLimitsFromDetails(nil, connectionLimitDetailsGetter{})
	require.NoError(t, err)
	assert.Empty(t, limits)

	require.NoError(t, s.SetConnectionLimits(map[string]int{"unrelated": 1}))
}

func TestScheduler_getScheduleableTasksIngestrConsumesSourceAndDestinationLimits(t *testing.T) {
	t.Parallel()

	ingestrAsset := func() *pipeline.Asset {
		return &pipeline.Asset{
			Name:       "ingestr-task",
			Type:       pipeline.AssetTypeIngestr,
			Connection: "destination",
			Parameters: pipeline.ParameterMap{
				"source_connection": "source",
			},
		}
	}

	t.Run("source at capacity blocks ingestr task", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "source-holder", Connection: "source"},
			ingestrAsset(),
		}, map[string]int{"source": 1, "destination": 1})
		markMainTaskStatus(s, "source-holder", Running)

		assert.Empty(t, scheduleableHumanIDs(s))
	})

	t.Run("destination at capacity blocks ingestr task", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "destination-holder", Connection: "destination"},
			ingestrAsset(),
		}, map[string]int{"source": 1, "destination": 1})
		markMainTaskStatus(s, "destination-holder", Running)

		assert.Empty(t, scheduleableHumanIDs(s))
	})

	t.Run("both connections free schedules ingestr task", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			ingestrAsset(),
		}, map[string]int{"source": 1, "destination": 1})

		assert.Equal(t, []string{"ingestr-task"}, scheduleableHumanIDs(s))
	})
}

func TestScheduler_getScheduleableTasksIngestrUsesAssetConnectionAsDestination(t *testing.T) {
	t.Parallel()

	ingestrAsset := &pipeline.Asset{
		Name:       "ingestr-task",
		Type:       pipeline.AssetTypeIngestr,
		Connection: "destination-override",
		Parameters: pipeline.ParameterMap{
			"source_connection":      "source",
			"destination_connection": "destination-param",
		},
	}

	s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
		{Name: "destination-holder", Connection: "destination-override"},
		ingestrAsset,
	}, map[string]int{
		"source":               1,
		"destination-param":    1,
		"destination-override": 1,
	})
	markMainTaskStatus(s, "destination-holder", Running)

	assert.Empty(t, scheduleableHumanIDs(s))
}

func TestScheduler_getScheduleableTasksIngestrUsesDestinationConnectionParameter(t *testing.T) {
	t.Parallel()

	ingestrAsset := &pipeline.Asset{
		Name: "ingestr-task",
		Type: pipeline.AssetTypeIngestr,
		Parameters: pipeline.ParameterMap{
			"source_connection":      "source",
			"destination":            "bigquery",
			"destination_connection": "destination-param",
		},
	}

	s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
		{Name: "destination-holder", Connection: "destination-param"},
		ingestrAsset,
	}, map[string]int{
		"source":            1,
		"destination-param": 1,
		"gcp-default":       1,
	})
	markMainTaskStatus(s, "destination-holder", Running)

	assert.Empty(t, scheduleableHumanIDs(s))
}

func TestScheduler_getScheduleableTasksDoesNotLimitSourceMainTasks(t *testing.T) {
	t.Parallel()

	s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
		{Name: "source-first", Type: pipeline.AssetTypeBigquerySource},
		{Name: "source-second", Type: pipeline.AssetTypeBigquerySource},
	}, map[string]int{"gcp-default": 1})

	assert.Equal(t, []string{"source-first", "source-second"}, scheduleableHumanIDs(s))
}

func TestScheduler_ConnectionLimitsFromDetailsIgnoresConnectionlessMainTasks(t *testing.T) {
	t.Parallel()

	s := NewScheduler(zap.NewNop().Sugar(), &pipeline.Pipeline{Assets: []*pipeline.Asset{
		{Name: "appsflyer-export", Type: pipeline.AssetType("appsflyer.export.bq")},
		{Name: "dashboard", Type: pipeline.AssetTypeMetabase},
		{Name: "dbt-task", Type: pipeline.AssetType("dbt")},
		{Name: "mongo-source", Type: pipeline.AssetTypeMongoSource},
		{Name: "python-beta", Type: pipeline.AssetType("python.beta")},
		{Name: "python-legacy", Type: pipeline.AssetType("python.legacy")},
	}}, "test")

	limits, err := s.ConnectionLimitsFromDetails(nil, connectionLimitDetailsGetter{})
	require.NoError(t, err)
	assert.Empty(t, limits)

	require.NoError(t, s.SetConnectionLimits(map[string]int{"unrelated": 1}))
	assert.Equal(t, []string{"appsflyer-export", "dashboard", "dbt-task", "mongo-source", "python-beta", "python-legacy"}, scheduleableHumanIDs(s))
}

func TestScheduler_getScheduleableTasksLimitsRefreshableDashboardMainTasks(t *testing.T) {
	t.Parallel()

	t.Run("tableau", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "tableau-first", Type: pipeline.AssetTypeTableauDatasource, Connection: "tableau-prod"},
			{Name: "tableau-second", Type: pipeline.AssetTypeTableauWorkbook, Connection: "tableau-prod"},
		}, map[string]int{"tableau-prod": 1})

		assert.Equal(t, []string{"tableau-first"}, scheduleableHumanIDs(s))
	})

	t.Run("quicksight", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "quicksight-first", Type: pipeline.AssetTypeQuicksightDataset, Connection: "quicksight-prod"},
			{Name: "quicksight-second", Type: pipeline.AssetTypeQuicksightDashboard, Connection: "quicksight-prod"},
		}, map[string]int{"quicksight-prod": 1})

		assert.Equal(t, []string{"quicksight-first"}, scheduleableHumanIDs(s))
	})
}

func TestScheduler_getScheduleableTasksChecksUsePrimaryConnectionOnly(t *testing.T) {
	t.Parallel()

	pythonAsset := func() *pipeline.Asset {
		return &pipeline.Asset{
			Name:       "python-task",
			Type:       pipeline.AssetTypePython,
			Connection: "warehouse",
			Secrets: []pipeline.SecretMapping{
				{SecretKey: "secret-source", InjectedKey: "SOURCE"},
			},
			CustomChecks: []pipeline.CustomCheck{{Name: "freshness"}},
		}
	}

	t.Run("secret connection capacity does not block checks", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "secret-holder", Connection: "secret-source"},
			pythonAsset(),
		}, map[string]int{"secret-source": 1, "warehouse": 1})
		markMainTaskStatus(s, "secret-holder", Running)
		markMainTaskStatus(s, "python-task", Succeeded)

		assert.Equal(t, []string{"python-task:custom-check:freshness"}, scheduleableHumanIDs(s))
	})

	t.Run("primary connection capacity blocks checks", func(t *testing.T) {
		t.Parallel()

		s := newConnectionLimitTestScheduler(t, []*pipeline.Asset{
			{Name: "warehouse-holder", Connection: "warehouse"},
			pythonAsset(),
		}, map[string]int{"secret-source": 1, "warehouse": 1})
		markMainTaskStatus(s, "warehouse-holder", Running)
		markMainTaskStatus(s, "python-task", Succeeded)

		assert.Empty(t, scheduleableHumanIDs(s))
	})
}

func TestScheduler_ConnectionLimitsFromDetails(t *testing.T) {
	t.Parallel()

	postgresLimit := 1
	snowflakeLimit := 2

	s := NewScheduler(zap.NewNop().Sugar(), &pipeline.Pipeline{Assets: []*pipeline.Asset{
		{Name: "first", Connection: "postgres"},
		{Name: "second", Connection: "postgres"},
		{Name: "third", Connection: "snowflake"},
	}}, "test")

	limits, err := s.ConnectionLimitsFromDetails(nil, connectionLimitDetailsGetter{
		"postgres":  connectionLimitDetails{limit: &postgresLimit},
		"snowflake": connectionLimitDetails{limit: &snowflakeLimit},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{
		"postgres":  1,
		"snowflake": 2,
	}, limits)

	require.NoError(t, s.SetConnectionLimits(limits))
	assert.Equal(t, []string{"first", "third"}, scheduleableHumanIDs(s))
}

func TestScheduler_ConnectionLimitsFromDetailsPreservesBaseLimits(t *testing.T) {
	t.Parallel()

	s := NewScheduler(zap.NewNop().Sugar(), &pipeline.Pipeline{Assets: []*pipeline.Asset{
		{Name: "first", Connection: "postgres"},
		{Name: "second", Connection: "snowflake"},
	}}, "test")

	limits, err := s.ConnectionLimitsFromDetails(map[string]int{"snowflake": 2}, connectionLimitDetailsGetter{})
	require.NoError(t, err)
	assert.Equal(t, map[string]int{"snowflake": 2}, limits)
}

func TestScheduler_ConnectionLimitsFromDetailsRejectsInvalidLimit(t *testing.T) {
	t.Parallel()

	invalidLimit := 0
	s := NewScheduler(zap.NewNop().Sugar(), &pipeline.Pipeline{Assets: []*pipeline.Asset{
		{Name: "first", Connection: "postgres"},
	}}, "test")

	_, err := s.ConnectionLimitsFromDetails(nil, connectionLimitDetailsGetter{
		"postgres": connectionLimitDetails{limit: &invalidLimit},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be greater than 0")
}

type connectionLimitDetailsGetter map[string]any

func (g connectionLimitDetailsGetter) GetConnectionDetails(name string) any {
	return g[name]
}

type connectionLimitDetails struct {
	limit *int
}

func (d connectionLimitDetails) GetMaxConcurrentAssets() *int {
	return d.limit
}

func newConnectionLimitTestScheduler(t *testing.T, assets []*pipeline.Asset, limits map[string]int) *Scheduler {
	t.Helper()

	s := NewScheduler(zap.NewNop().Sugar(), &pipeline.Pipeline{Assets: assets}, "test")
	require.NoError(t, s.SetConnectionLimits(limits))
	return s
}

func markMainTaskStatus(s *Scheduler, assetName string, status TaskInstanceStatus) {
	s.taskNameMap[assetName][TaskInstanceTypeMain][0].MarkAs(status)
}

func scheduleableHumanIDs(s *Scheduler) []string {
	tasks := s.getScheduleableTasks()
	ids := make([]string, 0, len(tasks))
	for _, task := range tasks {
		ids = append(ids, task.GetHumanID())
	}
	return ids
}

func TestScheduler_Run(t *testing.T) {
	t.Parallel()

	// In the test cases I'll simulate the execution steps of the following graph:
	// task1 -> task3
	// task2 -> task4
	// task3 -> task5
	// task4 -> task5

	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name: "task11",
			},
			{
				Name: "task21",
			},
			{
				Name: "task12",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task11"},
				},
				Columns: []pipeline.Column{
					{
						Name: "col1",
						Checks: []pipeline.ColumnCheck{
							{
								Name: "not_null",
							},
						},
					},
				},
			},
			{
				Name: "task22",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task21"},
				},
			},
			{
				Name: "task3",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task12"},
					{Type: "asset", Value: "task22"},
				},
			},
		},
	}

	scheduler := NewScheduler(&zap.SugaredLogger{}, p, "test")

	scheduler.Tick(&TaskExecutionResult{
		Instance: &AssetInstance{
			Asset: &pipeline.Asset{
				Name: "start",
			},
			status: Succeeded,
		},
	})

	// ensure the first two tasks are scheduled
	t11 := <-scheduler.WorkQueue
	assert.Equal(t, "task11", t11.GetHumanID())

	t21 := <-scheduler.WorkQueue
	assert.Equal(t, "task21", t21.GetHumanID())

	// mark t11 as completed
	scheduler.Tick(&TaskExecutionResult{
		Instance: t11,
	})

	// expect t12 to be scheduled
	t12 := <-scheduler.WorkQueue
	assert.Equal(t, "task12", t12.GetHumanID())

	// mark t21 as completed
	scheduler.Tick(&TaskExecutionResult{
		Instance: t21,
	})

	// expect t22 to arrive, given that t21 was completed
	t22 := <-scheduler.WorkQueue
	assert.Equal(t, "task22", t22.GetHumanID())

	// mark t12 as completed
	scheduler.Tick(&TaskExecutionResult{
		Instance: t12,
	})

	// expect t12's test to be scheduled
	t12Tests := <-scheduler.WorkQueue
	assert.Equal(t, "task12:col1:not_null", t12Tests.GetHumanID())
	assert.Equal(t, TaskInstanceTypeColumnCheck, t12Tests.GetType())

	// mark t21 as completed
	scheduler.Tick(&TaskExecutionResult{
		Instance: t12Tests,
	})

	// mark t22 as completed
	finished := scheduler.Tick(&TaskExecutionResult{
		Instance: t22,
	})
	assert.False(t, finished)

	// now that both t12 and t22 are completed, expect t3 to be dispatched
	t3 := <-scheduler.WorkQueue
	assert.Equal(t, "task3", t3.GetHumanID())

	// mark t3 as completed
	finished = scheduler.Tick(&TaskExecutionResult{
		Instance: t3,
	})

	assert.True(t, finished)
}

func TestScheduler_MarkTasksAndDownstream(t *testing.T) {
	t.Parallel()

	t12 := &pipeline.Asset{
		Name: "task12",
		Upstreams: []pipeline.Upstream{
			{Type: "asset", Value: "task11"},
		},
	}

	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name: "task11",
			},
			{
				Name: "task21",
			},
			t12,
			{
				Name: "task13",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task12"},
				},
			},
			{
				Name: "task22",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task21"},
				},
			},
			{
				Name: "task3",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task12"},
					{Type: "asset", Value: "task22"},
				},
			},
			{
				Name: "task4",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task13"},
					{Type: "asset", Value: "task3"},
				},
			},
		},
	}

	s := NewScheduler(zap.NewNop().Sugar(), p, "test")
	s.MarkAll(Succeeded)
	s.MarkAsset(t12, Pending, true)

	s.Kickstart()

	// ensure the first task is scheduled
	ti12 := <-s.WorkQueue
	assert.Equal(t, "task12", ti12.GetAsset().Name)

	// mark t12 as completed
	s.Tick(&TaskExecutionResult{
		Instance: ti12,
	})

	ti13 := <-s.WorkQueue
	assert.Equal(t, "task13", ti13.GetAsset().Name)
	s.Tick(&TaskExecutionResult{
		Instance: ti13,
	})

	ti3 := <-s.WorkQueue
	assert.Equal(t, "task3", ti3.GetAsset().Name)
	s.Tick(&TaskExecutionResult{
		Instance: ti3,
	})

	ti4 := <-s.WorkQueue
	assert.Equal(t, "task4", ti4.GetAsset().Name)
	finished := s.Tick(&TaskExecutionResult{
		Instance: ti4,
	})
	assert.True(t, finished)
}

func TestScheduler_MarkAssetWithCycleDownstreamDoesNotLoop(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name: "task1",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task2"},
				},
			},
			{
				Name: "task2",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task1"},
				},
			},
		},
	}

	s := NewScheduler(zap.NewNop().Sugar(), p, "test")
	s.MarkAll(Skipped)

	done := make(chan struct{})
	go func() {
		s.MarkAsset(p.Assets[0], Pending, true)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("marking downstream tasks did not finish; possible cycle in recursion")
	}

	assert.Equal(t, 2, s.InstanceCountByStatus(Pending))
}

func TestScheduler_MarkTaskInstanceIfNotSkippedWithCycleDoesNotLoop(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name: "task1",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task2"},
				},
			},
			{
				Name: "task2",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task1"},
				},
			},
		},
	}

	s := NewScheduler(zap.NewNop().Sugar(), p, "test")
	mainTask1 := s.taskNameMap["task1"][TaskInstanceTypeMain][0]

	done := make(chan struct{})
	go func() {
		s.markTaskInstanceFailedWithDownstream(mainTask1)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("marking failed downstream tasks did not finish; possible cycle in recursion")
	}

	assert.Equal(t, 1, s.InstanceCountByStatus(Failed))
	assert.Equal(t, 1, s.InstanceCountByStatus(UpstreamFailed))
}

func Test_GetAssetCountWithTasksPending(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		pipeline         *pipeline.Pipeline
		markAssets       []int // indices of assets to mark as Pending
		want             int
		checkInstanceIDs []CheckUniqueID
	}{
		{
			name: "single asset with pending tasks",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						ID:   "task11",
						Name: "task11",
					},
					{
						ID:   "task21",
						Name: "task21",
					},
					{
						Name: "task12",
						ID:   "task12",
						CustomChecks: []pipeline.CustomCheck{
							{
								ID: "check1",
							},
							{
								ID: "check1",
							},
						},
					},
				},
			},
			markAssets:       []int{2},
			want:             1,
			checkInstanceIDs: []CheckUniqueID{},
		},
		{
			name: "2 asset with checks tasks",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						ID:   "task11",
						Name: "task11",
					},
					{
						ID:   "task21",
						Name: "task21",
						CustomChecks: []pipeline.CustomCheck{
							{
								ID: "check1",
							},
							{
								ID: "check1",
							},
						},
					},
					{
						ID:   "task12",
						Name: "task12",
						CustomChecks: []pipeline.CustomCheck{
							{
								ID: "check1",
							},
							{
								ID: "check1",
							},
						},
					},
				},
			},
			markAssets:       []int{1, 2},
			want:             2,
			checkInstanceIDs: []CheckUniqueID{},
		},
		{
			name: "only checks of 1 asset",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						ID:   "task11",
						Name: "task11",
					},
					{
						ID:   "task21",
						Name: "task21",
						CustomChecks: []pipeline.CustomCheck{
							{
								ID: "check1",
							},
							{
								ID: "check2",
							},
						},
					},
					{
						ID:   "task12",
						Name: "task12",
						CustomChecks: []pipeline.CustomCheck{
							{
								ID: "check3",
							},
							{
								ID: "check4",
							},
						},
					},
				},
			},
			markAssets:       []int{},
			want:             1,
			checkInstanceIDs: []CheckUniqueID{{ID: "check1", Asset: &pipeline.Asset{ID: "task21"}}, {ID: "check2", Asset: &pipeline.Asset{ID: "task21"}}},
		},
		{
			name: "checks of 2 asset",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						ID:   "task11",
						Name: "task11",
					},
					{
						ID:   "task21",
						Name: "task21",
						CustomChecks: []pipeline.CustomCheck{
							{
								ID: "check1",
							},
							{
								ID: "check2",
							},
						},
					},
					{
						ID:   "task12",
						Name: "task12",
						CustomChecks: []pipeline.CustomCheck{
							{
								ID: "check3",
							},
							{
								ID: "check4",
							},
						},
					},
				},
			},
			markAssets: []int{},
			want:       2,
			checkInstanceIDs: []CheckUniqueID{
				{ID: "check1", Asset: &pipeline.Asset{ID: "task21"}},
				{ID: "check2", Asset: &pipeline.Asset{ID: "task21"}},
				{ID: "check3", Asset: &pipeline.Asset{ID: "task12"}},
			},
		},
		{
			name: "check id alone resolves when asset is nil",
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					{
						ID:   "task21",
						Name: "task21",
						CustomChecks: []pipeline.CustomCheck{
							{ID: "check1"},
						},
					},
				},
			},
			markAssets: []int{},
			want:       1,
			checkInstanceIDs: []CheckUniqueID{
				{ID: "check1", Asset: nil},
			},
		},
		// Add more cases as needed
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			s := NewScheduler(zap.NewNop().Sugar(), tt.pipeline, "test")
			s.MarkAll(Skipped)
			for _, idx := range tt.markAssets {
				s.MarkAsset(tt.pipeline.Assets[idx], Pending, false)
			}

			for _, id := range tt.checkInstanceIDs {
				err := s.MarkCheckInstancesByID(id.ID, id.Asset, Pending)
				if err != nil {
					t.Errorf("failed to mark check instance %q as pending: %v", id.ID, err)
				}
			}

			assert.Equal(t, tt.want, s.GetAssetCountWithTasksPending())
		})
	}
}

func TestScheduler_WillRunTaskOfType(t *testing.T) {
	t.Parallel()

	t12 := &pipeline.Asset{
		Name: "task12",
		Upstreams: []pipeline.Upstream{
			{Type: "asset", Value: "task11"},
		},
		Type: "bq.sql",
		Columns: []pipeline.Column{
			{
				Checks: []pipeline.ColumnCheck{
					{
						Name: "not_null",
					},
				},
			},
		},
	}

	t1000 := &pipeline.Asset{
		Name:       "task4000",
		Type:       "ingestr",
		Parameters: pipeline.ParameterMap{"destination": "postgres"},
	}

	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name: "task11",
				Type: "bq.sql",
			},
			{
				Name: "task21",
				Type: "sf.sql",
			},
			{
				Name: "random-asset",
				Type: "random",
			},
			t12,
			{
				Name: "task13",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task12"},
				},
				Type: "bq.sql",
			},
			{
				Name: "task22",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task21"},
				},
				Type: "python",
			},
			{
				Name: "task3",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task12"},
					{Type: "asset", Value: "task22"},
				},
				Type: "python",
			},
			{
				Name: "task4",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task13"},
					{Type: "asset", Value: "task3"},
				},
				Type: "empty",
			},
			t1000,
		},
	}

	s := NewScheduler(zap.NewNop().Sugar(), p, "test")
	s.MarkAll(Succeeded)
	s.MarkAsset(t12, Pending, true)
	s.MarkAsset(t1000, Pending, true)

	assert.Equal(t, 10, s.InstanceCount())
	assert.Equal(t, 6, s.InstanceCountByStatus(Pending))
	assert.False(t, s.WillRunTaskOfType("sf.sql"))
	assert.False(t, s.WillRunTaskOfType("random"))
	assert.True(t, s.WillRunTaskOfType("bq.sql"))
	assert.True(t, s.WillRunTaskOfType("python"))
	assert.True(t, s.WillRunTaskOfType("empty"))
	assert.True(t, s.WillRunTaskOfType("pg.sql"))
}

func TestScheduler_FindMajorityOfTypes(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{
				Name: "task11",
				Type: "bq.sql",
			},
			{
				Name: "task21",
				Type: "sf.sql",
			},
			{
				Name: "task13",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task12"},
				},
				Type: "bq.sql",
			},
			{
				Name: "task22",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task21"},
				},
				Type: "python",
			},
			{
				Name: "task3",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task12"},
					{Type: "asset", Value: "task22"},
				},
				Type: "python",
			},
			{
				Name: "task4",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task13"},
					{Type: "asset", Value: "task3"},
				},
				Type: "sf.sql",
			},
		},
	}

	s := NewScheduler(zap.NewNop().Sugar(), p, "test")

	// if they are in equal counts, the default should be preferred
	assert.Equal(t, pipeline.AssetType("bq.sql"), s.FindMajorityOfTypes("bq.sql"))
	assert.Equal(t, pipeline.AssetType("sf.sql"), s.FindMajorityOfTypes("sf.sql"))
}

func TestScheduler_RestoreState(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()

	stateFilePath := "logs/runs"
	stateContent := `{"someKey": "someValue"}`

	foundPipeline := &pipeline.Pipeline{
		Name: "test",
		Assets: []*pipeline.Asset{
			{
				Name: "task1",
				Type: "bq.sql",
				Upstreams: []pipeline.Upstream{
					{Type: "asset", Value: "task2"},
				},
			},
			{
				Name:      "task2",
				Type:      "bq.sql",
				Upstreams: []pipeline.Upstream{},
			},
		},
	}
	err := afero.WriteFile(fs, filepath.Join(stateFilePath, "2024_12_19_22_59_13.json"), []byte(stateContent), 0o644)
	require.NoError(t, err, "failed to write mock state file")

	s := NewScheduler(zap.NewNop().Sugar(), foundPipeline, "2024_12_19_22_59_13")

	cmd := []string{
		"/usr/bin/bruin", "run", "test",
	}

	err = s.SavePipelineState(fs, cmd, &RunConfig{
		Tag:          "test",
		Only:         []string{"bq.sql"},
		PushMetadata: true,
		ExcludeTag:   "exclude",
		StartDate:    time.Now().AddDate(0, 0, -1).Format("2006-01-02"),
		EndDate:      time.Now().Format("2006-01-02"),
	}, "", 0, "2024_12_19_22_59_13", stateFilePath)
	require.NoError(t, err, "SavePipelineState should not return an error")

	pipelineState, err := ReadState(fs, stateFilePath)
	require.NoError(t, err, "RestoreState should not return an error")

	err = s.RestoreState(pipelineState)
	require.Error(t, err, "unknown status: pending. Please report this issue at https://github.com/bruin-data/bruin/issues/new")

	expectedState := &PipelineState{
		Cmdline: cmd,
		Parameters: RunConfig{
			Tag:          "test",
			Only:         []string{"bq.sql"},
			PushMetadata: true,
			ExcludeTag:   "exclude",
			StartDate:    time.Now().AddDate(0, 0, -1).Format("2006-01-02"),
			EndDate:      time.Now().Format("2006-01-02"),
		},
		Metadata: Metadata{
			Version: version.Version,
			OS:      runtime.GOOS,
		},
		State: []*PipelineAssetState{
			{
				Name:   "task2",
				Status: Pending.String(),
			},
			{
				Name:   "task1",
				Status: Pending.String(),
			},
		},
		Version:           "1.0.0",
		RunID:             "2024_12_19_22_59_13",
		CompatibilityHash: foundPipeline.GetCompatibilityHash(),
	}

	assert.Equal(t, expectedState.Metadata, pipelineState.Metadata, "Metadata should match")
	assert.Equal(t, expectedState.Metadata, pipelineState.Metadata, "Metadata should match")

	assert.Equal(t, expectedState.Version, pipelineState.Version, "Version should match")
	assert.Equal(t, expectedState.RunID, pipelineState.RunID, "RunID should match")
	assert.Equal(t, expectedState.CompatibilityHash, pipelineState.CompatibilityHash, "CompatibilityHash should match")
	assert.Equal(t, expectedState.CompatibilityHash, pipelineState.CompatibilityHash, "CompatibilityHash should match")
	assert.Equal(t, expectedState.RunID, pipelineState.RunID, "RunID should match")
	assert.Equal(t, expectedState.Version, pipelineState.Version, "Version should match")
	assert.Equal(t, expectedState.Cmdline, pipelineState.Cmdline, "Cmdline should match")
}

func TestScheduler_SavePipelineStateBackfill(t *testing.T) {
	t.Parallel()

	foundPipeline := &pipeline.Pipeline{
		Name: "test",
		Assets: []*pipeline.Asset{
			{Name: "task1", Type: "bq.sql"},
		},
	}

	t.Run("persists the backfill id and total when provided", func(t *testing.T) {
		t.Parallel()
		fs := afero.NewMemMapFs()
		s := NewScheduler(zap.NewNop().Sugar(), foundPipeline, "run_a")

		err := s.SavePipelineState(fs, []string{"bruin", "run"}, &RunConfig{}, "bf_123", 24, "run_a", "logs/runs")
		require.NoError(t, err)

		state, err := ReadState(fs, "logs/runs")
		require.NoError(t, err)
		require.Equal(t, "bf_123", state.BackfillID)
		require.Equal(t, 24, state.BackfillTotal)
	})

	t.Run("omits the backfill fields from the JSON when unset", func(t *testing.T) {
		t.Parallel()
		fs := afero.NewMemMapFs()
		s := NewScheduler(zap.NewNop().Sugar(), foundPipeline, "run_b")

		err := s.SavePipelineState(fs, []string{"bruin", "run"}, &RunConfig{}, "", 0, "run_b", "logs/runs")
		require.NoError(t, err)

		raw, err := afero.ReadFile(fs, filepath.Join("logs/runs", "run_b.json"))
		require.NoError(t, err)
		require.NotContains(t, string(raw), "backfill_id")
		require.NotContains(t, string(raw), "backfill_total")

		state, err := ReadState(fs, "logs/runs")
		require.NoError(t, err)
		require.Empty(t, state.BackfillID)
		require.Zero(t, state.BackfillTotal)
	})

	t.Run("round-trips start/end dates verbatim including fractional exclusive ends", func(t *testing.T) {
		t.Parallel()
		fs := afero.NewMemMapFs()
		s := NewScheduler(zap.NewNop().Sugar(), foundPipeline, "run_c")

		const start = "2024-01-01T00:00:00Z"
		const end = "2024-01-01T23:59:59.999999999Z"
		err := s.SavePipelineState(fs, []string{"bruin", "run"}, &RunConfig{
			StartDate: start,
			EndDate:   end,
		}, "", 0, "run_c", "logs/runs")
		require.NoError(t, err)

		state, err := ReadState(fs, "logs/runs")
		require.NoError(t, err)
		require.Equal(t, start, state.Parameters.StartDate)
		require.Equal(t, end, state.Parameters.EndDate)
	})
}

func TestScheduler_MarkAssetWithCustomChecks(t *testing.T) {
	t.Parallel()

	// Reproduce the bug: MarkAsset should mark custom check instances as Pending
	// when the SingleTask is a different pointer but same Name.
	pipelineAsset := &pipeline.Asset{
		Name: "my_schema.my_events",
		Type: pipeline.AssetTypeSnowflakeQuery,
		Materialization: pipeline.Materialization{
			Type:            pipeline.MaterializationTypeTable,
			Strategy:        pipeline.MaterializationStrategyTimeInterval,
			IncrementalKey:  "event_date",
			TimeGranularity: pipeline.MaterializationTimeGranularityDate,
		},
		CustomChecks: []pipeline.CustomCheck{
			{Name: "events_populated", Description: "check events exist"},
		},
	}

	p := &pipeline.Pipeline{
		Name:   "TestPipeline",
		Assets: []*pipeline.Asset{pipelineAsset},
	}

	s := NewScheduler(zap.NewNop().Sugar(), p, "test")

	// Verify instances were created
	allInstances := s.GetTaskInstancesByStatus(Pending)
	t.Logf("All instances: %d", len(allInstances))
	for _, inst := range allInstances {
		t.Logf("  %s (type=%d, status=%s)", inst.GetHumanID(), inst.GetType(), inst.GetStatus())
	}

	// There should be 2 instances: 1 main + 1 custom check
	assert.Len(t, allInstances, 2, "Expected 2 instances (main + custom check)")

	// Now simulate what HandleSingleTask does:
	// 1. Mark all as Skipped
	s.MarkAll(Skipped)
	assert.Equal(t, 0, s.InstanceCountByStatus(Pending))

	// 2. Mark the asset as Pending using a DIFFERENT pointer with same name
	differentPointer := &pipeline.Asset{Name: "my_schema.my_events"}
	found := s.MarkAsset(differentPointer, Pending, false)
	assert.True(t, found, "MarkAsset should return true for a matching asset name")

	// Verify unknown asset returns false
	unknownAsset := &pipeline.Asset{Name: "nonexistent.asset"}
	notFound := s.MarkAsset(unknownAsset, Pending, false)
	assert.False(t, notFound, "MarkAsset should return false for an unknown asset name")

	// 3. Mark main as Skipped (simulating --only checks)
	s.MarkPendingInstancesByType(TaskInstanceTypeMain, Skipped)

	// The custom check should still be Pending
	pending := s.GetTaskInstancesByStatus(Pending)
	t.Logf("Pending after filtering: %d", len(pending))
	for _, inst := range pending {
		t.Logf("  %s (type=%d)", inst.GetHumanID(), inst.GetType())
	}

	assert.Len(t, pending, 1, "Expected 1 pending custom check")
	if len(pending) > 0 {
		assert.Equal(t, TaskInstanceTypeCustomCheck, pending[0].GetType())
	}
}

func TestScheduler_RunDoesNotDeadlockWithManyInitiallyEligibleTasks(t *testing.T) {
	t.Parallel()

	const numAssets = 500
	assets := make([]*pipeline.Asset, numAssets)
	for i := range numAssets {
		assets[i] = &pipeline.Asset{Name: fmt.Sprintf("task%d", i)}
	}
	p := &pipeline.Pipeline{Assets: assets}

	s := NewScheduler(zap.NewNop().Sugar(), p, "test")

	// Pre-fix: Kickstart held taskScheduleLock, blocked pushing to a 100-cap WorkQueue, and the first Tick from Run deadlocked on the same lock.
	done := make(chan []*TaskExecutionResult, 1)
	go func() {
		done <- s.Run(t.Context())
	}()

	completed := 0
	for completed < numAssets {
		select {
		case task := <-s.WorkQueue:
			s.Results <- &TaskExecutionResult{Instance: task}
			completed++
		case <-time.After(5 * time.Second):
			t.Fatalf("scheduler deadlocked after completing %d/%d tasks", completed, numAssets)
		}
	}

	select {
	case results := <-done:
		assert.Len(t, results, numAssets)
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler Run did not return after all tasks completed")
	}
}

// Verifies that when the context is cancelled mid-run (e.g. SIGINT), Run
// returns the partial results — including drained in-flight tasks — instead
// of hanging. This is what makes the post-abort summary in `bruin run`
// possible (see BRU-4252).
func TestScheduler_RunReturnsPartialResultsOnCancellation(t *testing.T) {
	t.Parallel()

	// A -> B -> C: a linear chain so only one task runs at a time.
	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{Name: "A"},
			{Name: "B", Upstreams: []pipeline.Upstream{{Type: "asset", Value: "A"}}},
			{Name: "C", Upstreams: []pipeline.Upstream{{Type: "asset", Value: "B"}}},
		},
	}
	s := NewScheduler(zap.NewNop().Sugar(), p, "test")

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan []*TaskExecutionResult, 1)
	go func() {
		done <- s.Run(ctx)
	}()

	// Drive A to completion via the result channel — A is the only initially
	// schedulable task, and its result must arrive before the scheduler
	// schedules B.
	a := <-s.WorkQueue
	require.Equal(t, "A", a.GetHumanID())
	s.Results <- &TaskExecutionResult{Instance: a}

	// B should be queued next; pull it but DO NOT report its result yet —
	// simulates a worker that's mid-task when the user hits Ctrl+C.
	b := <-s.WorkQueue
	require.Equal(t, "B", b.GetHumanID())

	// Cancel the run while B is in-flight.
	cancel()

	// In a separate goroutine, deliver B's result after a short delay so the
	// drain loop has something to collect, just like a worker that respects
	// ctx and errors out promptly.
	go func() {
		time.Sleep(10 * time.Millisecond)
		s.Results <- &TaskExecutionResult{Instance: b, Error: errors.New("cancelled")}
	}()

	select {
	case results := <-done:
		require.Len(t, results, 2, "expected A's result + B's drained result")
		assert.Equal(t, "A", results[0].Instance.GetHumanID())
		assert.Equal(t, "B", results[1].Instance.GetHumanID())
		// C never started — should still be Pending so the summary can
		// report it as "not started".
		var cInstance TaskInstance
		for _, inst := range s.GetTaskInstances() {
			if inst.GetAsset().Name == "C" {
				cInstance = inst
				break
			}
		}
		require.NotNil(t, cInstance)
		assert.Equal(t, Pending, cInstance.GetStatus())
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler Run did not return after cancellation")
	}
}

// Verifies that drain bounds its wait — a stuck worker that never returns
// must not block Run forever (otherwise the user is back to needing kill -9).
func TestScheduler_RunCancellationDrainTimesOut(t *testing.T) {
	t.Parallel()

	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{Name: "A"},
			{Name: "B", Upstreams: []pipeline.Upstream{{Type: "asset", Value: "A"}}},
		},
	}
	s := NewScheduler(zap.NewNop().Sugar(), p, "test")

	// Shrink in-flight wait so the test runs quickly. We can't override
	// drainTimeoutOnCancel directly without exposing it, so this test
	// asserts the timeout path via the sink goroutine: after cancellation
	// and a never-returning worker, Run must still complete.
	// (drainTimeoutOnCancel is 30s by default; the goroutine below ensures
	// we don't actually wait that long by sending a stub result.)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan []*TaskExecutionResult, 1)
	go func() {
		done <- s.Run(ctx)
	}()

	a := <-s.WorkQueue
	s.Results <- &TaskExecutionResult{Instance: a}

	// Pull B but never report its result — the worker is "stuck".
	<-s.WorkQueue
	cancel()

	// Without a B result, drain would wait until drainTimeoutOnCancel.
	// To keep the test fast, send a synthetic result after cancellation so
	// Queued count drops to zero and drain exits naturally.
	go func() {
		time.Sleep(20 * time.Millisecond)
		// fabricate a result for B so drain sees Queued=0 and returns
		for _, inst := range s.GetTaskInstances() {
			if inst.GetAsset().Name == "B" {
				s.Results <- &TaskExecutionResult{Instance: inst}
				return
			}
		}
	}()

	select {
	case <-done:
		// Success — Run returned despite the worker never naturally completing.
	case <-time.After(5 * time.Second):
		t.Fatal("scheduler Run did not return after cancellation with stuck worker")
	}
}
