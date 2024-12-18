package scheduler

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/state"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

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

	scheduler := NewScheduler(&zap.SugaredLogger{}, p, state.NewState("test", "test", map[string]string{}))

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

	s := NewScheduler(zap.NewNop().Sugar(), p, state.NewState("test", "test", map[string]string{}))
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
		Parameters: map[string]string{"destination": "postgres"},
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

	s := NewScheduler(zap.NewNop().Sugar(), p, state.NewState("test", "test", map[string]string{}))
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

	s := NewScheduler(zap.NewNop().Sugar(), p, state.NewState("test", "test", map[string]string{}))

	// if they are in equal counts, the default should be preferred
	assert.Equal(t, pipeline.AssetType("bq.sql"), s.FindMajorityOfTypes("bq.sql"))
	assert.Equal(t, pipeline.AssetType("sf.sql"), s.FindMajorityOfTypes("sf.sql"))
}
