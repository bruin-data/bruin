package executor

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func TestConcurrent_Start(t *testing.T) {
	t.Parallel()

	t11 := &pipeline.Asset{
		Name: "task11",
		Type: "test",
	}

	t21 := &pipeline.Asset{
		Name: "task21",
		Type: "test",
	}

	t12 := &pipeline.Asset{
		Name:      "task12",
		Type:      "test",
		DependsOn: []string{"task11"},
	}

	t22 := &pipeline.Asset{
		Name:      "task22",
		Type:      "test",
		DependsOn: []string{"task21"},
	}

	t3 := &pipeline.Asset{
		Name:      "task3",
		Type:      "test",
		DependsOn: []string{"task12", "task22"},
	}

	p := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{t11, t21, t12, t22, t3},
	}

	mockOperator := new(mockOperator)
	for _, a := range p.Assets {
		a := a
		mockOperator.On("Run", mock.Anything, mock.MatchedBy(func(ti scheduler.TaskInstance) bool {
			return ti.GetAsset().Name == a.Name
		})).
			Return(nil).
			Once()
	}

	logger := zap.NewNop().Sugar()
	s := scheduler.NewScheduler(logger, p)
	assert.Equal(t, 5, s.InstanceCount())

	ops := map[pipeline.AssetType]Config{
		"test": {
			scheduler.TaskInstanceTypeMain: mockOperator,
		},
	}

	ex := NewConcurrent(logger, ops, 8)
	ex.Start(s.WorkQueue, s.Results)

	results := s.Run(context.Background())
	assert.Len(t, results, len(p.Assets))

	mockOperator.AssertExpectations(t)
}
