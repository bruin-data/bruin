package executor

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockOperator struct {
	mock.Mock
}

func (d *mockOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	args := d.Called(ctx, ti)
	return args.Error(0)
}

func TestLocal_RunSingleTask(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "task1",
		Type: "test",
	}
	instance := &scheduler.AssetInstance{
		Asset: asset,
	}

	t.Run("simple instance is executed successfully", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)
		mockOperator.On("Run", mock.Anything, instance).
			Return(nil)

		l := Sequential{
			TaskTypeMap: map[pipeline.AssetType]Config{
				"test": {
					scheduler.TaskInstanceTypeMain: mockOperator,
				},
			},
		}

		err := l.RunSingleTask(context.Background(), instance)

		require.NoError(t, err)
		mockOperator.AssertExpectations(t)
	})

	t.Run("missing instance is rejected", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)

		l := Sequential{
			TaskTypeMap: map[pipeline.AssetType]Config{
				"some-other-instance": {
					scheduler.TaskInstanceTypeMain: mockOperator,
				},
			},
		}

		err := l.RunSingleTask(context.Background(), instance)

		require.Error(t, err)
		mockOperator.AssertExpectations(t)
	})

	t.Run("missing instance is rejected", func(t *testing.T) {
		t.Parallel()

		mockOperator := new(mockOperator)
		mockOperator.On("Run", mock.Anything, instance).
			Return(errors.New("some error occurred"))

		l := Sequential{
			TaskTypeMap: map[pipeline.AssetType]Config{
				"test": {
					scheduler.TaskInstanceTypeMain: mockOperator,
				},
			},
		}

		err := l.RunSingleTask(context.Background(), instance)

		require.Error(t, err)
		mockOperator.AssertExpectations(t)
	})
}
