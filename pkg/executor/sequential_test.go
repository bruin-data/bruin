package executor

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockOperator struct {
	mock.Mock
}

type operatorFunc func(context.Context, scheduler.TaskInstance) error

func (f operatorFunc) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return f(ctx, ti)
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

		err := l.RunSingleTask(t.Context(), instance)

		require.NoError(t, err)
		mockOperator.AssertExpectations(t)
	})

	t.Run("full refresh restriction is applied before dispatch", func(t *testing.T) {
		t.Parallel()

		restricted := true
		restrictedAsset := &pipeline.Asset{
			Name:              "restricted-task",
			Type:              "test",
			RefreshRestricted: &restricted,
		}
		restrictedInstance := &scheduler.AssetInstance{Asset: restrictedAsset}
		mockOperator := new(mockOperator)
		mockOperator.On("Run", mock.MatchedBy(func(ctx context.Context) bool {
			fullRefresh, ok := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
			return ok && !fullRefresh
		}), restrictedInstance).Return(nil)
		l := Sequential{
			TaskTypeMap: map[pipeline.AssetType]Config{
				"test": {
					scheduler.TaskInstanceTypeMain: mockOperator,
				},
			},
		}
		var output bytes.Buffer
		ctx := context.WithValue(t.Context(), pipeline.RunConfigFullRefresh, true)
		ctx = context.WithValue(ctx, KeyPrinter, &output)

		err := l.RunSingleTask(ctx, restrictedInstance)

		require.NoError(t, err)
		require.Equal(t, "Warning: full refresh is restricted for asset \"restricted-task\"; running incrementally.\n", output.String())
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

		err := l.RunSingleTask(t.Context(), instance)

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

		err := l.RunSingleTask(t.Context(), instance)

		require.Error(t, err)
		mockOperator.AssertExpectations(t)
	})
}

func TestSequential_RunSingleTaskTimeout(t *testing.T) {
	t.Parallel()

	t.Run("configured timeout fails the asset with a clear error", func(t *testing.T) {
		t.Parallel()

		asset := &pipeline.Asset{
			Name:    "dataset.slow_asset",
			Type:    "test",
			Timeout: pipeline.DurationSeconds(25 * time.Millisecond),
		}
		instance := &scheduler.AssetInstance{Asset: asset}
		operator := operatorFunc(func(ctx context.Context, _ scheduler.TaskInstance) error {
			<-ctx.Done()
			return nil
		})
		executor := Sequential{
			TaskTypeMap: map[pipeline.AssetType]Config{
				"test": {scheduler.TaskInstanceTypeMain: operator},
			},
		}

		err := executor.RunSingleTask(t.Context(), instance)

		var timeoutErr *AssetTimeoutError
		require.ErrorAs(t, err, &timeoutErr)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, "dataset.slow_asset", timeoutErr.AssetName)
		require.Equal(t, 25*time.Millisecond, timeoutErr.Timeout)
		require.Equal(t, `asset "dataset.slow_asset" timed out after 25ms`, err.Error())
	})

	t.Run("an earlier run deadline is not reported as an asset timeout", func(t *testing.T) {
		t.Parallel()

		asset := &pipeline.Asset{
			Name:    "dataset.slow_asset",
			Type:    "test",
			Timeout: pipeline.DurationSeconds(time.Hour),
		}
		instance := &scheduler.AssetInstance{Asset: asset}
		operator := operatorFunc(func(ctx context.Context, _ scheduler.TaskInstance) error {
			<-ctx.Done()
			return ctx.Err()
		})
		executor := Sequential{
			TaskTypeMap: map[pipeline.AssetType]Config{
				"test": {scheduler.TaskInstanceTypeMain: operator},
			},
		}
		ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
		defer cancel()

		err := executor.RunSingleTask(ctx, instance)

		require.ErrorIs(t, err, context.DeadlineExceeded)
		var timeoutErr *AssetTimeoutError
		require.NotErrorAs(t, err, &timeoutErr)
	})
}
