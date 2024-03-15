package ingestr

import (
	"context"
	"errors"
	"testing"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

type checker struct {
	fn func() error
}

func (c checker) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	return c.fn()
}

func TestColumnCheckOperatorOperator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantError bool
		configs   map[pipeline.AssetType]executor.Config
	}{
		{
			wantError: true,
			configs: map[pipeline.AssetType]executor.Config{
				pipeline.AssetTypePostgresQuery: {
					scheduler.TaskInstanceTypeColumnCheck: checker{
						fn: func() error {
							return errors.New("some failed check")
						},
					},
				},
			},
		},
		{
			wantError: true,
			configs:   map[pipeline.AssetType]executor.Config{},
		},
		{
			wantError: false,
			configs: map[pipeline.AssetType]executor.Config{
				pipeline.AssetTypePostgresQuery: {
					scheduler.TaskInstanceTypeColumnCheck: checker{
						fn: func() error {
							return nil
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("Run", func(t *testing.T) {
			t.Parallel()
			op := IngestrCheckOperator{
				configs: &tt.configs,
			}

			asset := scheduler.AssetInstance{
				Asset: &pipeline.Asset{
					Type:            "ingestr",
					ExecutableFile:  pipeline.ExecutableFile{},
					DefinitionFile:  pipeline.TaskDefinitionFile{},
					Materialization: pipeline.Materialization{},
					Parameters: map[string]string{
						"destination": "postgres",
					},
				},
			}

			err := op.Run(context.Background(), &asset)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCustomColumnCheckOperatorOperator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantError bool
		configs   map[pipeline.AssetType]executor.Config
	}{
		{
			wantError: true,
			configs: map[pipeline.AssetType]executor.Config{
				pipeline.AssetTypePostgresQuery: {
					scheduler.TaskInstanceTypeCustomCheck: checker{
						fn: func() error {
							return errors.New("some failed check")
						},
					},
				},
			},
		},
		{
			wantError: true,
			configs:   map[pipeline.AssetType]executor.Config{},
		},
		{
			wantError: false,
			configs: map[pipeline.AssetType]executor.Config{
				pipeline.AssetTypePostgresQuery: {
					scheduler.TaskInstanceTypeCustomCheck: checker{
						fn: func() error {
							return nil
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run("Run", func(t *testing.T) {
			t.Parallel()
			op := &IngestrCustomCheckOperator{
				configs: &tt.configs,
			}

			asset := scheduler.AssetInstance{
				Asset: &pipeline.Asset{
					Type:            "ingestr",
					ExecutableFile:  pipeline.ExecutableFile{},
					DefinitionFile:  pipeline.TaskDefinitionFile{},
					Materialization: pipeline.Materialization{},
					Parameters: map[string]string{
						"destination": "postgres",
					},
				},
			}

			err := op.Run(context.Background(), &asset)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
