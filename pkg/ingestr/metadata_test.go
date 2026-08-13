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

type stubConn struct{ present bool }

func (s stubConn) GetConnection(string) any {
	if s.present {
		return struct{}{}
	}
	return nil
}

func TestIngestrMetadataPushOperator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantError   bool
		wantPushed  bool
		wantSkipped bool
		connPresent bool
		destination string
		configs     map[pipeline.AssetType]executor.Config
	}{
		{
			name:        "delegates to destination metadata pusher",
			destination: "bigquery",
			connPresent: true,
			wantPushed:  true,
			configs: map[pipeline.AssetType]executor.Config{
				pipeline.AssetTypeBigqueryQuery: {},
			},
		},
		{
			name:        "propagates destination pusher error",
			destination: "bigquery",
			connPresent: true,
			wantError:   true,
			wantPushed:  true,
			configs: map[pipeline.AssetType]executor.Config{
				pipeline.AssetTypeBigqueryQuery: {},
			},
		},
		{
			name:        "skips when destination connection is not configured",
			destination: "bigquery",
			wantSkipped: true,
			configs: map[pipeline.AssetType]executor.Config{
				pipeline.AssetTypeBigqueryQuery: {},
			},
		},
		{
			name:        "skips when destination has no metadata pusher",
			destination: "duckdb",
			wantSkipped: true,
			configs:     map[pipeline.AssetType]executor.Config{},
		},
		{
			name:        "skips when destination pusher is a no-op",
			destination: "duckdb",
			wantSkipped: true,
			configs: map[pipeline.AssetType]executor.Config{
				pipeline.AssetTypeDuckDBQuery: {
					scheduler.TaskInstanceTypeMetadataPush: executor.NoOpOperator{},
				},
			},
		},
		{
			name:        "errors on unknown destination",
			destination: "not-a-real-destination",
			wantError:   true,
			configs:     map[pipeline.AssetType]executor.Config{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			pushed := false
			if cfg, ok := tt.configs[pipeline.AssetTypeBigqueryQuery]; ok {
				cfg[scheduler.TaskInstanceTypeMetadataPush] = checker{fn: func() error {
					pushed = true
					if tt.wantError {
						return errors.New("push failed")
					}
					return nil
				}}
			}

			op := NewMetadataPushOperator(&tt.configs, stubConn{present: tt.connPresent})
			asset := scheduler.AssetInstance{
				Pipeline: &pipeline.Pipeline{},
				Asset: &pipeline.Asset{
					Type:       "ingestr",
					Parameters: pipeline.ParameterMap{"destination": tt.destination},
				},
			}

			err := op.Run(context.Background(), &asset)
			if tt.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.wantPushed, pushed)
			require.Equal(t, tt.wantSkipped, asset.GetStatus() == scheduler.Skipped)
		})
	}
}
