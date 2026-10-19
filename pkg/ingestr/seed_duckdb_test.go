//go:build !bruin_no_duckdb

package ingestr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	duck "github.com/bruin-data/bruin/pkg/duckdb"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

type seedProcessRunner func(context.Context) error

func (run seedProcessRunner) RunIngestr(ctx context.Context, _ []string, _ []string, _ *git.Repo) error {
	return run(ctx)
}

func TestSeedOperatorDuckDBProcessHandoff(t *testing.T) {
	t.Parallel()
	runnerFailure := errors.New("seed process failed")
	for _, tt := range []struct {
		name string
		err  error
	}{
		{name: "success"},
		{name: "failure", err: runnerFailure},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := duck.Config{Path: filepath.Join(t.TempDir(), "seed.db")}
			ctx, cleanup := duck.WithConnectionReuse(t.Context())
			t.Cleanup(cleanup)
			conn, err := duck.NewEphemeralConnection(cfg)
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, "CREATE TABLE numbers AS SELECT 7 AS n")
			require.NoError(t, err)

			runChild := func(ctx context.Context) ([]byte, error) {
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestSeedDuckDBProcessHelper$")
				cmd.Env = append(os.Environ(), "BRUIN_TEST_SEED_DUCKDB_PATH="+cfg.Path)
				return cmd.CombinedOutput()
			}
			output, err := runChild(ctx)
			require.Error(t, err)
			require.Contains(t, string(output), "IO Error", "native SQL must retain the file lock")

			destination := new(mockConnection)
			destination.On("GetIngestrURI").Return(cfg.GetIngestrURI(), nil)
			op := &SeedOperator{
				conn: &simpleConnectionFetcher{
					connections: map[string]*mockConnection{"duck": destination},
				},
				finder: new(mockFinder),
				runner: seedProcessRunner(func(ctx context.Context) error {
					output, err := runChild(ctx)
					if err != nil {
						return fmt.Errorf("seed subprocess: %w: %s", err, output)
					}
					return tt.err
				}),
			}
			ti := &scheduler.AssetInstance{
				Pipeline: &pipeline.Pipeline{},
				Asset: &pipeline.Asset{
					Name:       "numbers",
					Type:       pipeline.AssetTypeDuckDBSeed,
					Connection: "duck",
					Parameters: pipeline.ParameterMap{"path": "seed.csv"},
				},
			}
			ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, false)
			err = op.Run(ctx, ti)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
			}

			var total int
			require.NoError(t, conn.QueryRowContext(ctx, "SELECT CAST(SUM(n) AS BIGINT) FROM numbers").Scan(&total))
			require.Equal(t, 48, total, "native SQL must resume after the seed process returns")
		})
	}
}

func TestSeedDuckDBProcessHelper(t *testing.T) {
	t.Parallel()
	path := os.Getenv("BRUIN_TEST_SEED_DUCKDB_PATH")
	if path == "" {
		return
	}
	conn, err := duck.NewEphemeralConnection(duck.Config{Path: path})
	require.NoError(t, err)
	_, err = conn.ExecContext(t.Context(), "INSERT INTO numbers VALUES (41)")
	require.NoError(t, err)
}
