package autocreate

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingOperator struct {
	assets      []*pipeline.Asset
	fullRefresh []bool
	err         error
}

func (r *recordingOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	r.assets = append(r.assets, ti.GetAsset())
	fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
	r.fullRefresh = append(r.fullRefresh, fullRefresh)
	return r.err
}

type fakeChecker struct {
	exists  bool
	err     error
	checked []string
}

func (f *fakeChecker) RelationExists(_ context.Context, tableName string) (bool, error) {
	f.checked = append(f.checked, tableName)
	return f.exists, f.err
}

type connections map[string]any

func (c connections) GetConnection(name string) any {
	return c[name]
}

// wrap is Wrap with a lock directory of the test's own, so parallel tests do
// not wait for each other's first-run locks.
func wrap(t *testing.T, inner executor.Operator, conns connections) *Operator {
	t.Helper()

	op := Wrap(inner, conns)
	op.lockDir = t.TempDir()
	return op
}

func incrementalAsset(strategy pipeline.MaterializationStrategy) *pipeline.Asset {
	return &pipeline.Asset{
		Name:       "analytics.orders",
		Type:       pipeline.AssetTypeDuckDBQuery,
		Connection: "warehouse",
		Materialization: pipeline.Materialization{
			Type:     pipeline.MaterializationTypeTable,
			Strategy: strategy,
		},
	}
}

// run runs the asset the way the executor does, which stores in the context
// whether the asset is fully refreshed in this run.
func run(t *testing.T, op *Operator, asset *pipeline.Asset, runFullRefresh bool) string {
	t.Helper()

	var out bytes.Buffer
	ctx := context.WithValue(context.Background(), executor.KeyPrinter, &out)
	ctx = context.WithValue(ctx, pipeline.RunConfigFullRefresh, asset.FullRefreshEnabled(runFullRefresh))
	ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{Name: "p"}, Asset: asset}
	require.NoError(t, op.Run(ctx, ti))

	return out.String()
}

func TestOperatorRunsMissingTableAsFirstRun(t *testing.T) {
	t.Parallel()

	inner := &recordingOperator{}
	checker := &fakeChecker{exists: false}
	op := wrap(t, inner, connections{"warehouse": checker})
	asset := incrementalAsset(pipeline.MaterializationStrategyMerge)

	out := run(t, op, asset, false)

	assert.Equal(t, []string{"analytics.orders"}, checker.checked)
	require.Len(t, inner.assets, 1)
	assert.NotSame(t, asset, inner.assets[0])
	assert.True(t, inner.assets[0].IsFirstRun())
	assert.True(t, inner.assets[0].FullRefreshEnabled(false))
	assert.Equal(t, asset.Name, inner.assets[0].Name)
	assert.False(t, asset.IsFirstRun(), "the pipeline's asset must stay unchanged")
	assert.Equal(t, []bool{true}, inner.fullRefresh, "the inner operator runs as a full refresh")
	assert.Contains(t, out, "Table 'analytics.orders' does not exist yet, creating it from the query.")
}

func TestOperatorIgnoresRefreshRestrictionOnFirstRun(t *testing.T) {
	t.Parallel()

	inner := &recordingOperator{}
	restricted := true
	asset := incrementalAsset(pipeline.MaterializationStrategyAppend)
	asset.RefreshRestricted = &restricted
	op := wrap(t, inner, connections{"warehouse": &fakeChecker{exists: false}})

	run(t, op, asset, true)

	require.Len(t, inner.assets, 1)
	assert.True(t, inner.assets[0].FullRefreshEnabled(true))
	assert.Equal(t, []bool{true}, inner.fullRefresh)
}

func TestOperatorLoadsExistingTableIncrementally(t *testing.T) {
	t.Parallel()

	inner := &recordingOperator{}
	checker := &fakeChecker{exists: true}
	asset := incrementalAsset(pipeline.MaterializationStrategyDeleteInsert)
	op := wrap(t, inner, connections{"warehouse": checker})

	out := run(t, op, asset, false)

	assert.Len(t, checker.checked, 1)
	assert.Equal(t, []*pipeline.Asset{asset}, inner.assets)
	assert.Equal(t, []bool{false}, inner.fullRefresh)
	assert.Empty(t, out)
}

func TestOperatorKeepsIncrementalLoadWhenCheckFails(t *testing.T) {
	t.Parallel()

	inner := &recordingOperator{}
	asset := incrementalAsset(pipeline.MaterializationStrategyMerge)
	op := wrap(t, inner, connections{"warehouse": &fakeChecker{err: errors.New("permission denied")}})

	out := run(t, op, asset, false)

	assert.Equal(t, []*pipeline.Asset{asset}, inner.assets)
	assert.Contains(t, out, "Could not check whether table 'analytics.orders' exists, loading it incrementally: permission denied")
}

func TestOperatorSkipsCheck(t *testing.T) {
	t.Parallel()

	restricted := true
	tests := []struct {
		name        string
		asset       func() *pipeline.Asset
		fullRefresh bool
	}{
		{
			name: "create+replace builds the table itself",
			asset: func() *pipeline.Asset {
				return incrementalAsset(pipeline.MaterializationStrategyCreateReplace)
			},
		},
		{
			name: "default strategy is create+replace",
			asset: func() *pipeline.Asset {
				return incrementalAsset(pipeline.MaterializationStrategyNone)
			},
		},
		{
			name: "ddl creates the table itself",
			asset: func() *pipeline.Asset {
				return incrementalAsset(pipeline.MaterializationStrategyDDL)
			},
		},
		{
			name: "data vault creates its targets itself",
			asset: func() *pipeline.Asset {
				return incrementalAsset(pipeline.MaterializationStrategyDataVaultHub)
			},
		},
		{
			name: "views are not loaded incrementally",
			asset: func() *pipeline.Asset {
				a := incrementalAsset(pipeline.MaterializationStrategyNone)
				a.Materialization.Type = pipeline.MaterializationTypeView
				return a
			},
		},
		{
			name: "no materialization",
			asset: func() *pipeline.Asset {
				return &pipeline.Asset{Name: "analytics.orders", Connection: "warehouse"}
			},
		},
		{
			name: "full refresh run rebuilds the table",
			asset: func() *pipeline.Asset {
				return incrementalAsset(pipeline.MaterializationStrategyMerge)
			},
			fullRefresh: true,
		},
		{
			name: "full_refresh parameter rebuilds the table",
			asset: func() *pipeline.Asset {
				a := incrementalAsset(pipeline.MaterializationStrategyMerge)
				a.Parameters = pipeline.ParameterMap{"full_refresh": "true"}
				return a
			},
		},
		{
			name: "restricted asset on a normal run",
			asset: func() *pipeline.Asset {
				a := incrementalAsset(pipeline.MaterializationStrategyCreateReplace)
				a.RefreshRestricted = &restricted
				return a
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inner := &recordingOperator{}
			checker := &fakeChecker{exists: false}
			asset := tt.asset()
			op := wrap(t, inner, connections{"warehouse": checker})

			out := run(t, op, asset, tt.fullRefresh)

			assert.Empty(t, checker.checked)
			assert.Equal(t, []*pipeline.Asset{asset}, inner.assets)
			assert.Empty(t, out)
		})
	}
}

func TestOperatorChecksRestrictedAssetOnFullRefreshRun(t *testing.T) {
	t.Parallel()

	// refresh_restricted keeps --full-refresh from rebuilding the table, so the
	// asset still loads incrementally and still needs its table on the first run.
	inner := &recordingOperator{}
	checker := &fakeChecker{exists: false}
	restricted := true
	asset := incrementalAsset(pipeline.MaterializationStrategyMerge)
	asset.RefreshRestricted = &restricted
	op := wrap(t, inner, connections{"warehouse": checker})

	run(t, op, asset, true)

	assert.Len(t, checker.checked, 1)
	require.Len(t, inner.assets, 1)
	assert.True(t, inner.assets[0].IsFirstRun())
}

func TestOperatorDoesNotCreateTableForAssetWithPreHooks(t *testing.T) {
	t.Parallel()

	inner := &recordingOperator{}
	asset := incrementalAsset(pipeline.MaterializationStrategyMerge)
	asset.Hooks = pipeline.Hooks{Pre: []pipeline.Hook{{Query: "SET search_path TO staging"}}}
	op := wrap(t, inner, connections{"warehouse": &fakeChecker{exists: false}})

	out := run(t, op, asset, false)

	assert.Equal(t, []*pipeline.Asset{asset}, inner.assets)
	assert.Equal(t, []bool{false}, inner.fullRefresh)
	assert.Contains(t, out, "Table 'analytics.orders' does not exist yet, but the asset has pre hooks")
	assert.Contains(t, out, "Run it once with --full-refresh to create the table.")
}

type blockingOperator struct {
	recordingOperator
	reason string
}

func (b *blockingOperator) FirstRunBlocker(_ context.Context, _ *pipeline.Pipeline, _ *pipeline.Asset) string {
	return b.reason
}

func TestOperatorHonoursFirstRunBlocker(t *testing.T) {
	t.Parallel()

	inner := &blockingOperator{reason: "its SQL switches the catalog"}
	asset := incrementalAsset(pipeline.MaterializationStrategyAppend)
	op := wrap(t, inner, connections{"warehouse": &fakeChecker{exists: false}})

	out := run(t, op, asset, false)

	assert.Equal(t, []*pipeline.Asset{asset}, inner.assets)
	assert.Contains(t, out, "Table 'analytics.orders' does not exist yet, but its SQL switches the catalog. Run it once with --full-refresh to create the table.")

	inner = &blockingOperator{}
	op = wrap(t, inner, connections{"warehouse": &fakeChecker{exists: false}})
	run(t, op, asset, false)
	require.Len(t, inner.assets, 1)
	assert.True(t, inner.assets[0].IsFirstRun())
}

func TestOperatorWithoutCheckerLoadsIncrementally(t *testing.T) {
	t.Parallel()

	inner := &recordingOperator{}
	asset := incrementalAsset(pipeline.MaterializationStrategyMerge)
	op := wrap(t, inner, connections{"warehouse": struct{}{}})

	out := run(t, op, asset, false)

	assert.Equal(t, []*pipeline.Asset{asset}, inner.assets)
	assert.Empty(t, out)
}

func TestOperatorReturnsInnerError(t *testing.T) {
	t.Parallel()

	inner := &recordingOperator{err: errors.New("boom")}
	op := wrap(t, inner, connections{"warehouse": &fakeChecker{exists: false}})
	ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{}, Asset: incrementalAsset(pipeline.MaterializationStrategyMerge)}

	require.EqualError(t, op.Run(context.Background(), ti), "boom")
}

func TestOperatorStrategies(t *testing.T) {
	t.Parallel()

	for _, strategy := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyAppend,
		pipeline.MaterializationStrategyMerge,
		pipeline.MaterializationStrategyDeleteInsert,
		pipeline.MaterializationStrategyTimeInterval,
		pipeline.MaterializationStrategyTruncateInsert,
		pipeline.MaterializationStrategySCD2ByColumn,
		pipeline.MaterializationStrategySCD2ByTime,
	} {
		t.Run(string(strategy), func(t *testing.T) {
			t.Parallel()

			inner := &recordingOperator{}
			op := wrap(t, inner, connections{"warehouse": &fakeChecker{exists: false}})

			run(t, op, incrementalAsset(strategy), false)

			require.Len(t, inner.assets, 1)
			assert.True(t, inner.assets[0].IsFirstRun())
		})
	}
}

// tableCreator reports the table missing until a first run has created it, the
// way a warehouse shared by overlapping runs would.
type tableCreator struct {
	mu         sync.Mutex
	created    bool
	firstRuns  int
	increments int
}

func (c *tableCreator) RelationExists(_ context.Context, _ string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.created, nil
}

func (c *tableCreator) Run(_ context.Context, ti scheduler.TaskInstance) error {
	if !ti.GetAsset().IsFirstRun() {
		c.mu.Lock()
		c.increments++
		c.mu.Unlock()
		return nil
	}

	// Creating the table takes a while, so the other runs check while it is
	// still missing.
	time.Sleep(50 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.created {
		return errors.New("the table was replaced by a second first run")
	}
	c.created = true
	c.firstRuns++
	return nil
}

func TestOverlappingRunsCreateTheTableOnce(t *testing.T) {
	t.Parallel()

	creator := &tableCreator{}
	lockDir := t.TempDir()

	// Each run gets its own Operator, like separate processes do; they only
	// share the lock directory.
	var wg sync.WaitGroup
	errs := make(chan error, 5)
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			op := Wrap(creator, connections{"warehouse": creator})
			op.lockDir = lockDir
			ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{}, Asset: incrementalAsset(pipeline.MaterializationStrategyAppend)}
			errs <- op.Run(context.Background(), ti)
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	assert.Equal(t, 1, creator.firstRuns)
	assert.Equal(t, 4, creator.increments)
}

func TestOperatorLoadsIncrementallyWhenCancelledWhileWaitingForTheLock(t *testing.T) {
	t.Parallel()

	inner := &recordingOperator{}
	op := wrap(t, inner, connections{"warehouse": &fakeChecker{exists: false}})
	unlock, err := op.lock(context.Background(), "warehouse", "analytics.orders")
	require.NoError(t, err)
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	ti := &scheduler.AssetInstance{Pipeline: &pipeline.Pipeline{}, Asset: incrementalAsset(pipeline.MaterializationStrategyMerge)}
	require.NoError(t, op.Run(ctx, ti))

	require.Len(t, inner.assets, 1)
	assert.False(t, inner.assets[0].IsFirstRun())
}
