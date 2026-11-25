// Package autocreate creates an incremental asset's table on its first run.
//
// Incremental strategies such as merge or append load into a table that has to
// exist already. Operator checks for the table before the asset runs, and when
// the table is missing it runs the asset as a full refresh, which creates the
// table from the query. Later runs find the table and load into it as usual.
package autocreate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/gofrs/flock"
)

// lockRetryDelay is how often a run waiting for another run's first run of the
// same table tries to take the lock.
const lockRetryDelay = 200 * time.Millisecond

// RelationChecker is implemented by connections whose incremental assets get
// their table created on the first run.
type RelationChecker interface {
	// RelationExists reports whether a table, view or any other relation that
	// the asset's materialization would write to exists under tableName, which
	// is the asset name. It resolves the name the same way the materialization
	// does, including the connection's default database or schema.
	//
	// Reporting false makes Bruin build the table from scratch, so false must
	// mean the platform confirmed that nothing by that name exists, including
	// when its schema or database is missing. Anything less certain has to
	// report true or an error, both of which keep the incremental load.
	RelationExists(ctx context.Context, tableName string) (bool, error)
}

// FirstRunBlocker is implemented by operators whose assets can move their
// table in their own SQL, for example with a statement that switches the
// session's catalog. The check looks the table up where the connection points,
// so for those assets a missing answer can be wrong.
type FirstRunBlocker interface {
	// FirstRunBlocker returns why the asset must not be created on its first
	// run, or "" when it can be. It gets the FirstRun asset and a context that
	// reports a full refresh, so templates render the way the first run would.
	FirstRunBlocker(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) string
}

// Operator runs an incremental asset as a full refresh when its table does not
// exist yet, and runs every other task unchanged.
type Operator struct {
	inner       executor.Operator
	connections config.ConnectionGetter
	// lockDir holds the files that keep two runs on this machine from creating
	// the same table at once.
	lockDir string
}

// Wrap returns an Operator that runs inner, checking for missing tables through
// connections, which has to be the getter inner reads the asset's connection
// from. On a first run inner gets a task instance that only carries the
// pipeline and the FirstRun asset, so it must not assert the instance's type.
func Wrap(inner executor.Operator, connections config.ConnectionGetter) *Operator {
	return &Operator{
		inner:       inner,
		connections: connections,
		lockDir:     filepath.Join(os.TempDir(), "bruin-autocreate"),
	}
}

func (o *Operator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	asset := ti.GetAsset()
	// The executor stores whether this asset is fully refreshed in this run;
	// assets that rebuild their table anyway skip the check.
	fullRefresh, _ := ctx.Value(pipeline.RunConfigFullRefresh).(bool)
	if !asset.Materialization.LoadsExistingTable() || asset.FullRefreshEnabled(fullRefresh) {
		return o.inner.Run(ctx, ti)
	}

	connName, err := ti.GetPipeline().GetConnectionNameForAsset(asset)
	if err != nil {
		return o.inner.Run(ctx, ti)
	}
	checker, ok := o.connections.GetConnection(connName).(RelationChecker)
	if !ok {
		// Connections without a RelationChecker keep the incremental load.
		return o.inner.Run(ctx, ti)
	}

	// Runs of the same asset can overlap, for example the partitions of a
	// backfill. Each would see the table missing and replace the one another
	// run just created, so the check and the first run hold a per-table lock:
	// one run creates the table, and the others find it once they get the lock.
	unlock, err := o.lock(ctx, connName, asset.Name)
	if err != nil {
		printf(ctx, "Could not lock table '%s' to check whether it exists, loading it incrementally: %v\n", asset.Name, err)
		return o.inner.Run(ctx, ti)
	}
	if !o.tableMissing(ctx, checker, asset) {
		unlock()
		return o.inner.Run(ctx, ti)
	}
	firstRunCtx := context.WithValue(ctx, pipeline.RunConfigFullRefresh, true)
	firstRun := asset.FirstRun()
	if reason := o.firstRunBlocker(firstRunCtx, ti.GetPipeline(), firstRun); reason != "" {
		unlock()
		printf(ctx, "Table '%s' does not exist yet, but %s. Run it once with --full-refresh to create the table.\n", asset.Name, reason)
		return o.inner.Run(ctx, ti)
	}
	defer unlock()

	printf(ctx, "Table '%s' does not exist yet, creating it from the query.\n", asset.Name)
	return o.inner.Run(firstRunCtx, &firstRunInstance{TaskInstance: ti, asset: firstRun})
}

// firstRunBlocker returns why the asset's table must not be created even though
// the check did not find it.
func (o *Operator) firstRunBlocker(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) string {
	if len(asset.Hooks.Pre) > 0 {
		// A pre hook runs after the check and can move the asset to another
		// database or schema, for example with USE or SET search_path, where a
		// table by that name may exist. Creating it could replace that table.
		return "the asset has pre hooks, which can change where it is created"
	}
	if blocker, ok := o.inner.(FirstRunBlocker); ok {
		return blocker.FirstRunBlocker(ctx, p, asset)
	}
	return ""
}

// tableMissing reports whether the platform confirmed that the asset's table
// does not exist. The check only exists to turn a certain failure into a first
// run, so a failed check keeps the incremental load instead of failing the asset.
func (o *Operator) tableMissing(ctx context.Context, checker RelationChecker, asset *pipeline.Asset) bool {
	exists, err := checker.RelationExists(ctx, asset.Name)
	if err != nil {
		printf(ctx, "Could not check whether table '%s' exists, loading it incrementally: %v\n", asset.Name, err)
		return false
	}

	return !exists
}

// lock takes the machine-wide lock for a table's check and first run, and
// returns the function that releases it. It waits while another run holds it.
func (o *Operator) lock(ctx context.Context, connName, tableName string) (func(), error) {
	if err := os.MkdirAll(o.lockDir, 0o700); err != nil {
		return nil, err
	}

	key := sha256.Sum256([]byte(connName + "\x00" + tableName))
	fileLock := flock.New(filepath.Join(o.lockDir, hex.EncodeToString(key[:16])+".lock"))
	locked, err := fileLock.TryLockContext(ctx, lockRetryDelay)
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, ctx.Err()
	}

	return func() { _ = fileLock.Unlock() }, nil
}

// firstRunInstance hands the inner operator the FirstRun copy of the asset.
type firstRunInstance struct {
	scheduler.TaskInstance
	asset *pipeline.Asset
}

func (t *firstRunInstance) GetAsset() *pipeline.Asset {
	return t.asset
}

func printf(ctx context.Context, format string, args ...any) {
	if printer, ok := ctx.Value(executor.KeyPrinter).(io.Writer); ok {
		fmt.Fprintf(printer, format, args...)
	}
}
