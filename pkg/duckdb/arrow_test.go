//go:build !bruin_no_duckdb

package duck

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/require"
)

func TestSelectArrowUsesSharedSession(t *testing.T) {
	t.Parallel()
	ctx, cleanup := WithConnectionReuse(t.Context())
	cfg := Config{Path: filepath.Join(t.TempDir(), "arrow.db")}
	t.Cleanup(cleanup) // Close the file before TempDir cleanup, including on Windows.
	first, err := NewClient(cfg)
	require.NoError(t, err)
	second, err := NewClient(cfg)
	require.NoError(t, err)
	session, release, err := first.connection.(*EphemeralConnection).openADBC(ctx, "")
	require.NoError(t, err)
	var once sync.Once
	defer once.Do(release)
	require.NoError(t, execADBCStatement(ctx, session, "CREATE TABLE numbers AS SELECT * FROM range(3) t(n)"))

	type result struct {
		rows int64
		sum  int64
		err  error
	}
	done := make(chan result, 1)
	go func() {
		record, err := second.SelectArrow(ctx, &query.Query{Query: "SELECT n FROM numbers"}, 10)
		if err != nil {
			done <- result{err: err}
			return
		}
		defer record.Release()
		var sum int64
		for _, n := range record.Column(0).(*array.Int64).Int64Values() {
			sum += n
		}
		done <- result{rows: record.NumRows(), sum: sum}
	}()
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, int64(3), r.rows)
		require.Equal(t, int64(3), r.sum)
	case <-time.After(10 * time.Second):
		once.Do(release)
		<-done
		t.Fatal("SelectArrow waited for another SQL session to close")
	}
}
