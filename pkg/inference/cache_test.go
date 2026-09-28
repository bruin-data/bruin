package inference

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestResultCacheLayersAndEviction(t *testing.T) {
	dir := t.TempDir()
	cache := newResultCache(dir, 2)
	calls := 0
	get := func(c *resultCache, key string) string {
		t.Helper()
		value, err := c.get(key, true, func() (string, error) {
			calls++
			return "value-" + key, nil
		})
		require.NoError(t, err)
		return value
	}
	require.Equal(t, "value-a", get(cache, "a"))
	require.Equal(t, "value-b", get(cache, "b"))
	// A memory hit must not read disk, and must promote the entry ahead of b.
	require.NoError(t, os.Remove(filepath.Join(dir, "a.json")))
	require.Equal(t, "value-a", get(cache, "a"))
	require.Equal(t, "value-c", get(cache, "c"))
	_, exists := cache.memory("b")
	require.False(t, exists, "least recently used entry should be evicted")
	require.Equal(t, "value-b", get(cache, "b"), "eviction must fall through to disk")
	require.Equal(t, 3, calls)
	require.Equal(t, "value-c", get(newResultCache(dir, 2), "c"), "disk must survive a new run")
	require.Equal(t, 3, calls)
}

func TestResultCacheConcurrentDuplicates(t *testing.T) {
	cache := newResultCache(t.TempDir(), 2)
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var calls atomic.Int64
		load := func() (string, error) {
			calls.Add(1)
			<-release
			return "shared", nil
		}
		for range 8 {
			go func() {
				value, err := cache.get("same", true, load)
				if err != nil || value != "shared" {
					t.Errorf("get returned %q, %v", value, err)
				}
			}()
		}
		synctest.Wait()
		require.Equal(t, int64(1), calls.Load(), "duplicates must join the in-flight request before it is saved")
		close(release)
		synctest.Wait()
		require.Equal(t, int64(1), calls.Load())
	})
}

func TestResultCacheFailureAndDisabled(t *testing.T) {
	dir := t.TempDir()
	cache := newResultCache(dir, 2)
	failure := errors.New("provider failed")
	_, err := cache.get("a", true, func() (string, error) { return "", failure })
	require.ErrorIs(t, err, failure)
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
	_, exists := cache.memory("a")
	require.False(t, exists)
	value, err := cache.get("a", true, func() (string, error) { return "original", nil })
	require.NoError(t, err)
	require.Equal(t, "original", value)
	// Disabling caching must bypass memory and even a corrupt disk entry.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.json"), []byte("corrupt"), 0o600))
	for _, fresh := range []string{"fresh-one", "fresh-two"} {
		value, err = cache.get("a", false, func() (string, error) { return fresh, nil })
		require.NoError(t, err)
		require.Equal(t, fresh, value)
	}
	data, err := os.ReadFile(filepath.Join(dir, "a.json"))
	require.NoError(t, err)
	require.Equal(t, "corrupt", string(data), "disabled caching must not overwrite disk")
	value, exists = cache.memory("a")
	require.True(t, exists)
	require.Equal(t, "original", value, "disabled caching must not update memory")
}

func TestOperatorDeduplicatesRenderedRequests(t *testing.T) {
	op, ti, conn, runner := fixture(t)
	asset := columnAsset(t)
	asset.DefinitionFile = ti.Asset.DefinitionFile
	ti.Asset = asset
	ti.Asset.Parameters["extract_parallelism"] = 4
	conn.result.Rows = [][]any{{1, "duplicate"}, {7, "duplicate"}, {19, "different"}}
	var calls atomic.Int64
	op.structured = func(_ context.Context, c *Client, state, _ string, _ []outputColumn) (map[string]any, error) {
		calls.Add(1)
		if c.Provider == "typesafe" {
			return map[string]any{"category": "billing"}, nil
		}
		return map[string]any{"urgent": true, "summary": state}, nil
	}
	require.NoError(t, op.Run(t.Context(), ti))
	groups := int64(2)
	require.Equal(t, 2*groups, calls.Load())
	require.Len(t, runner.rows, 3)
	for i, id := range []int64{1, 7, 19} {
		require.Equal(t, id, runner.rows[i]["id"])
		require.Equal(t, "billing", runner.rows[i]["category"])
		require.Equal(t, conn.result.Rows[i][1], runner.rows[i]["summary"])
		require.Equal(t, true, runner.rows[i]["urgent"])
	}
	conn.result.Rows[0][0] = 25
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 2*groups, calls.Load(), "new row IDs must reuse disk entries for identical requests")
	ti.Asset.Parameters["cache"] = false
	require.NoError(t, op.Run(t.Context(), ti))
	require.Equal(t, 5*groups, calls.Load(), "cache: false must call each group for every row")
}

func TestOperatorCacheUsesBruinHome(t *testing.T) {
	op, ti, _, _ := fixture(t)
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	op.cacheDir = ""
	op.structured = func(context.Context, *Client, string, string, []outputColumn) (map[string]any, error) {
		return map[string]any{"category": "billing"}, nil
	}
	require.NoError(t, op.Run(t.Context(), ti))
	files, err := filepath.Glob(filepath.Join(home, ".bruin", "inference", "*", "*.json"))
	require.NoError(t, err)
	require.Len(t, files, 2)
}

func TestOperatorCacheDisabledDoesNotTouchFilesystem(t *testing.T) {
	op, ti, _, runner := fixture(t)
	ti.Asset.Parameters["cache"] = false
	op.cacheDir = filepath.Join(t.TempDir(), "absent")
	op.structured = func(context.Context, *Client, string, string, []outputColumn) (map[string]any, error) {
		return map[string]any{"category": "billing"}, nil
	}
	require.NoError(t, op.Run(t.Context(), ti))
	require.NoDirExists(t, op.cacheDir)
	// An unusable cache location must not prevent inference or acquire a lock.
	require.NoError(t, os.WriteFile(op.cacheDir, []byte("untouched"), 0o600))
	require.NoError(t, op.Run(t.Context(), ti))
	data, err := os.ReadFile(op.cacheDir)
	require.NoError(t, err)
	require.Equal(t, "untouched", string(data))
	require.Equal(t, 2, runner.calls)
}

func TestCacheParameter(t *testing.T) {
	asset := testAsset()
	cfg, err := readConfig(asset)
	require.NoError(t, err)
	require.True(t, cfg.cache)
	for _, value := range []any{true, false, "true", "false"} {
		asset.Parameters["cache"] = value
		cfg, err = readConfig(asset)
		require.NoError(t, err)
		require.Equal(t, value == true || value == "true", cfg.cache)
	}
	for _, value := range []any{nil, 0, "yes", []string{"false"}} {
		asset.Parameters["cache"] = value
		require.ErrorContains(t, ValidateAsset(asset), "cache must be true or false")
	}
	delete(asset.Parameters, "cache")
	asset.Parameters["force"] = true
	require.ErrorContains(t, ValidateAsset(asset), "use cache: false")
}
