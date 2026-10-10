package inference

import (
	"container/list"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sync/singleflight"
)

// resultCache is local to one asset run. Evicted results remain available on disk.
type resultCache struct {
	directory string
	capacity  int
	mu        sync.Mutex
	entries   map[string]*list.Element
	recent    list.List
	inflight  singleflight.Group
}

type cachedResult struct{ key, value string }

func newResultCache(directory string, capacity int) *resultCache {
	return &resultCache{directory: directory, capacity: capacity, entries: make(map[string]*list.Element)}
}

func (c *resultCache) memory(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.entries[key]; ok {
		c.recent.MoveToFront(entry)
		return entry.Value.(cachedResult).value, true
	}
	return "", false
}

// get calls load only on a miss. load must validate the result before returning it.
// Disabling caching bypasses memory, disk reads/writes, and in-flight deduplication.
func (c *resultCache) get(key string, enabled bool, load func() (string, error)) (string, error) {
	if !enabled {
		return load()
	}
	path := filepath.Join(c.directory, key+".json")
	if value, ok := c.memory(key); ok {
		return value, nil
	}
	value, err, _ := c.inflight.Do(key, func() (any, error) {
		// Another request may have filled memory before this flight started.
		if value, ok := c.memory(key); ok {
			return value, nil
		}
		var result string
		data, err := os.ReadFile(path)
		switch {
		case err == nil:
			if json.Unmarshal(data, &result) != nil {
				return "", fmt.Errorf("invalid inference cache entry; remove %s and retry", path)
			}
		case os.IsNotExist(err):
			result, err = load()
			if err == nil {
				err = saveResult(path, result)
			}
		}
		if err != nil {
			return "", err
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.entries[key] = c.recent.PushFront(cachedResult{key, result})
		if c.recent.Len() > c.capacity {
			oldest := c.recent.Back()
			delete(c.entries, oldest.Value.(cachedResult).key)
			c.recent.Remove(oldest)
		}
		return result, nil
	})
	if err != nil {
		return "", err
	}
	return value.(string), nil
}
