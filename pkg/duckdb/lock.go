package duck

import (
	"context"
	"io"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Mutex is the mutex with synchronized map, it allows reducing unnecessary locks among different keys.
// This implementation comes from the mapmutex package, I simply copied it here instead of adding it as a dependency.
// See the code here: https://github.com/EagleChen/mapmutex/blob/master/mutex.go
type Mutex struct {
	locks     map[interface{}]interface{}
	m         *sync.Mutex
	maxRetry  int
	maxDelay  float64 // in nanosend
	baseDelay float64 // in nanosecond
	factor    float64
	jitter    float64
}

// TryLock tries to acquire the lock.
func (m *Mutex) TryLock(key interface{}) bool {
	for i := range m.maxRetry {
		m.m.Lock()
		if _, ok := m.locks[key]; ok { // if locked
			m.m.Unlock()
			time.Sleep(m.backoff(i))
		} else { // if unlock, lockit
			m.locks[key] = struct{}{}
			m.m.Unlock()
			return true
		}
	}

	return false
}

// Unlock unlocks for the key
// please call Unlock only after having acquired the lock.
func (m *Mutex) Unlock(key interface{}) {
	m.m.Lock()
	delete(m.locks, key)
	m.m.Unlock()
}

func (m *Mutex) backoff(retries int) time.Duration {
	if retries == 0 {
		return time.Duration(m.baseDelay) * time.Nanosecond
	}
	backoff, maxDelay := m.baseDelay, m.maxDelay
	for backoff < maxDelay && retries > 0 {
		backoff *= m.factor
		retries--
	}
	if backoff > maxDelay {
		backoff = maxDelay
	}
	backoff *= 1 + m.jitter*(rand.Float64()*2-1) //nolint:gosec
	if backoff < 0 {
		return 0
	}
	return time.Duration(backoff) * time.Nanosecond
}

// NewMapMutex returns a mapmutex with default configs.
func NewMapMutex() *Mutex {
	return &Mutex{
		locks:     make(map[interface{}]interface{}),
		m:         &sync.Mutex{},
		maxRetry:  200,
		maxDelay:  100000000, // 0.1 second
		baseDelay: 10,        // 10 nanosecond
		factor:    1.1,
		jitter:    0.2,
	}
}

// NewCustomizedMapMutex returns a customized mapmutex.
func NewCustomizedMapMutex(mRetry int, mDelay, bDelay, factor, jitter float64) *Mutex {
	return &Mutex{
		locks:     make(map[interface{}]interface{}),
		m:         &sync.Mutex{},
		maxRetry:  mRetry,
		maxDelay:  mDelay,
		baseDelay: bDelay,
		factor:    factor,
		jitter:    jitter,
	}
}

// SQL sessions share a database instance. External processes need exclusive
// access and must release that instance's file lock before opening the file.
type databaseLock struct {
	sync.RWMutex
	database io.Closer
	schema   sync.Mutex
	scopes   atomic.Int64
}

// Cache access requires the per-file lock followed by connectionReuseGate.RLock,
// or just connectionReuseGate.Lock during a process-wide cache handoff.
func (d *databaseLock) closeDatabase() {
	if d.database != nil {
		_ = d.database.Close()
		d.database = nil
	}
}

var databaseLocks sync.Map

// Cached sessions hold the read side for their full lifetime. Always acquire
// per-file locks before this gate; never wait for a file while holding the gate.
// The write side drains cached sessions and protects all cache pointers without
// waiting on file locks held by ingestion or uncached SQL execution.
var connectionReuseGate sync.RWMutex

var activeScripts atomic.Int64

// SuspendConnectionReuse releases cached file locks for arbitrary Python/R code.
// SQL continues with ephemeral connections until every overlapping script exits.
// Scripts must still declare dependencies when sharing files with other work.
func SuspendConnectionReuse(ctx context.Context) func() {
	if connectionReuse(ctx) == nil {
		return func() {}
	}
	connectionReuseGate.Lock()
	activeScripts.Add(1)
	databaseLocks.Range(func(_, value any) bool {
		value.(*databaseLock).closeDatabase()
		return true
	})
	connectionReuseGate.Unlock()
	return func() { activeScripts.Add(-1) }
}

func databaseLockKey(path string) string {
	// GetIngestrURI adds duckdb:/// to the configured path, including when
	// that path is absolute. Use the same key as native SQL operations.
	path = strings.TrimPrefix(path, "duckdb:///")
	if path != "" && path != ":memory:" && !strings.HasPrefix(path, "md:") {
		if absolute, err := filepath.Abs(path); err == nil {
			path = absolute
		}
	}
	return path
}

func databaseLockFor(path string) *databaseLock {
	lock, _ := databaseLocks.LoadOrStore(databaseLockKey(path), &databaseLock{})
	return lock.(*databaseLock)
}

func LockDatabase(path string) {
	lock := databaseLockFor(path)
	lock.Lock()
	connectionReuseGate.RLock()
	lock.closeDatabase()
	connectionReuseGate.RUnlock()
}

func UnlockDatabase(path string) {
	databaseLockFor(path).Unlock()
}

// LockDatabases acquires source/destination files once, in a consistent order,
// including when different URIs refer to the same file.
func LockDatabases(paths ...string) func() {
	keys := make([]string, len(paths))
	for i, path := range paths {
		keys[i] = databaseLockKey(path)
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	for _, key := range keys {
		LockDatabase(key)
	}
	return func() {
		for _, key := range keys {
			UnlockDatabase(key)
		}
	}
}

type connectionReuseKey struct{}

type connectionReuseScope struct {
	sync.Mutex
	databases map[*databaseLock]struct{}
}

func (s *connectionReuseScope) register(lock *databaseLock) bool {
	s.Lock()
	defer s.Unlock()
	if s.databases == nil {
		return false
	}
	if _, ok := s.databases[lock]; !ok {
		lock.scopes.Add(1)
		s.databases[lock] = struct{}{}
	}
	return true
}

func (d *databaseLock) releaseScope() {
	if d.scopes.Add(-1) == 0 {
		d.closeDatabase()
	}
}

// WithConnectionReuse keeps DuckDB instances (including lakehouse attachments)
// alive for a pipeline run. Cleanup cancels the scope and closes idle databases;
// active sessions are closed asynchronously once their workers release them.
// This preserves bounded CLI shutdown when a worker ignores cancellation.
// Outside this scope, connections remain ephemeral.
func WithConnectionReuse(ctx context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(ctx)
	scope := &connectionReuseScope{databases: make(map[*databaseLock]struct{})}
	return context.WithValue(ctx, connectionReuseKey{}, scope), func() {
		scope.Lock()
		cancel()
		databases := scope.databases
		scope.databases = nil
		scope.Unlock()
		for lock := range databases {
			if lock.TryLock() {
				if connectionReuseGate.TryRLock() {
					lock.releaseScope()
					connectionReuseGate.RUnlock()
					lock.Unlock()
					continue
				}
				lock.Unlock()
			}
			go func() {
				lock.Lock()
				defer lock.Unlock()
				connectionReuseGate.RLock()
				defer connectionReuseGate.RUnlock()
				lock.releaseScope()
			}()
		}
	}
}

func connectionReuse(ctx context.Context) *connectionReuseScope {
	scope, _ := ctx.Value(connectionReuseKey{}).(*connectionReuseScope)
	return scope
}
