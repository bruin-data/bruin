package duck

import "sync"

// databaseLocks maps database paths to their corresponding locks to prevent concurrent access
var databaseLocks = struct {
	sync.RWMutex
	locks map[string]*sync.Mutex
}{
	locks: make(map[string]*sync.Mutex),
}

// this allows us to share locks between different components to the same database
func LockDatabase(path string) {
	databaseLocks.RLock()
	lock, ok := databaseLocks.locks[path]
	databaseLocks.RUnlock()
	if !ok {
		databaseLocks.Lock()
		lock = &sync.Mutex{}
		lock.Lock()
		databaseLocks.locks[path] = lock
		databaseLocks.Unlock()
		return
	}

	lock.Lock()
}

func UnlockDatabase(path string) {
	databaseLocks.Lock()
	delete(databaseLocks.locks, path)
	databaseLocks.Unlock()
}
