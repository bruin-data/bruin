package duck

import (
	"testing"
)

func TestLockSuccess(t *testing.T) {
	t.Parallel()

	m := NewMapMutex()

	if !m.TryLock("123") {
		t.Error("fail to get lock")
	}
	m.Unlock("123")
}

func TestLockFail(t *testing.T) {
	t.Parallel()

	// fail fast
	m := NewCustomizedMapMutex(1, 1, 1, 2, 0.1)

	c := make(chan bool)
	finish := make(chan bool)

	num := 5
	success := make([]int, num)

	for i := range num {
		go func(i int) {
			if m.TryLock("123") {
				<-c // block here
				success[i] = 1
				m.Unlock("123")
			}
			finish <- true
		}(i)
	}

	// most goroutines fail to get the lock
	for range num - 1 {
		<-finish
	}

	sum := 0
	for _, s := range success {
		sum += s
	}

	if sum != 0 {
		t.Error("some other goroutine got the lock")
	}

	// finish the success one
	c <- true
	// wait
	<-finish
	for _, s := range success {
		sum += s
	}
	if sum != 1 {
		t.Error("no goroutine got the lock")
	}
}

func TestLockIndivisually(t *testing.T) {
	t.Parallel()

	m := NewMapMutex()

	if !m.TryLock(123) || !m.TryLock(456) {
		t.Error("different locks affect each other")
	}
}
