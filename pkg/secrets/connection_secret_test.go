package secrets

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// messageLogger keeps debug and warn messages so tests can assert on them.
type messageLogger struct {
	mockLogger
	mu     sync.Mutex
	debugs []string
	warns  []string
}

func (l *messageLogger) Debugf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.debugs = append(l.debugs, fmt.Sprintf(format, args...))
}

func (l *messageLogger) Warnf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warns = append(l.warns, fmt.Sprintf(format, args...))
}

// fakeSecretStore is a connectionSecretStore over an in-memory map of connection name to payload.
type fakeSecretStore struct {
	payloads map[string]string
	errs     map[string]error
	listErr  error
	delay    time.Duration
	reads    atomic.Int32
}

func (s *fakeSecretStore) readConnections(name string) (*config.Connections, error) {
	s.reads.Add(1)
	time.Sleep(s.delay)
	if err, ok := s.errs[name]; ok {
		return nil, err
	}
	payload, ok := s.payloads[name]
	if !ok {
		return nil, newSecretNotFoundError("secret '%s' not found", name)
	}

	return readConnectionSecret(name, &payload)
}

func (s *fakeSecretStore) listConnectionNames() ([]string, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	names := make([]string, 0, len(s.payloads)+len(s.errs))
	for name := range s.payloads {
		names = append(names, name)
	}
	for name := range s.errs {
		names = append(names, name)
	}

	return names, nil
}

func TestParseConnectionSecret(t *testing.T) {
	t.Parallel()

	connections, err := parseConnectionSecret("my-conn", []byte(testGenericSecretJSON))
	require.NoError(t, err)
	require.Len(t, connections.Generic, 1)
	require.Equal(t, "my-conn", connections.Generic[0].Name)
	require.Equal(t, "somevalue", connections.Generic[0].Value)

	for _, payload := range []string{
		`not json`,
		`{"details": {"value": "x"}}`,
		`{"type": "generic"}`,
		`{"type": " ", "details": {"value": "x"}}`,
		`{"type": "generic", "details": "x"}`,
	} {
		_, err := parseConnectionSecret("my-conn", []byte(payload))
		require.Error(t, err, payload)
		require.Contains(t, err.Error(), "my-conn", payload)
	}
}

func TestCachedSecretBackend_OneReadPerName(t *testing.T) {
	t.Parallel()

	store := &fakeSecretStore{payloads: map[string]string{"conn": testGenericSecretJSON}}
	backend := newCachedSecretBackend(store, &mockLogger{})

	require.Equal(t, "generic", backend.GetConnectionType("conn"))
	require.NotNil(t, backend.GetConnection("conn"))
	require.NotNil(t, backend.GetConnectionDetails("conn"))
	_, err := backend.ResolveConnection("conn")
	require.NoError(t, err)
	require.Equal(t, "generic", backend.GetConnectionType("conn"))

	require.EqualValues(t, 1, store.reads.Load())
}

func TestCachedSecretBackend_ConcurrentLookupsShareOneRead(t *testing.T) {
	t.Parallel()

	store := &fakeSecretStore{payloads: map[string]string{"conn": testGenericSecretJSON}, delay: 20 * time.Millisecond}
	backend := newCachedSecretBackend(store, &mockLogger{})

	const goroutineCount = 10
	results := make([]any, goroutineCount)
	var wg sync.WaitGroup
	for i := range goroutineCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch i % 3 {
			case 0:
				results[i] = backend.GetConnection("conn")
			case 1:
				results[i] = backend.GetConnectionDetails("conn")
			default:
				results[i] = backend.GetConnectionType("conn")
			}
		}()
	}
	wg.Wait()

	for i, result := range results {
		require.NotEmpty(t, result, "lookup %d", i)
	}
	require.EqualValues(t, 1, store.reads.Load())
}

func TestCachedSecretBackend_FailuresAreNotCached(t *testing.T) {
	t.Parallel()

	store := &fakeSecretStore{
		payloads: map[string]string{},
		errs:     map[string]error{"conn": errors.New("throttled")},
	}
	backend := newCachedSecretBackend(store, &mockLogger{})

	require.Nil(t, backend.GetConnection("conn"))
	require.Empty(t, backend.GetConnectionType("conn"))

	delete(store.errs, "conn")
	store.payloads["conn"] = testGenericSecretJSON
	require.NotNil(t, backend.GetConnection("conn"))
	require.EqualValues(t, 3, store.reads.Load())
}

func TestCachedSecretBackend_NotFoundVersusError(t *testing.T) {
	t.Parallel()

	store := &fakeSecretStore{
		payloads: map[string]string{},
		errs:     map[string]error{"broken": errors.New("access denied")},
	}
	log := &messageLogger{}
	backend := newCachedSecretBackend(store, log)

	_, err := backend.ResolveConnection("missing")
	require.True(t, isSecretNotFound(err))
	_, err = backend.ResolveConnection("broken")
	require.Error(t, err)
	require.False(t, isSecretNotFound(err))

	// A missing secret stays quiet so the caller's "connection not found" message shows; other failures warn.
	require.Nil(t, backend.GetConnection("missing"))
	require.Empty(t, log.warns)
	require.Nil(t, backend.GetConnection("broken"))
	require.Len(t, log.warns, 1)
	require.Contains(t, log.warns[0], "access denied")
}

func TestCachedSecretBackend_RejectsEmptyName(t *testing.T) {
	t.Parallel()

	store := &fakeSecretStore{payloads: map[string]string{}}
	backend := newCachedSecretBackend(store, &mockLogger{})

	_, err := backend.ResolveConnection(" ")
	require.ErrorContains(t, err, "empty connection name")
	require.EqualValues(t, 0, store.reads.Load())
}

func TestCachedSecretBackend_GetConnectionsConfig(t *testing.T) {
	t.Parallel()

	t.Run("merges connections and skips secrets that aren't connections", func(t *testing.T) {
		t.Parallel()

		store := &fakeSecretStore{payloads: map[string]string{
			"a-generic":  testGenericSecretJSON,
			"b-postgres": `{"type": "postgres", "details": {"username": "u", "password": "p", "host": "h", "port": 5432, "database": "d"}}`,
			"unrelated":  `plain text`,
		}}
		log := &messageLogger{}
		backend := newCachedSecretBackend(store, log)

		connections, err := backend.GetConnectionsConfig()
		require.NoError(t, err)
		require.Len(t, connections.Generic, 1)
		require.Equal(t, "a-generic", connections.Generic[0].Name)
		require.Len(t, connections.Postgres, 1)
		require.Equal(t, "b-postgres", connections.Postgres[0].Name)
		require.Empty(t, log.warns)
		require.Len(t, log.debugs, 1)
		require.Contains(t, log.debugs[0], "unrelated")
	})

	t.Run("skips secrets deleted after listing", func(t *testing.T) {
		t.Parallel()

		store := &fakeSecretStore{
			payloads: map[string]string{"a-generic": testGenericSecretJSON},
			errs:     map[string]error{"gone": newSecretNotFoundError("secret 'gone' not found")},
		}
		connections, err := newCachedSecretBackend(store, &mockLogger{}).GetConnectionsConfig()
		require.NoError(t, err)
		require.Len(t, connections.Generic, 1)
	})

	t.Run("returns read errors", func(t *testing.T) {
		t.Parallel()

		store := &fakeSecretStore{
			payloads: map[string]string{"a-generic": testGenericSecretJSON},
			errs:     map[string]error{"locked": errors.New("access denied")},
		}
		_, err := newCachedSecretBackend(store, &mockLogger{}).GetConnectionsConfig()
		require.ErrorContains(t, err, "access denied")
	})

	t.Run("returns list errors", func(t *testing.T) {
		t.Parallel()

		store := &fakeSecretStore{listErr: errors.New("list denied")}
		_, err := newCachedSecretBackend(store, &mockLogger{}).GetConnectionsConfig()
		require.ErrorContains(t, err, "list denied")
	})
}
