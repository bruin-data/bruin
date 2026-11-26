package secrets

import (
	"fmt"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingLogger struct {
	logger.Logger
	debugf, warnf int
}

func (l *recordingLogger) Debugf(string, ...any) { l.debugf++ }
func (l *recordingLogger) Warnf(string, ...any)  { l.warnf++ }

func TestLogLookupFailure(t *testing.T) {
	t.Parallel()

	t.Run("missing secret logs at debug", func(t *testing.T) {
		t.Parallel()
		l := &recordingLogger{}
		logLookupFailure(l, newSecretNotFoundError("secret '%s' not found", "x"))
		assert.Equal(t, 1, l.debugf)
		assert.Equal(t, 0, l.warnf)
	})

	t.Run("other failures log at warn", func(t *testing.T) {
		t.Parallel()
		l := &recordingLogger{}
		logLookupFailure(l, errors.New("access denied"))
		assert.Equal(t, 0, l.debugf)
		assert.Equal(t, 1, l.warnf)
	})

	t.Run("wrapped not-found is still recognized", func(t *testing.T) {
		t.Parallel()
		wrapped := errors.Wrap(newSecretNotFoundError("missing"), "fetch manager")
		assert.True(t, isSecretNotFound(wrapped))
	})
}

func TestSecretNotFoundMatchesErrConnectionNotFound(t *testing.T) {
	t.Parallel()

	err := fmt.Errorf("failed to resolve connection 'pg': %w", newSecretNotFoundError("secret '%s' not found in Vault", "pg"))
	require.ErrorIs(t, err, config.ErrConnectionNotFound)
	require.NotErrorIs(t, errors.New("secret 'pg' not found in Vault"), config.ErrConnectionNotFound)
}
