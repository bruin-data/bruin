package secrets

import (
	"fmt"

	"github.com/bruin-data/bruin/pkg/logger"
	"github.com/pkg/errors"
)

type secretNotFoundError struct{ msg string }

func (e *secretNotFoundError) Error() string { return e.msg }

func newSecretNotFoundError(format string, args ...any) *secretNotFoundError {
	return &secretNotFoundError{msg: fmt.Sprintf(format, args...)}
}

func isSecretNotFound(err error) bool {
	var e *secretNotFoundError
	return errors.As(err, &e)
}

// logLookupFailure keeps a missing secret quiet (debug) so the caller's clean
// "connection not found" message surfaces, while other failures are reported at
// warn level so their cause stays visible without a stack trace.
func logLookupFailure(l logger.Logger, err error) {
	if isSecretNotFound(err) {
		l.Debugf("%v", err)
		return
	}
	l.Warnf("%v", err)
}
