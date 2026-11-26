package executor

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWarningLines(t *testing.T) {
	output := ">> loading rows\n>> WARNING: source returned 0 rows\n>> swarm of 10 rows loaded\n>> UserWarning: column dropped\nWarning: full refresh is restricted\n"

	assert.Equal(t, []string{
		"WARNING: source returned 0 rows",
		"UserWarning: column dropped",
		"Warning: full refresh is restricted",
	}, WarningLines([]byte(output)))
	assert.Empty(t, WarningLines([]byte(">> all good\n")))
}
