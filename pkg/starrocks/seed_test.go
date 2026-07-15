package starrocks

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQuoteValue(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "''", quoteValue(""))
	assert.Equal(t, "'Ada'", quoteValue("Ada"))
	assert.Equal(t, "'O''Hara'", quoteValue("O'Hara"))
	assert.Equal(t, "'C:\\\\tmp\\\\file'", quoteValue(`C:\tmp\file`))
}
