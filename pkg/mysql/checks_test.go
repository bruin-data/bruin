package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAcceptedValuesCheck(t *testing.T) {
	t.Parallel()

	// Basic test to ensure the check struct can be created
	check := &AcceptedValuesCheck{conn: nil}
	assert.NotNil(t, check)
}

func TestPatternCheck(t *testing.T) {
	t.Parallel()

	// Basic test to ensure the check struct can be created
	check := &PatternCheck{conn: nil}
	assert.NotNil(t, check)
}