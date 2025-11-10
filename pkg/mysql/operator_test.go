package mysql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewColumnCheckOperator(t *testing.T) {
	t.Parallel()

	// This is a basic test to ensure the function doesn't panic
	// In a real implementation, you would inject a mock connection getter
	operator := NewColumnCheckOperator(nil)
	assert.NotNil(t, operator)
}

func TestNewMetadataPushOperator(t *testing.T) {
	t.Parallel()

	// This is a basic test to ensure the function doesn't panic
	// In a real implementation, you would inject a mock connection getter
	operator := NewMetadataPushOperator(nil)
	assert.NotNil(t, operator)
}
