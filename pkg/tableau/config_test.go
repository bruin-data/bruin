package tableau

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_GetName(t *testing.T) {
	config := Config{
		Name: "test-tableau",
	}
	assert.Equal(t, "test-tableau", config.GetName())
}
