package clickup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	c := Config{APIToken: "pk_1+2&3"}
	assert.Equal(t, "clickup://?api_key=pk_1%2B2%263", c.GetIngestrURI())
}
