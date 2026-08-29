package ripestat

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_GetIngestrURI(t *testing.T) {
	t.Parallel()

	c := Config{}
	assert.Equal(t, "ripestat://", c.GetIngestrURI())
}

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{})
	require.NoError(t, err)
	require.NotNil(t, client)

	uri, err := client.GetIngestrURI()
	require.NoError(t, err)
	assert.Equal(t, "ripestat://", uri)
}
