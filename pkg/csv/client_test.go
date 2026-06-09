package csv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClient_GetIngestrURI(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{Path: "input.csv"})
	require.NoError(t, err)

	got, err := client.GetIngestrURI()
	require.NoError(t, err)
	require.Equal(t, "csv://input.csv", got)
}
