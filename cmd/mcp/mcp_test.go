package mcp

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToolsListMarksToolsReadOnly(t *testing.T) {
	t.Parallel()

	resp := processRequest(JSONRPCRequest{JSONRPC: "2.0", ID: 1, Method: "tools/list"}, false)
	require.Nil(t, resp.Error)

	raw, err := json.Marshal(resp.Result)
	require.NoError(t, err)

	var result struct {
		Tools []struct {
			Name        string `json:"name"`
			Annotations struct {
				ReadOnlyHint    *bool `json:"readOnlyHint"`
				DestructiveHint *bool `json:"destructiveHint"`
			} `json:"annotations"`
		} `json:"tools"`
	}
	require.NoError(t, json.Unmarshal(raw, &result))
	require.NotEmpty(t, result.Tools)

	for _, tool := range result.Tools {
		require.NotNil(t, tool.Annotations.ReadOnlyHint, tool.Name)
		assert.True(t, *tool.Annotations.ReadOnlyHint, tool.Name)
		require.NotNil(t, tool.Annotations.DestructiveHint, tool.Name)
		assert.False(t, *tool.Annotations.DestructiveHint, tool.Name)
	}
}
