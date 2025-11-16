package config

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGoogleCloudPlatformConnectionMarshalJSONAlwaysIncludesUseADC(t *testing.T) {
	t.Parallel()

	conn := GoogleCloudPlatformConnection{
		Name:      "conn1",
		ProjectID: "my-project",
		Location:  "us",
		// UseApplicationDefaultCredentials intentionally left as zero value to ensure it marshals as false.
	}

	raw, err := conn.MarshalJSON()
	require.NoError(t, err)

	var payload map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &payload))

	adcValue, exists := payload["use_application_default_credentials"]
	require.True(t, exists, "use_application_default_credentials should always be present in marshaled JSON")
	assert.False(t, adcValue.(bool))
}
