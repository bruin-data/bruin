package pipeline_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDurationSeconds_JSON(t *testing.T) {
	t.Parallel()

	var asset struct {
		Timeout pipeline.DurationSeconds `json:"timeout,omitempty"`
	}
	err := json.Unmarshal([]byte(`{"timeout":5400}`), &asset)
	require.NoError(t, err)
	require.Equal(t, 90*time.Minute, asset.Timeout.Duration())

	out, err := json.Marshal(asset)
	require.NoError(t, err)
	require.JSONEq(t, `{"timeout":5400}`, string(out))
}

func TestDurationSeconds_YAML(t *testing.T) {
	t.Parallel()

	var asset struct {
		Timeout pipeline.DurationSeconds `yaml:"timeout"`
	}
	err := yaml.Unmarshal([]byte("timeout: 1h30m\n"), &asset)
	require.NoError(t, err)
	require.Equal(t, 90*time.Minute, asset.Timeout.Duration())

	out, err := yaml.Marshal(&asset)
	require.NoError(t, err)
	require.Contains(t, string(out), "timeout: 1h30m")
}

func TestAsset_TimeoutJSONRoundTrip(t *testing.T) {
	t.Parallel()

	asset := pipeline.Asset{
		Name:    "dataset.asset",
		Timeout: pipeline.DurationSeconds(90 * time.Minute),
	}

	out, err := json.Marshal(asset)
	require.NoError(t, err)
	require.Contains(t, string(out), `"timeout":5400`)

	var rebuilt pipeline.Asset
	err = json.Unmarshal(out, &rebuilt)
	require.NoError(t, err)
	require.Equal(t, 90*time.Minute, rebuilt.Timeout.Duration())
}
