package quicksight

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestBasicOperator_RunTask_NoRefresh(t *testing.T) {
	t.Parallel()

	op := NewBasicOperator(nil)

	assert.NotNil(t, op)
}

func TestBasicOperator_AssetTypes(t *testing.T) {
	t.Parallel()

	assert.Equal(t, pipeline.AssetTypeQuicksightDataset, pipeline.AssetType("quicksight.dataset"))
	assert.Equal(t, pipeline.AssetTypeQuicksightDashboard, pipeline.AssetType("quicksight.dashboard"))
}

func TestResolveIncrementalRefresh(t *testing.T) {
	t.Parallel()

	t.Run("default is incremental", func(t *testing.T) {
		t.Parallel()
		assert.True(t, resolveIncrementalRefresh(context.Background(), map[string]string{}))
	})

	t.Run("explicit incremental true", func(t *testing.T) {
		t.Parallel()
		assert.True(t, resolveIncrementalRefresh(context.Background(), map[string]string{"incremental": "true"}))
	})

	t.Run("explicit incremental false", func(t *testing.T) {
		t.Parallel()
		assert.False(t, resolveIncrementalRefresh(context.Background(), map[string]string{"incremental": "false"}))
	})

	t.Run("full refresh flag overrides incremental", func(t *testing.T) {
		t.Parallel()
		ctx := context.WithValue(context.Background(), pipeline.RunConfigFullRefresh, true)
		assert.False(t, resolveIncrementalRefresh(ctx, map[string]string{"incremental": "true"}))
	})

	t.Run("full refresh flag overrides default", func(t *testing.T) {
		t.Parallel()
		ctx := context.WithValue(context.Background(), pipeline.RunConfigFullRefresh, true)
		assert.False(t, resolveIncrementalRefresh(ctx, map[string]string{}))
	})
}

func TestResolveRefreshTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		params   map[string]string
		expected time.Duration
	}{
		{
			name:     "default timeout",
			params:   map[string]string{},
			expected: 60 * time.Minute,
		},
		{
			name:     "custom timeout",
			params:   map[string]string{"refresh_timeout_minutes": "30"},
			expected: 30 * time.Minute,
		},
		{
			name:     "invalid timeout uses default",
			params:   map[string]string{"refresh_timeout_minutes": "abc"},
			expected: 60 * time.Minute,
		},
		{
			name:     "zero timeout uses default",
			params:   map[string]string{"refresh_timeout_minutes": "0"},
			expected: 60 * time.Minute,
		},
		{
			name:     "negative timeout uses default",
			params:   map[string]string{"refresh_timeout_minutes": "-5"},
			expected: 60 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := resolveRefreshTimeout(tt.params)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetBoolParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		params    map[string]string
		key       string
		wantValue bool
		wantOk    bool
	}{
		{"missing key", map[string]string{}, "key", false, false},
		{"empty value", map[string]string{"key": ""}, "key", false, false},
		{"true", map[string]string{"key": "true"}, "key", true, true},
		{"false", map[string]string{"key": "false"}, "key", false, true},
		{"invalid", map[string]string{"key": "maybe"}, "key", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			val, ok := getBoolParam(tt.params, tt.key)
			assert.Equal(t, tt.wantValue, val)
			assert.Equal(t, tt.wantOk, ok)
		})
	}
}

func TestGetIntParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		params    map[string]string
		key       string
		wantValue int
		wantOk    bool
	}{
		{"missing key", map[string]string{}, "key", 0, false},
		{"empty value", map[string]string{"key": ""}, "key", 0, false},
		{"valid int", map[string]string{"key": "42"}, "key", 42, true},
		{"invalid", map[string]string{"key": "abc"}, "key", 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			val, ok := getIntParam(tt.params, tt.key)
			assert.Equal(t, tt.wantValue, val)
			assert.Equal(t, tt.wantOk, ok)
		})
	}
}

func TestIsIncrementalNotSupported(t *testing.T) {
	t.Parallel()

	assert.True(t, isIncrementalNotSupported(errors.New("incremental refresh is not supported")))
	assert.False(t, isIncrementalNotSupported(errors.New("rate limit exceeded")))
}
