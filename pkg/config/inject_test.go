package config

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockDetailsGetter struct {
	mock.Mock
}

func (m *mockDetailsGetter) GetConnectionDetails(name string) any {
	args := m.Called(name)
	return args.Get(0)
}

func (m *mockDetailsGetter) GetConnectionType(name string) string {
	args := m.Called(name)
	return args.String(0)
}

func TestInjectConnectionEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		asset               *pipeline.Asset
		setup               func(*mockDetailsGetter)
		wantEnv             map[string]string
		wantEnvContains     map[string]string
		wantConnectionTypes map[string]string
	}{
		{
			name:  "no connection field does nothing",
			asset: &pipeline.Asset{},
			setup: func(_ *mockDetailsGetter) {},
			wantEnv:             map[string]string{},
			wantConnectionTypes: map[string]string{},
		},
		{
			name: "connection already in secrets is skipped",
			asset: &pipeline.Asset{
				Connection: "my_bq",
				Secrets: []pipeline.SecretMapping{
					{SecretKey: "my_bq", InjectedKey: "my_bq"},
				},
			},
			setup:               func(_ *mockDetailsGetter) {},
			wantEnv:             map[string]string{},
			wantConnectionTypes: map[string]string{},
		},
		{
			name: "connection not found in config is skipped",
			asset: &pipeline.Asset{
				Connection: "missing",
			},
			setup: func(m *mockDetailsGetter) {
				m.On("GetConnectionDetails", "missing").Return(nil)
			},
			wantEnv:             map[string]string{},
			wantConnectionTypes: map[string]string{},
		},
		{
			name: "connection is injected with type",
			asset: &pipeline.Asset{
				Connection: "my_bq",
			},
			setup: func(m *mockDetailsGetter) {
				m.On("GetConnectionDetails", "my_bq").Return(&GoogleCloudPlatformConnection{
					ProjectID: "test-project",
				})
				m.On("GetConnectionType", "my_bq").Return("google_cloud_platform")
			},
			wantEnvContains: map[string]string{
				"my_bq": `"project_id":"test-project"`,
			},
			wantConnectionTypes: map[string]string{
				"my_bq": "google_cloud_platform",
			},
		},
		{
			name: "generic connection injects raw value",
			asset: &pipeline.Asset{
				Connection: "slack_hook",
			},
			setup: func(m *mockDetailsGetter) {
				m.On("GetConnectionDetails", "slack_hook").Return(&GenericConnection{
					Value: "https://hooks.slack.com/xxx",
				})
				m.On("GetConnectionType", "slack_hook").Return("generic")
			},
			wantEnv: map[string]string{
				"slack_hook": "https://hooks.slack.com/xxx",
			},
			wantConnectionTypes: map[string]string{
				"slack_hook": "generic",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := &mockDetailsGetter{}
			tt.setup(cfg)

			envVars := make(map[string]string)
			connTypes := make(map[string]string)
			err := InjectConnectionEnv(cfg, tt.asset, envVars, connTypes)
			assert.NoError(t, err)

			assert.Equal(t, tt.wantConnectionTypes, connTypes)
			for k, v := range tt.wantEnv {
				assert.Contains(t, envVars, k)
				assert.Equal(t, v, envVars[k])
			}
			for k, substr := range tt.wantEnvContains {
				assert.Contains(t, envVars, k)
				assert.Contains(t, envVars[k], substr)
			}
			cfg.AssertExpectations(t)
		})
	}
}
