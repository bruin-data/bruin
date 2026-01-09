package dataprocserverless

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		config    Config
		wantErr   bool
		errFields []string
	}{
		{
			name: "valid with service account JSON",
			config: Config{
				ServiceAccountJSON: `{"type": "service_account"}`,
				ProjectID:          "my-project",
				Region:             "us-central1",
				Workspace:          "gs://my-bucket/workspace",
			},
			wantErr: false,
		},
		{
			name: "valid with service account file",
			config: Config{
				ServiceAccountFile: "/path/to/credentials.json",
				ProjectID:          "my-project",
				Region:             "us-central1",
				Workspace:          "gs://my-bucket/workspace",
			},
			wantErr: false,
		},
		{
			name: "valid with ADC enabled",
			config: Config{
				UseApplicationDefaultCredentials: true,
				ProjectID:                        "my-project",
				Region:                           "us-central1",
				Workspace:                        "gs://my-bucket/workspace",
			},
			wantErr: false,
		},
		{
			name: "valid with ADC and optional fields",
			config: Config{
				UseApplicationDefaultCredentials: true,
				ProjectID:                        "my-project",
				Region:                           "us-central1",
				Workspace:                        "gs://my-bucket/workspace",
				ExecutionRole:                    "my-service-account@my-project.iam.gserviceaccount.com",
				SubnetworkURI:                    "projects/my-project/regions/us-central1/subnetworks/my-subnet",
			},
			wantErr: false,
		},
		{
			name: "invalid without any credentials",
			config: Config{
				ProjectID: "my-project",
				Region:    "us-central1",
				Workspace: "gs://my-bucket/workspace",
			},
			wantErr:   true,
			errFields: []string{"service_account_json, service_account_file, or use_application_default_credentials"},
		},
		{
			name: "invalid without project_id",
			config: Config{
				UseApplicationDefaultCredentials: true,
				Region:                           "us-central1",
				Workspace:                        "gs://my-bucket/workspace",
			},
			wantErr:   true,
			errFields: []string{"project_id"},
		},
		{
			name: "invalid without region",
			config: Config{
				UseApplicationDefaultCredentials: true,
				ProjectID:                        "my-project",
				Workspace:                        "gs://my-bucket/workspace",
			},
			wantErr:   true,
			errFields: []string{"region"},
		},
		{
			name: "invalid without workspace",
			config: Config{
				UseApplicationDefaultCredentials: true,
				ProjectID:                        "my-project",
				Region:                           "us-central1",
			},
			wantErr:   true,
			errFields: []string{"workspace"},
		},
		{
			name:    "invalid with empty config",
			config:  Config{},
			wantErr: true,
			errFields: []string{
				"project_id",
				"region",
				"workspace",
				"service_account_json, service_account_file, or use_application_default_credentials",
			},
		},
		{
			name: "ADC false is same as not set",
			config: Config{
				UseApplicationDefaultCredentials: false,
				ProjectID:                        "my-project",
				Region:                           "us-central1",
				Workspace:                        "gs://my-bucket/workspace",
			},
			wantErr:   true,
			errFields: []string{"service_account_json, service_account_file, or use_application_default_credentials"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.config.validate()

			if tt.wantErr {
				require.Error(t, err)
				var missingErr *MissingFieldsError
				require.True(t, errors.As(err, &missingErr), "expected MissingFieldsError, got %T", err)
				assert.Equal(t, tt.errFields, missingErr.Fields)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestClient_GetClientOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		config         Config
		expectEmpty    bool
		expectNumOpts  int
		expectContains string
	}{
		{
			name: "returns credentials JSON option when service account JSON is set",
			config: Config{
				ServiceAccountJSON: `{"type": "service_account"}`,
				ProjectID:          "my-project",
				Region:             "us-central1",
				Workspace:          "gs://my-bucket/workspace",
			},
			expectEmpty:   false,
			expectNumOpts: 1,
		},
		{
			name: "returns credentials file option when service account file is set",
			config: Config{
				ServiceAccountFile: "/path/to/credentials.json",
				ProjectID:          "my-project",
				Region:             "us-central1",
				Workspace:          "gs://my-bucket/workspace",
			},
			expectEmpty:   false,
			expectNumOpts: 1,
		},
		{
			name: "returns empty options for ADC",
			config: Config{
				UseApplicationDefaultCredentials: true,
				ProjectID:                        "my-project",
				Region:                           "us-central1",
				Workspace:                        "gs://my-bucket/workspace",
			},
			expectEmpty:   true,
			expectNumOpts: 0,
		},
		{
			name: "service account JSON takes precedence over file",
			config: Config{
				ServiceAccountJSON: `{"type": "service_account"}`,
				ServiceAccountFile: "/path/to/credentials.json",
				ProjectID:          "my-project",
				Region:             "us-central1",
				Workspace:          "gs://my-bucket/workspace",
			},
			expectEmpty:   false,
			expectNumOpts: 1,
		},
		{
			name: "service account JSON takes precedence over ADC",
			config: Config{
				ServiceAccountJSON:               `{"type": "service_account"}`,
				UseApplicationDefaultCredentials: true,
				ProjectID:                        "my-project",
				Region:                           "us-central1",
				Workspace:                        "gs://my-bucket/workspace",
			},
			expectEmpty:   false,
			expectNumOpts: 1,
		},
		{
			name: "service account file takes precedence over ADC",
			config: Config{
				ServiceAccountFile:               "/path/to/credentials.json",
				UseApplicationDefaultCredentials: true,
				ProjectID:                        "my-project",
				Region:                           "us-central1",
				Workspace:                        "gs://my-bucket/workspace",
			},
			expectEmpty:   false,
			expectNumOpts: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := &Client{Config: tt.config}
			opts := client.getClientOptions()

			if tt.expectEmpty {
				assert.Empty(t, opts)
			} else {
				assert.Len(t, opts, tt.expectNumOpts)
			}
		})
	}
}

func TestNewClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name: "creates client with valid service account JSON config",
			config: Config{
				ServiceAccountJSON: `{"type": "service_account"}`,
				ProjectID:          "my-project",
				Region:             "us-central1",
				Workspace:          "gs://my-bucket/workspace",
			},
			wantErr: false,
		},
		{
			name: "creates client with valid service account file config",
			config: Config{
				ServiceAccountFile: "/path/to/credentials.json",
				ProjectID:          "my-project",
				Region:             "us-central1",
				Workspace:          "gs://my-bucket/workspace",
			},
			wantErr: false,
		},
		{
			name: "creates client with valid ADC config",
			config: Config{
				UseApplicationDefaultCredentials: true,
				ProjectID:                        "my-project",
				Region:                           "us-central1",
				Workspace:                        "gs://my-bucket/workspace",
			},
			wantErr: false,
		},
		{
			name: "fails with invalid config - no credentials",
			config: Config{
				ProjectID: "my-project",
				Region:    "us-central1",
				Workspace: "gs://my-bucket/workspace",
			},
			wantErr: true,
		},
		{
			name: "fails with invalid config - missing required fields",
			config: Config{
				UseApplicationDefaultCredentials: true,
			},
			wantErr: true,
		},
		{
			name:    "fails with empty config",
			config:  Config{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, err := NewClient(tt.config)

			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(t, client)
			} else {
				require.NoError(t, err)
				require.NotNil(t, client)
				assert.Equal(t, tt.config.ProjectID, client.ProjectID)
				assert.Equal(t, tt.config.Region, client.Region)
				assert.Equal(t, tt.config.Workspace, client.Workspace)
				assert.Equal(t, tt.config.UseApplicationDefaultCredentials, client.UseApplicationDefaultCredentials)
			}
		})
	}
}
