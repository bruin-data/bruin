package bigquery

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/google"
)

func TestConfig_GetConnectionURI(t *testing.T) {
	t.Parallel()
	tests := []struct {
		Name                string
		ProjectID           string
		CredentialsFilePath string
		CredentialsJSON     string
		Credentials         *google.Credentials
		Location            string
		wantErr             bool
		Expected            string
	}{
		{
			Name:                "no creds",
			ProjectID:           "project-id",
			CredentialsFilePath: "",
			CredentialsJSON:     "",
			Credentials:         nil,
			Location:            "location",
			wantErr:             true,
		},
		{
			Name:                "creds file doesn't exist",
			ProjectID:           "project-id",
			CredentialsFilePath: "./creds-fake.json",
			CredentialsJSON:     "",
			Credentials:         nil,
			Location:            "location",
			wantErr:             true,
		},
		{
			Name:                "creds file",
			ProjectID:           "project-id",
			CredentialsFilePath: "./testdata/creds.json",
			CredentialsJSON:     "",
			Credentials:         nil,
			Location:            "location",
			wantErr:             false,
			Expected:            "bigquery://project-id?credentials_base64=eyJjcmVkcyI6InNvbWUtY3JlZHMifQ==&location=location",
		},
		{
			Name:                "creds json",
			ProjectID:           "project-id",
			CredentialsFilePath: "",
			CredentialsJSON:     "{\"creds\":\"some-creds\"}",
			Credentials:         nil,
			Location:            "location",
			wantErr:             false,
			Expected:            "bigquery://project-id?credentials_base64=eyJjcmVkcyI6InNvbWUtY3JlZHMifQ==&location=location",
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()

			config := Config{
				ProjectID:           tt.ProjectID,
				CredentialsFilePath: tt.CredentialsFilePath,
				CredentialsJSON:     tt.CredentialsJSON,
				Credentials:         tt.Credentials,
				Location:            tt.Location,
			}

			result, err := config.GetConnectionURI()
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.Expected, result)
			}
		})
	}
}

func TestConfig_UsesApplicationDefaultCredentials(t *testing.T) {
	t.Parallel()
	tests := []struct {
		Name                             string
		CredentialsFilePath              string
		CredentialsJSON                  string
		Credentials                      *google.Credentials
		UseApplicationDefaultCredentials bool
		Expected                         bool
	}{
		{
			Name:                             "explicit ADC enabled",
			CredentialsFilePath:              "",
			CredentialsJSON:                  "",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: true,
			Expected:                         true,
		},
		{
			Name:                             "explicit ADC disabled with no other creds",
			CredentialsFilePath:              "",
			CredentialsJSON:                  "",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: false,
			Expected:                         false,
		},
		{
			Name:                             "implicit ADC when no creds provided",
			CredentialsFilePath:              "",
			CredentialsJSON:                  "",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: false,
			Expected:                         false,
		},
		{
			Name:                             "ADC enabled with service account file (ADC takes precedence)",
			CredentialsFilePath:              "/path/to/creds.json",
			CredentialsJSON:                  "",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: true,
			Expected:                         true,
		},
		{
			Name:                             "ADC enabled with service account JSON (ADC takes precedence)",
			CredentialsFilePath:              "",
			CredentialsJSON:                  "{\"creds\":\"some-creds\"}",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: true,
			Expected:                         true,
		},
		{
			Name:                             "ADC enabled with credentials object (ADC takes precedence)",
			CredentialsFilePath:              "",
			CredentialsJSON:                  "",
			Credentials:                      &google.Credentials{},
			UseApplicationDefaultCredentials: true,
			Expected:                         true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()

			config := Config{
				CredentialsFilePath:              tt.CredentialsFilePath,
				CredentialsJSON:                  tt.CredentialsJSON,
				Credentials:                      tt.Credentials,
				UseApplicationDefaultCredentials: tt.UseApplicationDefaultCredentials,
			}

			result := config.UsesApplicationDefaultCredentials()
			require.Equal(t, tt.Expected, result)
		})
	}
}

func TestConfig_IsValid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		Name                             string
		ProjectID                        string
		CredentialsFilePath              string
		CredentialsJSON                  string
		Credentials                      *google.Credentials
		UseApplicationDefaultCredentials bool
		Expected                         bool
	}{
		{
			Name:                             "valid with project ID and service account file",
			ProjectID:                        "project-id",
			CredentialsFilePath:              "/path/to/creds.json",
			CredentialsJSON:                  "",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: false,
			Expected:                         true,
		},
		{
			Name:                             "valid with project ID and service account JSON",
			ProjectID:                        "project-id",
			CredentialsFilePath:              "",
			CredentialsJSON:                  "{\"creds\":\"some-creds\"}",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: false,
			Expected:                         true,
		},
		{
			Name:                             "valid with project ID and credentials object",
			ProjectID:                        "project-id",
			CredentialsFilePath:              "",
			CredentialsJSON:                  "",
			Credentials:                      &google.Credentials{},
			UseApplicationDefaultCredentials: false,
			Expected:                         true,
		},
		{
			Name:                             "valid with project ID and ADC enabled",
			ProjectID:                        "project-id",
			CredentialsFilePath:              "",
			CredentialsJSON:                  "",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: true,
			Expected:                         true,
		},
		{
			Name:                             "invalid without project ID",
			ProjectID:                        "",
			CredentialsFilePath:              "/path/to/creds.json",
			CredentialsJSON:                  "",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: false,
			Expected:                         false,
		},
		{
			Name:                             "invalid without any credentials",
			ProjectID:                        "project-id",
			CredentialsFilePath:              "",
			CredentialsJSON:                  "",
			Credentials:                      nil,
			UseApplicationDefaultCredentials: false,
			Expected:                         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			t.Parallel()

			config := Config{
				ProjectID:                        tt.ProjectID,
				CredentialsFilePath:              tt.CredentialsFilePath,
				CredentialsJSON:                  tt.CredentialsJSON,
				Credentials:                      tt.Credentials,
				UseApplicationDefaultCredentials: tt.UseApplicationDefaultCredentials,
			}

			result := config.IsValid()
			require.Equal(t, tt.Expected, result)
		})
	}
}

func TestNewDB_ADCWarning(t *testing.T) {
	t.Parallel()

	config := &Config{
		ProjectID:                        "test-project",
		CredentialsFilePath:              "/path/to/creds.json",
		UseApplicationDefaultCredentials: true,
	}

	client, err := NewDB(config)

	if err != nil && !strings.Contains(err.Error(), "failed to create bigquery client") {
		t.Errorf("Expected client creation to succeed or fail with credential error, got: %v", err)
	}

	if client != nil {
		if !client.config.UseApplicationDefaultCredentials {
			t.Error("Expected UseApplicationDefaultCredentials to be true")
		}
	}
}
