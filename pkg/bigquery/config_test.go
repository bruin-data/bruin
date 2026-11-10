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
		AccessToken         string
		Credentials         *google.Credentials
		Location            string
		wantErr             bool
		Expected            string
	}{
		{
			Name:        "access token cannot be used for ingestr",
			ProjectID:   "project-id",
			AccessToken: "ya29.some-token",
			Location:    "location",
			wantErr:     true,
		},
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
				AccessToken:         tt.AccessToken,
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

func TestConfig_IsValid(t *testing.T) {
	t.Parallel()
	tests := []struct {
		Name                             string
		ProjectID                        string
		CredentialsFilePath              string
		CredentialsJSON                  string
		AccessToken                      string
		Credentials                      *google.Credentials
		UseApplicationDefaultCredentials bool
		Expected                         bool
	}{
		{
			Name:        "valid with project ID and access token",
			ProjectID:   "project-id",
			AccessToken: "ya29.some-token",
			Expected:    true,
		},
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
				AccessToken:                      tt.AccessToken,
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

	if err != nil && !strings.Contains(err.Error(), "failed to create BigQuery client using Application Default Credentials (ADC)") {
		t.Errorf("Expected client creation to succeed or fail with credential error, got: %v", err)
	}

	if client != nil {
		if !client.config.UseApplicationDefaultCredentials {
			t.Error("Expected UseApplicationDefaultCredentials to be true")
		}
	}
}

func TestMaxBillableBytesFromEnv(t *testing.T) { //nolint:paralleltest // uses t.Setenv
	for _, tc := range []struct {
		name         string
		env          string
		byConnection string
		want         *int64
		wantErr      bool
	}{
		{name: "unset means no cap"},
		{name: "one cap for every connection", env: " 1000 ", want: int64Ptr(1000)},
		{name: "invalid fails closed", env: "lots", wantErr: true},
		{name: "zero fails closed", env: "0", wantErr: true},
		{name: "per-connection cap", byConnection: `{"bq-prod": 500, "bq-dev": 900}`, want: int64Ptr(500)},
		{name: "per-connection cap wins over the shared cap", env: "1000", byConnection: `{"bq-prod": 5000}`, want: int64Ptr(5000)},
		{name: "a connection missing from the map falls back to the shared cap", env: "1000", byConnection: `{"bq-dev": 900}`, want: int64Ptr(1000)},
		{name: "a connection missing from the map has no cap", byConnection: `{"bq-dev": 900}`},
		{name: "invalid map fails closed", byConnection: `{"bq-prod": "lots"}`, wantErr: true},
		{name: "null map fails closed", byConnection: `null`, wantErr: true},
		{name: "empty map is no per-connection cap", env: "1000", byConnection: `{}`, want: int64Ptr(1000)},
		{name: "non-positive map value fails closed", byConnection: `{"bq-prod": 0}`, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(MaxBillableBytesEnv, tc.env)
			t.Setenv(MaxBillableBytesByConnectionEnv, tc.byConnection)

			got, err := MaxBillableBytesFromEnv("bq-prod")
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestBytesBilledLimitTakesTheStricterCap(t *testing.T) {
	t.Parallel()

	require.Nil(t, Config{}.bytesBilledLimit())
	require.Equal(t, int64Ptr(500), Config{MaxBillableBytes: int64Ptr(500)}.bytesBilledLimit())
	require.Equal(t, int64Ptr(1000), Config{EnvMaxBillableBytes: int64Ptr(1000)}.bytesBilledLimit())
	require.Equal(t, int64Ptr(1000), Config{MaxBillableBytes: int64Ptr(5000), EnvMaxBillableBytes: int64Ptr(1000)}.bytesBilledLimit())
	require.Equal(t, int64Ptr(500), Config{MaxBillableBytes: int64Ptr(500), EnvMaxBillableBytes: int64Ptr(1000)}.bytesBilledLimit())
}

func TestEnvCapAloneDoesNotTriggerTheDryRunCheck(t *testing.T) {
	t.Parallel()

	client := &Client{config: &Config{EnvMaxBillableBytes: int64Ptr(1000)}}
	require.False(t, client.hasActiveQueryLimits(t.Context()))
}

func int64Ptr(v int64) *int64 {
	return &v
}
