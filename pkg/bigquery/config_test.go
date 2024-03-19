package bigquery

import (
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
			Expected:            "bigquery://project-id?credentials_base64=eyJjcmVkcyI6InNvbWUtY3JlZHMifQ==",
		},
		{
			Name:                "creds json",
			ProjectID:           "project-id",
			CredentialsFilePath: "",
			CredentialsJSON:     "{\"creds\":\"some-creds\"}",
			Credentials:         nil,
			Location:            "location",
			wantErr:             false,
			Expected:            "bigquery://project-id?credentials_base64=eyJjcmVkcyI6InNvbWUtY3JlZHMifQ==",
		},
	}
	for _, tt := range tests {
		tt := tt
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
