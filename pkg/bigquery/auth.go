package bigquery

import (
	"context"
	"os"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const readOnlyScope = "https://www.googleapis.com/auth/bigquery.readonly"

func (c Config) clientOptions(ctx context.Context) ([]option.ClientOption, error) {
	if c.ReadOnly {
		credentials, err := c.readOnlyCredentials(ctx)
		if err != nil {
			return nil, err
		}
		return []option.ClientOption{option.WithTokenSource(credentials.TokenSource)}, nil
	}

	options := []option.ClientOption{option.WithScopes(scopes...)}
	if c.UseApplicationDefaultCredentials {
		return options, nil
	}
	switch {
	case c.CredentialsJSON != "":
		options = append(options, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(c.CredentialsJSON)))
	case c.CredentialsFilePath != "":
		options = append(options, option.WithAuthCredentialsFile(option.ServiceAccount, c.CredentialsFilePath))
	case c.AccessToken != "":
		options = append(options, option.WithTokenSource(oauth2.StaticTokenSource(&oauth2.Token{AccessToken: c.AccessToken})))
	case c.Credentials != nil:
		options = append(options, option.WithCredentials(c.Credentials))
	default:
		return nil, errors.New("no credentials provided")
	}
	return options, nil
}

func (c Config) readOnlyCredentials(ctx context.Context) (*google.Credentials, error) {
	if c.UseApplicationDefaultCredentials || c.AccessToken != "" || c.Credentials != nil {
		return nil, errors.New("read_only requires service_account_json or service_account_file; ADC, access_token, and preconfigured credentials are not supported")
	}
	data := []byte(c.CredentialsJSON)
	if len(data) == 0 {
		if c.CredentialsFilePath == "" {
			return nil, errors.New("read_only requires service_account_json or service_account_file")
		}
		var err error
		data, err = os.ReadFile(c.CredentialsFilePath)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read read-only BigQuery credentials")
		}
	}
	credentials, err := google.CredentialsFromJSONWithType(ctx, data, google.ServiceAccount, readOnlyScope)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create read-only BigQuery credentials")
	}
	return credentials, nil
}
