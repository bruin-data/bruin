package bigquery

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var scopes = []string{
	bigquery.Scope,
	"https://www.googleapis.com/auth/cloud-platform",
	"https://www.googleapis.com/auth/drive",
}

type Querier interface {
	RunQueryWithoutResult(ctx context.Context, query *query.Query) error
}

type Selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}

type DB interface {
	Querier
	Selector
}

type Client struct {
	client *bigquery.Client
	config *Config
}

func NewDB(c *Config) (*Client, error) {
	options := []option.ClientOption{
		option.WithScopes(scopes...),
	}

	switch {
	case c.CredentialsJSON != "":
		options = append(options, option.WithCredentialsJSON([]byte(c.CredentialsJSON)))
	case c.CredentialsFilePath != "":
		options = append(options, option.WithCredentialsFile(c.CredentialsFilePath))
	case c.Credentials != nil:
		options = append(options, option.WithCredentials(c.Credentials))
	default:
		return nil, errors.New("no credentials provided")
	}

	client, err := bigquery.NewClient(
		context.Background(),
		c.ProjectID,
		options...,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create bigquery client")
	}

	if c.Location != "" {
		client.Location = c.Location
	}

	return &Client{
		client: client,
		config: c,
	}, nil
}

func (d *Client) GetConnectionURI() (string, error) {
	return d.config.GetConnectionURI()
}

func (d *Client) IsValid(ctx context.Context, query *query.Query) (bool, error) {
	q := d.client.Query(query.ToDryRunQuery())
	q.DryRun = true

	job, err := q.Run(ctx)
	if err != nil {
		return false, formatError(err)
	}

	status := job.LastStatus()
	if err := status.Err(); err != nil {
		return false, err
	}

	return true, nil
}

func (d *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	q := d.client.Query(query.String())
	_, err := q.Read(ctx)
	if err != nil {
		return formatError(err)
	}

	return nil
}

func (d *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	q := d.client.Query(query.String())
	rows, err := q.Read(ctx)
	if err != nil {
		return nil, formatError(err)
	}

	result := make([][]interface{}, 0)
	for {
		var values []bigquery.Value
		err := rows.Next(&values)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}

		interfaces := make([]interface{}, len(values))
		for i, v := range values {
			interfaces[i] = v
		}

		result = append(result, interfaces)
	}

	return result, nil
}

func formatError(err error) error {
	var googleError *googleapi.Error
	if !errors.As(err, &googleError) {
		return err
	}

	if googleError.Code == 404 || googleError.Code == 400 {
		return fmt.Errorf("%s", googleError.Message)
	}

	return googleError
}
