package bigquery

import (
	"context"
	"fmt"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/pipeline"
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
	Ping(ctx context.Context) error
}
type Selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
	SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error)
}

type MetadataUpdater interface {
	UpdateTableMetadataIfNotExist(ctx context.Context, asset *pipeline.Asset) error
}

type DB interface {
	Querier
	Selector
	MetadataUpdater
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

func (d *Client) GetIngestrURI() (string, error) {
	return d.config.GetIngestrURI()
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

func (d *Client) SelectWithSchema(ctx context.Context, queryObj *query.Query) (*query.QueryResult, error) {
	q := d.client.Query(queryObj.String())
	rows, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to initiate query read: %w", err)
	}

	result := &query.QueryResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
	}

	for {
		var values []bigquery.Value
		err := rows.Next(&values)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read row: %w", err)
		}

		row := make([]interface{}, len(values))
		for i, v := range values {
			row[i] = v
		}
		result.Rows = append(result.Rows, row)
	}

	if rows.Schema != nil {
		for _, field := range rows.Schema {
			result.Columns = append(result.Columns, field.Name)
		}
	} else {
		return nil, errors.New("schema information is not available")
	}

	return result, nil
}

type NoMetadataUpdatedError struct{}

func (m NoMetadataUpdatedError) Error() string {
	return "no metadata found for the given asset to be pushed to BigQuery"
}

func (d *Client) UpdateTableMetadataIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	anyColumnHasDescription := false
	colsByName := make(map[string]*pipeline.Column, len(asset.Columns))
	for _, col := range asset.Columns {
		colsByName[col.Name] = &col
		if col.Description != "" {
			anyColumnHasDescription = true
		}
	}

	if asset.Description == "" && (len(asset.Columns) == 0 || !anyColumnHasDescription) {
		return NoMetadataUpdatedError{}
	}

	tableComponents := strings.Split(asset.Name, ".")
	if len(tableComponents) != 2 {
		return fmt.Errorf("asset name must be in schema.table format to update the metadata, '%s' given", asset.Name)
	}

	tableRef := d.client.Dataset(tableComponents[0]).Table(tableComponents[1])
	meta, err := tableRef.Metadata(ctx)
	if err != nil {
		return err
	}

	schema := meta.Schema
	colsChanged := false
	for _, field := range schema {
		if col, ok := colsByName[field.Name]; ok {
			field.Description = col.Description
			colsChanged = true
		}
	}

	update := bigquery.TableMetadataToUpdate{}

	if colsChanged {
		update.Schema = schema
	}

	if asset.Description != "" {
		update.Description = asset.Description
	}

	primaryKeys := asset.ColumnNamesWithPrimaryKey()
	if len(primaryKeys) > 0 {
		update.TableConstraints = &bigquery.TableConstraints{
			PrimaryKey: &bigquery.PrimaryKey{Columns: primaryKeys},
		}
	}

	if _, err = tableRef.Update(ctx, update, meta.ETag); err != nil {
		return errors.Wrap(err, "failed to update table metadata")
	}

	return nil
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

// Test runs a simple query (SELECT 1) to validate the connection.
func (d *Client) Ping(ctx context.Context) error {
	// Define the test query
	q := query.Query{
		Query: "SELECT 1",
	}

	// Use the existing RunQueryWithoutResult method
	err := d.RunQueryWithoutResult(ctx, &q)
	if err != nil {
		return errors.Wrap(err, "failed to run test query on Bigquery connection")
	}

	return nil // Return nil if the query runs successfully
}
