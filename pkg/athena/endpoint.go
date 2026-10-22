package athena

import (
	"context"
	"database/sql/driver"
	"errors"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	drv "github.com/uber/athenadriver/go"
)

// endpointConnector runs queries against a custom Athena-compatible endpoint.
// athenadriver creates its AWS session internally and offers no endpoint option, so this
// connector owns the Athena client and query lifecycle while reusing the driver's row decoding.
type endpointConnector struct {
	api    athenaiface.AthenaAPI
	config *drv.Config
	tracer *drv.DriverTracer
}

// validateEndpoint checks the custom endpoint and its credentials for every path that uses it.
func (c *Config) validateEndpoint() (*url.URL, error) {
	endpoint, err := parseEndpointURL(c.Endpoint)
	if err != nil {
		return nil, err
	}
	// Never fall back to the default credential chain: it would sign requests to an arbitrary
	// host with ambient credentials, such as an instance role. A profile is resolved into
	// these keys when the connection is created.
	if c.AccessID == "" || c.SecretAccessKey == "" {
		return nil, errors.New("endpoint_url requires access_key_id and secret_access_key, or a profile")
	}
	return endpoint, nil
}

func newEndpointConnector(c *Config) (*endpointConnector, error) {
	endpoint, err := c.validateEndpoint()
	if err != nil {
		return nil, err
	}

	dsn, err := c.ToDBConnectionURI()
	if err != nil {
		return nil, err
	}
	driverConfig, err := drv.NewConfig(dsn)
	if err != nil {
		return nil, err
	}

	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(c.Region),
		Endpoint:    aws.String(endpoint.String()),
		Credentials: credentials.NewStaticCredentials(c.AccessID, c.SecretAccessKey, c.SessionToken),
		HTTPClient:  newEndpointHTTPClient(endpoint.Scheme == "http"),
	})
	if err != nil {
		return nil, err
	}

	return &endpointConnector{
		api:    athena.New(sess),
		config: driverConfig,
		tracer: drv.NewNoOpsObservability(),
	}, nil
}

func (c *endpointConnector) Connect(context.Context) (driver.Conn, error) {
	return &endpointConn{connector: c}, nil
}

func (c *endpointConnector) Driver() driver.Driver {
	return &drv.SQLDriver{}
}

type endpointConn struct {
	connector *endpointConnector
}

var errEndpointArgs = errors.New("query arguments are not supported with a custom Athena endpoint")

func (c *endpointConn) Prepare(string) (driver.Stmt, error) {
	return nil, errEndpointArgs
}

func (c *endpointConn) Close() error {
	return nil
}

func (c *endpointConn) Begin() (driver.Tx, error) {
	return nil, errors.New("athena does not support transactions")
}

func (c *endpointConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		return nil, errEndpointArgs
	}

	queryID, err := c.run(ctx, query)
	if err != nil {
		return nil, err
	}
	return drv.NewRows(ctx, c.connector.api, queryID, c.connector.config, c.connector.tracer)
}

func (c *endpointConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		return nil, errEndpointArgs
	}

	queryID, err := c.run(ctx, query)
	if err != nil {
		return nil, err
	}
	rows, err := drv.NewRows(ctx, c.connector.api, queryID, c.connector.config, c.connector.tracer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if rows.ResultOutput != nil && rows.ResultOutput.UpdateCount != nil {
		return driver.RowsAffected(*rows.ResultOutput.UpdateCount), nil
	}
	return driver.RowsAffected(0), nil
}

// run starts the query and waits for it to succeed, returning its execution ID.
func (c *endpointConn) run(ctx context.Context, query string) (string, error) {
	api := c.connector.api
	config := c.connector.config

	resp, err := api.StartQueryExecutionWithContext(ctx, &athena.StartQueryExecutionInput{
		QueryString:           aws.String(query),
		QueryExecutionContext: &athena.QueryExecutionContext{Database: aws.String(config.GetDB())},
		ResultConfiguration:   &athena.ResultConfiguration{OutputLocation: aws.String(config.GetOutputBucket())},
		WorkGroup:             aws.String(drv.DefaultWGName),
	})
	if err != nil {
		return "", err
	}
	queryID := aws.StringValue(resp.QueryExecutionId)

	for {
		status, err := api.GetQueryExecutionWithContext(ctx, &athena.GetQueryExecutionInput{
			QueryExecutionId: aws.String(queryID),
		})
		if err != nil {
			// The query may still be running, e.g. when ctx was canceled mid-request.
			c.stop(ctx, queryID)
			if ctx.Err() != nil {
				return "", ctx.Err()
			}
			return "", err
		}

		execStatus := status.QueryExecution.Status
		switch aws.StringValue(execStatus.State) {
		case athena.QueryExecutionStateSucceeded:
			return queryID, nil
		case athena.QueryExecutionStateFailed:
			return "", errors.New(aws.StringValue(execStatus.StateChangeReason))
		case athena.QueryExecutionStateCancelled:
			return "", context.Canceled
		}

		select {
		case <-ctx.Done():
			c.stop(ctx, queryID)
			return "", ctx.Err()
		case <-time.After(config.GetResultPollIntervalSeconds()):
		}
	}
}

// stop makes a best-effort attempt to stop an unfinished query. ctx may already be done,
// so the request runs on a detached context with its own deadline.
func (c *endpointConn) stop(ctx context.Context, queryID string) {
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	_, _ = c.connector.api.StopQueryExecutionWithContext(stopCtx, &athena.StopQueryExecutionInput{
		QueryExecutionId: aws.String(queryID),
	})
}
