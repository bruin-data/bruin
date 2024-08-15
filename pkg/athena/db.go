package athena

import (
	"context"
	"database/sql"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/pkg/errors"
	"log"
	"sync"

	"github.com/bruin-data/bruin/pkg/query"
	_ "github.com/uber/athenadriver/go"
)

type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type Client struct {
	connection querier
	config     *config.AwsConnection
	mutex      sync.Mutex
}

type AthenaConfig interface {
	ToDBConnectionURI() string
	GetIngestrURI() string
}

func NewClient(ctx context.Context, c *config.AwsConnection) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) initializeDb() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.connection != nil {
		return nil
	}

	athenaUri, err := c.config.ToAthenaDsn()
	if err != nil {
		return errors.Wrap(err, "failed to create DSN for Athena")
	}

	db, err := sql.Open("athena", athenaUri)
	if err != nil {
		return errors.Errorf("Failed to open database connection: %v", err)
	}

	c.connection = db
	return nil
}

func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
	err := c.initializeDb()
	if err != nil {
		return err
	}

	_, err = c.connection.QueryContext(ctx, query.String())
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.ToIngestrURI()
}

// Select runs a query and returns the results.
func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	err := c.initializeDb()
	if err != nil {
		return nil, err
	}

	rows, err := c.connection.QueryContext(ctx, query.String())
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	results := make([][]interface{}, 0)

	for rows.Next() {
		var result []interface{}
		if err := rows.Scan(&result); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		results = append(results, result)
	}

	if len(results) == 0 {
		return make([][]interface{}, 0), nil
	}

	return results, nil
}

type awsConnectionFetcher interface {
	GetAwsConnection(name string) (*config.AwsConnection, error)
}

type AthenaConnectionFetcher struct {
	fetcher     awsConnectionFetcher
	connections map[string]*Client
	mutex       sync.Mutex
}

func NewAthenaConnectionFetcher(fetcher awsConnectionFetcher) *AthenaConnectionFetcher {
	return &AthenaConnectionFetcher{
		fetcher:     fetcher,
		connections: make(map[string]*Client),
	}
}

func (a *AthenaConnectionFetcher) GetAthenaClient(name string) (AthenaQuerier, error) {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	if conn, ok := a.connections[name]; ok {
		return conn, nil
	}

	awsConn, err := a.fetcher.GetAwsConnection(name)
	if err != nil {
		return nil, err
	}

	client, err := NewClient(context.Background(), awsConn)
	if err != nil {
		return nil, err
	}

	a.connections[name] = client

	return client, nil
}

func (a *AthenaConnectionFetcher) GetConnection(name string) (interface{}, error) {
	return a.GetAthenaClient(name)
}
