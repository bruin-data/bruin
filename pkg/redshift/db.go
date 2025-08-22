package redshift

import (
	"context"
	"fmt"
	"strings"

	"github.com/bruin-data/bruin/pkg/query"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
)

// Config represents Redshift connection configuration
type Config struct {
	Username string
	Password string
	Host     string
	Port     int
	Database string
	Schema   string
	SslMode  string
}

// ToDBConnectionURI returns a connection URI to be used with the pgx package.
func (c Config) ToDBConnectionURI() string {
	connectionURI := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.Username,
		c.Password,
		c.Host,
		c.Port,
		c.Database,
		c.SslMode,
	)
	if c.Schema != "" {
		connectionURI += "&search_path=" + c.Schema
	}

	return connectionURI
}

func (c Config) GetIngestrURI() string {
	return fmt.Sprintf("redshift://%s:%s@%s:%d/%s",
		c.Username,
		c.Password,
		c.Host,
		c.Port,
		c.Database,
	)
}

func (c Config) GetDatabase() string {
	return c.Database
}

type Client struct {
	connection *pgxpool.Pool
	config     *Config
}

func NewClient(ctx context.Context, config *Config) (*Client, error) {
	conn, err := pgxpool.New(ctx, config.ToDBConnectionURI())
	if err != nil {
		return nil, err
	}

	return &Client{
		connection: conn,
		config:     config,
	}, nil
}

func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
	rows, err := c.connection.Query(ctx, query.String())
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	collectedRows, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) ([]interface{}, error) {
		return row.Values()
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to collect row values")
	}

	if len(collectedRows) == 0 {
		return make([][]interface{}, 0), nil
	}

	return collectedRows, nil
}

func (c *Client) BuildTableExistsQuery(tableName string) (string, error) {
	tableComponents := strings.Split(tableName, ".")
	for _, component := range tableComponents {
		if component == "" {
			return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
		}
	}

	var schemaName string
	switch len(tableComponents) {
	case 1:
		schemaName = "public"
		tableName = tableComponents[0]
	case 2:
		schemaName = tableComponents[0]
		tableName = tableComponents[1]
	default:
		return "", fmt.Errorf("table name must be in format schema.table or table, '%s' given", tableName)
	}
	targetTable := tableName

	query := fmt.Sprintf(
		"SELECT COUNT(*) FROM SVV_TABLES WHERE schemaname = '%s' AND tablename = '%s'",
		schemaName,
		targetTable,
	)
	return strings.TrimSpace(query), nil
}
