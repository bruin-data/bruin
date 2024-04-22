package mysql

import (
	_ "github.com/go-sql-driver/mysql"
)

// Commented out lots of things that we might need in the future to include mysql assets

type Client struct {
	// conn connection
	config MySQLConfig
}

type MySQLConfig interface {
	GetIngestrURI() string
	// ToDBConnectionURI() string
}

func NewClient(c MySQLConfig) (*Client, error) {
	//  conn, err := sqlx.Connect("mysql", c.ToDBConnectionURI())
	//  if err != nil {
	//	  return nil, err
	//  }

	return &Client{
		// connection: conn,
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}

//
//  type connection interface {
//	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
//	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
//  }
//
//  func (c *Client) RunQueryWithoutResult(ctx context.Context, query *query.Query) error {
//	_, err := c.connection.ExecContext(ctx, query.String())
//	if err != nil {
//		return err
//	}
//
//	return nil
//  }
//
//// Select runs a query and returns the results.
//  func (c *Client) Select(ctx context.Context, query *query.Query) ([][]interface{}, error) {
//	rows, err := c.connection.QueryContext(ctx, query.String())
//	if err != nil {
//		return nil, err
//	}
//
//	defer rows.Close()
//
//	collectedRows := make([][]interface{}, 0)
//
//	for rows.Next() {
//		var row []interface{}
//		err := rows.Scan(row)
//		if err != nil {
//			return nil, errors.Wrap(err, "failed to collect row values")
//		}
//		collectedRows = append(collectedRows, row)
//	}
//
//	return collectedRows, nil
//  }
