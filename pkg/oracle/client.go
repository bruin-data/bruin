package oracle

import "context"

// Keeping the original Client for backward compatibility
type Client struct {
	db *DB
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.db.GetIngestrURI()
}

func (c *Client) Ping(ctx context.Context) error {
	return c.db.Ping(ctx)
}

func NewClient(config Config) (*Client, error) {
	db, err := NewDB(&config)
	if err != nil {
		return nil, err
	}
	return &Client{db: db}, nil
}
