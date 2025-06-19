package quickbooks

// Client provides access to QuickBooks via ingestr.
type Client struct {
	config Config
}

// NewClient initializes a QuickBooks client with given configuration.
func NewClient(c Config) (*Client, error) {
	return &Client{config: c}, nil
}

// GetIngestrURI returns the ingestr URI for the client.
func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
