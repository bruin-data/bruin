package granola

// Client provides access to Granola via ingestr.
type Client struct {
	config Config
}

// NewClient initializes a Granola client with the given configuration.
func NewClient(c Config) (*Client, error) {
	return &Client{config: c}, nil
}

// GetIngestrURI returns the ingestr URI for the client.
func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI()
}
