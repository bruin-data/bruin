package mixpanel

// Client provides a minimal wrapper around configuration for Mixpanel.
type Client struct {
	config Config
}

// GetIngestrURI returns the ingestr connection URI for Mixpanel.
func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}

// NewClient returns a new Mixpanel client.
func NewClient(c Config) (*Client, error) {
	return &Client{c}, nil
}
