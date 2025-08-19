package revenuecat

// Client provides access to RevenueCat via ingestr.
type Client struct {
	config Config
}

// NewClient initializes a RevenueCat client with given configuration.
func NewClient(c Config) (*Client, error) {
	return &Client{config: c}, nil
}

// GetIngestrURI returns the ingestr URI for the client.
func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}