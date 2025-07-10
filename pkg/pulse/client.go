package pulse

// Client is a Pulse connection client.
type Client struct {
	cfg Config
}

// NewClient creates a new Pulse client.
func NewClient(cfg Config) (*Client, error) {
	if _, err := cfg.GetIngestrURI(); err != nil {
		return nil, err
	}
	return &Client{cfg: cfg}, nil
}

// GetIngestrURI returns the ingestr URI for this client.
func (c *Client) GetIngestrURI() (string, error) {
	return c.cfg.GetIngestrURI()
}
