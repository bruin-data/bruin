package iceberg

// Client is a thin wrapper around Config. Iceberg is only used as an ingestr
// destination in Bruin, so the client just exposes the ingestr URI.
type Client struct {
	config Config
}

func NewClient(c Config) (*Client, error) {
	return &Client{config: c}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI()
}
