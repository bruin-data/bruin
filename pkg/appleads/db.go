package appleads

type Client struct {
	config Config
}

type AppleAdsConfig interface {
	GetIngestrURI() string
}

func NewClient(c Config) (*Client, error) {
	return &Client{c}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
