package shopify

type Client struct {
	config ShopifyConfig
}

type ShopifyConfig interface {
	GetIngestrURI() string
}

func NewClient(c ShopifyConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
