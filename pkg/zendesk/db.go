package zendesk

type Client struct {
	config ZendeskConfig
}

type ZendeskConfig interface {
	GetIngestrURI() string
}

func NewClient(c Config) (*Client, error) {
	return &Client{
		config: c}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
