package gitlab

type Client struct {
	config GitLabConfig
}

type GitLabConfig interface {
	GetIngestrURI() string
}

func NewClient(c GitLabConfig) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.config.GetIngestrURI(), nil
}
