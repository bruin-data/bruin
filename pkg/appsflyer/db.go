package appsflyer

import "fmt"

type Client struct {
	config Config
}

type AppsflyerConfig interface {
	GetIngestrURI() string
}

func NewClient(c Config) (*Client, error) {
	return &Client{
		config: c,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	fmt.Println("Getting AppsFlyer ingestion URI")
	return c.config.GetIngestrURI(), nil
}
