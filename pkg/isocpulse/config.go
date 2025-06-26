package isocpulse

import (
	"fmt"
	"net/url"
	"strings"
)

type Config struct {
	Token string `json:"token"`
}

func (c Config) GetIngestrURI() (string, error) {
	token := strings.TrimSpace(c.Token)
	if token == "" {
		return "", fmt.Errorf("token is required")
	}
	u := url.URL{
		Scheme: "isoc-pulse",
		RawQuery: url.Values{
			"token": []string{token},
		}.Encode(),
	}

	return u.String(), nil
}

type Client struct {
	cfg Config
}

func NewClient(cfg Config) (*Client, error) {
	return &Client{
		cfg: cfg,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.cfg.GetIngestrURI()
}
