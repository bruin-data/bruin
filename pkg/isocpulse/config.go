package isocpulse

import (
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	Token string `json:"token"`
}

func (c Config) GetIngestrURI() (string, error) {
	u := url.URL{
		Scheme: "isoc-pulse",
		RawQuery: url.Values{
			"token": []string{c.Token},
		}.Encode(),
	}

	return u.String(), nil
}

type Client struct {
	cfg Config
}

func NewClient(cfg Config) (*Client, error) {
	token := strings.TrimSpace(cfg.Token)
	if token == "" {
		return nil, errors.New("token is required")
	}
	return &Client{
		cfg: cfg,
	}, nil
}

func (c *Client) GetIngestrURI() (string, error) {
	return c.cfg.GetIngestrURI()
}
