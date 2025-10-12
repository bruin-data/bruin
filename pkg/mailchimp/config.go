package mailchimp

import "net/url"

type Config struct {
	APIKey string
	Server string
}

func (c *Config) GetIngestrURI() string {
	u := &url.URL{
		Scheme: "mailchimp",
		Host:   "",
	}

	q := u.Query()
	if c.APIKey != "" {
		q.Set("api_key", c.APIKey)
	}
	if c.Server != "" {
		q.Set("server", c.Server)
	}
	u.RawQuery = q.Encode()

	return u.String()
}
