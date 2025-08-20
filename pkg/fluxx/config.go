package fluxx

import "net/url"

type Config struct {
	Instance     string
	ClientID     string
	ClientSecret string
}

func (c *Config) GetIngestrURI() string {
	v := url.Values{}
	v.Set("client_id", c.ClientID)
	v.Set("client_secret", c.ClientSecret)
	return "fluxx://" + c.Instance + "?" + v.Encode()
}