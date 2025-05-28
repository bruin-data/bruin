package solidgate

import (
	"net/url"
)

type Config struct {
	SecretKey string
	PublicKey string
}

func (c *Config) GetIngestrURI() string {
	q := url.Values{}
	q.Set("secret_key", c.SecretKey)
	q.Set("public_key", c.PublicKey)
	return "solidgate://?" + q.Encode()
}
