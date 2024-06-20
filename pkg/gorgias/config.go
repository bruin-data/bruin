package gorgias

import (
	"fmt"
)

type Config struct {
	Domain string
	APIKey string
	Email  string
}

func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf("gorgias://%s?api_key=%s&email=%s", c.Domain, c.APIKey, c.Email)
}
