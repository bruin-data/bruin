package dynamodb

import (
	"fmt"
	"net/url"
	"strings"
)

type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Set("access_key_id", c.AccessKeyID)
	params.Set("secret_access_key", c.SecretAccessKey)

	region := strings.TrimSpace(c.Region)
	uri := url.URL{
		Scheme:   "dynamodb",
		Host:     fmt.Sprintf("dynamodb.%s.amazonaws.com", region),
		RawQuery: params.Encode(),
	}
	return uri.String()
}
