package kinesis

import (
	"net/url"
)

type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

func (c *Config) GetIngestrURI() string {
	query := url.Values{}
	query.Set("aws_access_key_id", c.AccessKeyID)
	query.Set("aws_secret_access_key", c.SecretAccessKey)
	query.Set("region_name", c.Region)
	u := url.URL{
		Scheme:   "kinesis",
		RawQuery: query.Encode(),
	}
	return u.String()
}
