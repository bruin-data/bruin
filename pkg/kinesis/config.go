package kinesis

import "fmt"

type Config struct {
	AccessKeyID     string
	SecretAccessKey string
	Region          string
}

func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf("kinesis://?aws_access_key_id=%s&aws_secret_access_key=%s&region_name=%s", c.AccessKeyID, c.SecretAccessKey, c.Region)
}
