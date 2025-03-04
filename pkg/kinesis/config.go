package kinesis

import "fmt"

type Config struct {
	AWS_ACCESS_KEY_ID     string
	AWS_SECRET_ACCESS_KEY string
	Region                string
}

func (c *Config) GetIngestrURI() string {
	return fmt.Sprintf("kinesis://?aws_access_key_id=%s&aws_secret_access_key=%s&region_name=%s", c.AWS_ACCESS_KEY_ID, c.AWS_SECRET_ACCESS_KEY, c.Region)
}
