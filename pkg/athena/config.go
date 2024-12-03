package athena

import (
	"log"

	drv "github.com/uber/athenadriver/go"
)

type Config struct {
	OutputBucket    string
	Region          string
	AccessID        string
	SecretAccessKey string
	Database        string
}

func (c *Config) ToDBConnectionURI() (string, error) {
	conf, err := drv.NewDefaultConfig(c.OutputBucket, c.Region, c.AccessID, c.SecretAccessKey)
	if err != nil {
		log.Fatalf("Failed to create Athena config: %v", err)
		return "", err
	}

	conf.SetDB(c.Database)
	if err != nil {
		log.Fatalf("Failed to create Athena config: %v", err)
		return "", err
	}

	return conf.Stringify(), nil
}

func (c *Config) GetIngestrURI() string {
	return "athena://?bucket=" + c.OutputBucket + "&access_key_id=" + c.AccessID + "&secret_access_key=" + c.SecretAccessKey + "&region_name=" + c.Region
}
