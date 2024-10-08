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

func (c *Config) ToDBConnectionURI() string {
	conf, err := drv.NewDefaultConfig(c.OutputBucket, c.Region, c.AccessID, c.SecretAccessKey)
	conf.SetDB(c.Database)
	if err != nil {
		log.Fatalf("Failed to create Athena config: %v", err)
		return ""
	}

	return conf.Stringify()
}
