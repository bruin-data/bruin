package phantombuster

import "fmt"

type Config struct {
	//phantombuster://?api_key=wEVH4Y5nrdS0hvgcMydUGMFbAshiPNP7tmBVHd9f7BE
	APIKey string `yaml:"api_key" json:"api_key" mapstructure:"api_key"`
}

func (c *Config) GetIngestrURI() string {
	fmt.Println("phantombuster://?api_key=" + c.APIKey)
	return "phantombuster://?api_key=" + c.APIKey
}
