package appsflyer

import "fmt"

type Config struct {
	ApiKey string
}

func (c *Config) GetIngestrURI() string {
	fmt.Println("Appsflyer API key:", c.ApiKey)
	return "appsflyer://?api_key=" + c.ApiKey
}
