package gsheets

import "fmt"

type Config struct {
	CredentialsBase64 string
}

func (c *Config) GetIngestrURI() string {
	fmt.Println("CredentialsBase64", c.CredentialsBase64)
	return "gsheets://?credentials_base64=" + c.CredentialsBase64
}
