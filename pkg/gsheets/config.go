package gsheets

import (
	"encoding/base64"
	"os"
)

type Config struct {
	CredentialsFilePath string
	CredentialsJSON     string
}

func (c *Config) GetIngestrURI() string {
	var creds []byte
	switch {
	case c.CredentialsJSON != "":
		creds = []byte(c.CredentialsJSON)
	case c.CredentialsFilePath != "":
		var err error
		creds, err = os.ReadFile(c.CredentialsFilePath)
		if err != nil {
			return ""
		}
	default:
		return ""
	}

	return "gsheets://?credentials_base64=" + base64.StdEncoding.EncodeToString(creds)
}
