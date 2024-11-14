package gsheets

import (
	"encoding/base64"
	"fmt"
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
	return fmt.Sprintf("gsheets://?credentials_base64=%s", base64.StdEncoding.EncodeToString(creds))
}
