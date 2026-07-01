package gsheets

import (
	"encoding/base64"
	"os"
)

type Config struct {
	ServiceAccountFile string
	ServiceAccountJSON string
}

func (c *Config) GetIngestrURI() string {
	var creds []byte
	switch {
	case c.ServiceAccountJSON != "":
		creds = []byte(c.ServiceAccountJSON)
	case c.ServiceAccountFile != "":
		var err error
		creds, err = os.ReadFile(c.ServiceAccountFile)
		if err != nil {
			return ""
		}
	default:
		// No credentials provided: fall back to Application Default Credentials.
		return "gsheets://"
	}

	return "gsheets://?credentials_base64=" + base64.StdEncoding.EncodeToString(creds)
}
