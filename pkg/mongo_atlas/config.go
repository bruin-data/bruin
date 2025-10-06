package mongoatlas

import (
	"net/url"
)

type Config struct {
	Username string
	Password string
	Host     string
	Database string
}

func (c *Config) GetIngestrURI() string {
	// MongoDB Atlas URI format: mongodb+srv://username:password@host/database
	u := &url.URL{
		Scheme: "mongodb+srv",
		Host:   c.Host,
		Path:   c.Database,
	}

	if c.Username != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	}

	return u.String()
}
