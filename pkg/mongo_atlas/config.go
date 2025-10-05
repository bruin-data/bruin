package mongoatlas

import (
	"fmt"
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
	return fmt.Sprintf("mongodb+srv://%s:%s@%s/%s",
		url.PathEscape(c.Username),
		url.PathEscape(c.Password),
		c.Host,
		c.Database,
	)
}
