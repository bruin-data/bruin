package elasticsearch

import (
	"fmt"
	"net/url"
)

type Config struct {
	Username    string
	Password    string
	Host        string
	Port        int
	Secure      string
	VerifyCerts string
}

func (c *Config) GetIngestrURI() string {
	//elasticsearch://elastic:changeme@localhost:9200?secure=false&verify_certs=false

	u := &url.URL{
		Scheme: "elasticsearch",
		Host:   fmt.Sprintf("%s:%d", c.Host, c.Port),
	}

	if c.Secure == "" {
		c.Secure = "true"
	}

	if c.VerifyCerts == "" {
		c.VerifyCerts = "true"
	}

	if c.Username != "" {
		u.User = url.UserPassword(c.Username, c.Password)
	}

	u.RawQuery = url.Values{
		"secure":       {c.Secure},
		"verify_certs": {c.VerifyCerts},
	}.Encode()

	fmt.Println(u.String())
	return u.String()
}
