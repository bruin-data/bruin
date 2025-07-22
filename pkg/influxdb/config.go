package influxdb

import (
	"fmt"
	"net/url"
)

type Config struct {
	Host   string
	Port   int
	Token  string
	Org    string
	Bucket string
	Secure string
}

func (c Config) GetIngestrURI() string {
	if c.Secure == "" {
		c.Secure = "true"
	}

	u := &url.URL{
		Scheme: "influxdb",
	}

	if c.Port != 0 {
		u.Host = fmt.Sprintf("%s:%d", c.Host, c.Port)
	} else {
		u.Host = c.Host
	}
	q := u.Query()
	if c.Token != "" {
		q.Set("token", c.Token)
	}
	if c.Org != "" {
		q.Set("org", c.Org)
	}
	if c.Bucket != "" {
		q.Set("bucket", c.Bucket)
	}
	q.Set("secure", c.Secure)
	u.RawQuery = q.Encode()
	fmt.Print(u.String())
	return u.String()
}
