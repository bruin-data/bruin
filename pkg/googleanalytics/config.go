package googleanalytics

import (
	"net/url"
)

type Config struct {
	ServiceAccountFile string
	PropertyID         string
}

func (c *Config) GetIngestrURI() string {
	//googleanalytics://?credentials_path=/path/to/service/account.json&property_id=<property_id>
	baseURL := "googleanalytics://"
	params := url.Values{}
	params.Add("credentials_path", c.ServiceAccountFile)
	params.Add("property_id", c.PropertyID)
	return baseURL + "?" + params.Encode()
}
