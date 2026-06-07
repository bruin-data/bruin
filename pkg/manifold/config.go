package manifold

import "net/url"

type Config struct {
	QueryParams     map[string]string
	QueryParamLists map[string][]string
}

func (c *Config) GetIngestrURI() string {
	params := url.Values{}

	for key, value := range c.QueryParams {
		if key == "" || value == "" {
			continue
		}
		params.Set(key, value)
	}

	for key, values := range c.QueryParamLists {
		if key == "" {
			continue
		}
		for _, value := range values {
			if value == "" {
				continue
			}
			params.Add(key, value)
		}
	}

	if len(params) == 0 {
		return "manifold://"
	}

	return "manifold://?" + params.Encode()
}
