package csv

import (
	"net/url"
	"strings"
)

type Config struct {
	Path     string
	Encoding string
	Layout   string
}

func (c Config) GetIngestrURI() string {
	params := url.Values{}

	encoding := strings.TrimSpace(c.Encoding)
	if encoding != "" {
		params.Set("encoding", encoding)
	}

	layout := strings.TrimSpace(c.Layout)
	if layout != "" {
		params.Set("layout", layout)
	}

	uri := "csv://" + strings.TrimSpace(c.Path)
	if q := params.Encode(); q != "" {
		uri += "?" + q
	}

	return uri
}
