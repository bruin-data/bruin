package sharepoint

import (
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type Config struct {
	TenantID     string
	ClientID     string
	ClientSecret string
	Hostname     string
	Site         string
	Library      string
	MaxFileSize  *int64
	MaxFiles     *int64
}

func (c Config) GetIngestrURI() (string, error) {
	params := url.Values{}

	required := map[string]string{
		"tenant_id":     c.TenantID,
		"client_id":     c.ClientID,
		"client_secret": c.ClientSecret,
		"hostname":      c.Hostname,
		"site":          c.Site,
	}

	for key, value := range required {
		value = strings.TrimSpace(value)
		if value == "" {
			return "", fmt.Errorf("sharepoint: %s must be provided", key)
		}
		params.Set(key, value)
	}

	if library := strings.TrimSpace(c.Library); library != "" {
		params.Set("library", library)
	}

	if c.MaxFileSize != nil {
		if *c.MaxFileSize < 0 {
			return "", errors.New("sharepoint: max_file_size cannot be negative")
		}
		params.Set("max_file_size", strconv.FormatInt(*c.MaxFileSize, 10))
	}

	if c.MaxFiles != nil {
		if *c.MaxFiles < 0 {
			return "", errors.New("sharepoint: max_files cannot be negative")
		}
		params.Set("max_files", strconv.FormatInt(*c.MaxFiles, 10))
	}

	return "sharepoint://?" + params.Encode(), nil
}
