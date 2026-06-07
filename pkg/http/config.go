package http

import (
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	URL string
}

func (c Config) GetIngestrURI() (string, error) {
	fileURL := strings.TrimSpace(c.URL)
	if fileURL == "" {
		return "", errors.New("HTTP: url must be provided")
	}

	parsedURL, err := url.Parse(fileURL)
	if err != nil {
		return "", err
	}

	scheme := strings.ToLower(parsedURL.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", errors.New("HTTP: url must use http or https scheme")
	}

	if parsedURL.Host == "" {
		return "", errors.New("HTTP: url must include a host")
	}

	return fileURL, nil
}
