package gcs

import (
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	ServiceAccountFile string
	ServiceAccountJSON string
	BucketName         string
	PathToFile         string
	Layout             string
}

func (c Config) GetIngestrURI() (string, error) {
	missingCredentials := c.ServiceAccountFile == "" && c.ServiceAccountJSON == ""
	if missingCredentials {
		return "", errors.New("GCS: either service_account_file or service_account_json must be provided")
	}

	params := url.Values{}
	switch {
	case c.ServiceAccountFile != "":
		params.Set("credentials_path", c.ServiceAccountFile)
	case c.ServiceAccountJSON != "":
		params.Set(
			"credentials_base64",
			base64.StdEncoding.EncodeToString([]byte(c.ServiceAccountJSON)),
		)
	}
	params.Set("layout", c.Layout)

	// When bucket and path are empty (e.g. GCS as source),
	// Go's url.URL.String() produces "gs:?params" (no "//"). Force "gs://?params".
	bucket := strings.TrimSpace(c.BucketName)
	pathToFile := strings.TrimSpace(c.PathToFile)
	if bucket == "" && pathToFile == "" {
		q := params.Encode()
		if q != "" {
			return "gs://?" + q, nil
		}
		return "gs://", nil
	}

	uri := url.URL{
		Scheme:   "gs",
		Host:     c.BucketName,
		Path:     c.PathToFile,
		RawQuery: params.Encode(),
	}
	return uri.String(), nil
}
