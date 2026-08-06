package gcs

import (
	"encoding/base64"
	"errors"
	"net/url"
	"strings"
)

type Config struct {
	ServiceAccountFile               string
	ServiceAccountJSON               string
	UseApplicationDefaultCredentials bool
	BucketName                       string
	PathToFile                       string
	Layout                           string
}

func (c Config) GetIngestrURI() (string, error) {
	missingCredentials := c.ServiceAccountFile == "" && c.ServiceAccountJSON == ""
	if missingCredentials && !c.UseApplicationDefaultCredentials {
		return "", errors.New("GCS: provide service_account_file, service_account_json, or enable use_application_default_credentials")
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
		Host:     bucket,
		Path:     pathToFile,
		RawQuery: params.Encode(),
	}
	return uri.String(), nil
}
