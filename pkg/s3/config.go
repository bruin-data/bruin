package s3

import (
	"net/url"
	"strings"
)

type Config struct {
	BucketName      string
	PathToFile      string
	AccessKeyID     string
	SecretAccessKey string
	EndpointURL     string
	Layout          string
}

// s3://<bucket_name>/<path_to_file>?access_key_id=<access_key_id>&secret_access_key=<secret_access_key>
func (c *Config) GetIngestrURI() string {
	params := url.Values{}
	params.Add("access_key_id", c.AccessKeyID)
	params.Add("secret_access_key", c.SecretAccessKey)

	endpointURL := strings.TrimSpace(c.EndpointURL)
	if endpointURL != "" {
		params.Add("endpoint_url", endpointURL)
	}

	layout := strings.TrimSpace(c.Layout)
	if layout != "" {
		params.Add("layout", layout)
	}

	// When bucket and path are empty (e.g. S3 as source),
	// Go's url.URL.String() produces "s3:?params" (no "//"). Force "s3://?params"
	bucket := strings.TrimSpace(c.BucketName)
	pathToFile := strings.TrimSpace(c.PathToFile)
	if bucket == "" && pathToFile == "" {
		q := params.Encode()
		if q != "" {
			return "s3://?" + q
		}
		return "s3://"
	}

	uri := url.URL{
		Scheme:   "s3",
		Host:     bucket,
		Path:     pathToFile,
		RawQuery: params.Encode(),
	}

	return uri.String()
}
