package privatus

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

// siteIDPattern mirrors the site ID format ingestr accepts.
var siteIDPattern = regexp.MustCompile(`^pa_[A-Za-z0-9]+$`)

type Config struct {
	APIKey  string
	SiteIDs []string
}

// GetIngestrURI builds a privatus://?api_key=... URI. Empty optional fields are left out so that
// ingestr applies its own default (every site in the workspace).
func (c *Config) GetIngestrURI() (string, error) {
	// The key goes into the URI exactly as configured: credential masking registers the configured
	// value, so a trimmed copy would leak in debug output.
	if strings.TrimSpace(c.APIKey) == "" {
		return "", errors.New("privatus: api_key must be provided")
	}

	siteIDs := make([]string, 0, len(c.SiteIDs))
	for _, id := range c.SiteIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if !siteIDPattern.MatchString(id) {
			return "", fmt.Errorf("privatus: invalid site id %q in site_ids, expected a value like pa_AB12CD34EF", id)
		}
		siteIDs = append(siteIDs, id)
	}

	params := url.Values{}
	params.Set("api_key", c.APIKey)
	if len(siteIDs) > 0 {
		params.Set("site_ids", strings.Join(siteIDs, ","))
	}

	// url.URL.String drops the "//" when there is no host, and ingestr requires the privatus:// prefix.
	return "privatus://?" + params.Encode(), nil
}
