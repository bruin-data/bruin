package bamboohr

import (
	"errors"
	"net/url"
	"strings"
)

// Config holds the settings required to connect to BambooHR, an HR platform for
// employee records, time off, and time tracking.
type Config struct {
	// CompanyDomain is the part before .bamboohr.com in the company URL. For
	// https://acme.bamboohr.com, use "acme". Required.
	CompanyDomain string
	// APIKey is a BambooHR API key. Optional, but exactly one of APIKey or
	// AccessToken must be provided; the two are mutually exclusive.
	APIKey string
	// AccessToken is an OAuth bearer token. Optional, but exactly one of APIKey or
	// AccessToken must be provided; the two are mutually exclusive.
	AccessToken string
	// Timezone is the company's IANA timezone, such as America/Denver. Optional,
	// but required for the timesheet_entries table, whose date boundaries BambooHR
	// interprets in the company timezone.
	Timezone string
}

// GetIngestrURI builds the ingestr source URI for BambooHR.
// Format: bamboohr://<company-domain>?api_key=<api-key>[&timezone=<tz>]
//
//	bamboohr://<company-domain>?access_token=<token>[&timezone=<tz>]
func (c *Config) GetIngestrURI() (string, error) {
	companyDomain := strings.TrimSpace(c.CompanyDomain)
	if companyDomain == "" {
		return "", errors.New("bamboohr: company_domain must be provided")
	}

	apiKey := strings.TrimSpace(c.APIKey)
	accessToken := strings.TrimSpace(c.AccessToken)
	if apiKey == "" && accessToken == "" {
		return "", errors.New("bamboohr: one of api_key or access_token must be provided")
	}
	if apiKey != "" && accessToken != "" {
		return "", errors.New("bamboohr: api_key and access_token are mutually exclusive")
	}

	params := url.Values{}
	if apiKey != "" {
		params.Set("api_key", apiKey)
	}
	if accessToken != "" {
		params.Set("access_token", accessToken)
	}
	if timezone := strings.TrimSpace(c.Timezone); timezone != "" {
		params.Set("timezone", timezone)
	}

	uri := url.URL{
		Scheme:   "bamboohr",
		Host:     companyDomain,
		RawQuery: params.Encode(),
	}

	return uri.String(), nil
}
