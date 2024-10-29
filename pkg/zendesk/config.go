package zendesk

type Config struct {
	ApiToken   string
	Email      string
	OAuthToken string
	Subdomain  string
}

func (c Config) GetIngestrURI() string {
	//zendesk://<email>:<api_token>@<sub-domain>
	if c.ApiToken != "" && c.Email != "" && c.Subdomain != "" {
		return "zendesk://email=" + c.Email + ":api_token=" + c.ApiToken + "@sub-domain" + c.Subdomain
	}

	//zendesk://:<oauth_token>@<sub-domain>
	if c.OAuthToken != "" && c.Subdomain != "" {
		return "zendesk://:oauth_token" + c.OAuthToken + "@sub-domain" + c.Subdomain
	}

	return ""
}
