package zendesk

type Config struct {
	ApiToken   string
	Email      string
	OAuthToken string
	Subdomain  string
}

func (c Config) GetIngestrURI() string {
	if c.ApiToken != "" || c.Email != "" {
		return "zendesk://" + c.Email + ":" + c.ApiToken + "@" + c.Subdomain
	}
	return "zendesk://:" + c.OAuthToken + "@" + c.Subdomain
}
