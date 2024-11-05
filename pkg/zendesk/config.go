package zendesk

type Config struct {
	APIToken   string
	Email      string
	OAuthToken string
	Subdomain  string
}

func (c Config) GetIngestrURI() string {
	if c.APIToken != "" || c.Email != "" {
		return "zendesk://" + c.Email + ":" + c.APIToken + "@" + c.Subdomain
	}
	return "zendesk://:" + c.OAuthToken + "@" + c.Subdomain
}
