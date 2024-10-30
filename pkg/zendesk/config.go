package zendesk

import "fmt"

type Config struct {
	ApiToken   string
	Email      string
	OAuthToken string
	Subdomain  string
}

func (c Config) GetIngestrURI() string {
	if c.ApiToken != "" && c.Email != "" && c.Subdomain != "" {
		return "zendesk://" + c.Email + ":" + c.ApiToken + "@" + c.Subdomain
	}

	if c.OAuthToken != "" && c.Subdomain != "" {
		fmt.Println("zendesk://:" + c.OAuthToken + "@" + c.Subdomain)
		return "zendesk://:" + c.OAuthToken + "@" + c.Subdomain
	}
	return ""
}
