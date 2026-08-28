package sklik

import (
	"fmt"
	"net/url"
	"strings"
)

// Config holds the credentials required to connect to the Sklik API.
type Config struct {
	// Token is the permanent API token generated in the Sklik UI. Required.
	Token string
	// UserID optionally scopes the connection to a specific Sklik account id.
	UserID string
}

// GetIngestrURI builds the ingestr source URI for Sklik.
// Format: sklik://?token=<api_token>[&user_id=<user_id>]
func (c *Config) GetIngestrURI() (string, error) {
	token := strings.TrimSpace(c.Token)
	if token == "" {
		return "", fmt.Errorf("sklik: token must be provided")
	}

	params := url.Values{}
	params.Set("token", token)

	if userID := strings.TrimSpace(c.UserID); userID != "" {
		params.Set("user_id", userID)
	}

	return "sklik://?" + params.Encode(), nil
}
