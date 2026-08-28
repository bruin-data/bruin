package sklik

import (
	"errors"
	"net/url"
	"strconv"
	"strings"
)

// Config holds the credentials required to connect to the Sklik API.
type Config struct {
	// Token is the permanent API token generated in the Sklik UI. Required.
	Token string
	// UserID optionally scopes the connection to a specific numeric Sklik account id.
	UserID *int64
}

// GetIngestrURI builds the ingestr source URI for Sklik.
// Format: sklik://?token=<api_token>[&user_id=<user_id>]
func (c *Config) GetIngestrURI() (string, error) {
	token := strings.TrimSpace(c.Token)
	if token == "" {
		return "", errors.New("sklik: token must be provided")
	}

	params := url.Values{}
	params.Set("token", token)

	if c.UserID != nil {
		params.Set("user_id", strconv.FormatInt(*c.UserID, 10))
	}

	return "sklik://?" + params.Encode(), nil
}
