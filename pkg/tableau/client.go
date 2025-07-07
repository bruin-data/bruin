package tableau

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type Client struct {
	config     Config
	httpClient *http.Client
	authToken  string
	siteID     string
}

type RefreshResponse struct {
	Status string `json:"status"`
}

type TSCredentials struct {
	Name     string `json:"name"`
	Password string `json:"password"`
	Site     TSSite `json:"site"`
}

type TSSite struct {
	ContentURL string `json:"contentUrl"`
}

type TSResponse struct {
	Credentials TSCredentialsResponse `json:"credentials"`
}

type TSCredentialsResponse struct {
	Token string         `json:"token"`
	Site  TSSiteResponse `json:"site"`
	User  TSUser         `json:"user"`
}

type TSSiteResponse struct {
	ID         string `json:"id"`
	ContentURL string `json:"contentUrl"`
}

type TSUser struct {
	ID string `json:"id"`
}

func NewClient(c Config) (*Client, error) {
	if c.Host == "" {
		return nil, errors.New("host is required for Tableau connection")
	}
	if c.SiteID == "" {
		return nil, errors.New("site_id is required for Tableau connection")
	}

	hasPAT := c.PersonalAccessTokenName != "" && c.PersonalAccessTokenSecret != ""
	hasUsernamePassword := c.Username != "" && c.Password != ""

	if !hasPAT && !hasUsernamePassword {
		return nil, errors.New("either personal access token (name and secret) or username and password are required for Tableau connection")
	}

	if c.APIVersion == "" {
		c.APIVersion = "3.4"
	}

	return &Client{
		config: c,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

func (c *Client) authenticate(ctx context.Context) error {
	if c.authToken != "" {
		return nil
	}

	authURL := fmt.Sprintf("https://%s/api/%s/auth/signin", c.config.Host, c.config.APIVersion)

	var authPayload map[string]interface{}

	// Check if we have PAT credentials.
	if c.config.PersonalAccessTokenName != "" && c.config.PersonalAccessTokenSecret != "" {
		authPayload = map[string]interface{}{
			"credentials": map[string]interface{}{
				"personalAccessTokenName":   c.config.PersonalAccessTokenName,
				"personalAccessTokenSecret": c.config.PersonalAccessTokenSecret,
				"site": map[string]interface{}{
					"contentUrl": c.config.SiteID,
				},
			},
		}
	} else {
		// fallback to username/password.
		authPayload = map[string]interface{}{
			"credentials": map[string]interface{}{
				"name":     c.config.Username,
				"password": c.config.Password,
				"site": map[string]interface{}{
					"contentUrl": c.config.SiteID,
				},
			},
		}
	}

	payloadBytes, err := json.Marshal(authPayload)
	if err != nil {
		return errors.Wrap(err, "failed to marshal authentication payload")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, authURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return errors.Wrap(err, "failed to create authentication request")
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to perform authentication request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.Errorf("authentication failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tsResponse TSResponse
	if err := json.NewDecoder(resp.Body).Decode(&tsResponse); err != nil {
		return errors.Wrap(err, "failed to decode authentication response")
	}

	if tsResponse.Credentials.Token == "" {
		return errors.New("no authentication token received from Tableau")
	}

	c.authToken = tsResponse.Credentials.Token
	c.siteID = tsResponse.Credentials.Site.ID

	return nil
}

func (c *Client) RefreshDataSource(ctx context.Context, datasourceID string) error {
	if err := c.authenticate(ctx); err != nil {
		return errors.Wrap(err, "failed to authenticate with Tableau")
	}

	refreshURL := fmt.Sprintf("https://%s/api/%s/sites/%s/datasources/%s/refresh",
		c.config.Host, c.config.APIVersion, c.siteID, datasourceID)

	refreshPayload := map[string]interface{}{
		"datasource": map[string]interface{}{
			"id": datasourceID,
		},
	}

	payloadBytes, err := json.Marshal(refreshPayload)
	if err != nil {
		return errors.Wrap(err, "failed to marshal refresh payload")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, refreshURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return errors.Wrap(err, "failed to create refresh request")
	}

	req.Header.Set("X-Tableau-Auth", c.authToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to perform refresh request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.Errorf("refresh failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) RefreshWorksheet(ctx context.Context, workbookID string) error {
	if err := c.authenticate(ctx); err != nil {
		return errors.Wrap(err, "failed to authenticate with Tableau")
	}

	refreshURL := fmt.Sprintf("https://%s/api/%s/sites/%s/workbooks/%s/refresh",
		c.config.Host, c.config.APIVersion, c.siteID, workbookID)

	refreshPayload := map[string]interface{}{
		"workbook": map[string]interface{}{
			"id": workbookID,
		},
	}

	payloadBytes, err := json.Marshal(refreshPayload)
	if err != nil {
		return errors.Wrap(err, "failed to marshal refresh payload")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, refreshURL, bytes.NewBuffer(payloadBytes))
	if err != nil {
		return errors.Wrap(err, "failed to create refresh request")
	}

	req.Header.Set("X-Tableau-Auth", c.authToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to perform refresh request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.Errorf("refresh failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (c *Client) Ping(ctx context.Context) error {
	if err := c.authenticate(ctx); err != nil {
		return errors.Wrap(err, "failed to authenticate during ping")
	}

	pingURL := fmt.Sprintf("https://%s/api/%s/sites/%s/users",
		c.config.Host, c.config.APIVersion, c.siteID)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pingURL, nil)
	if err != nil {
		return errors.Wrap(err, "failed to create ping request")
	}

	req.Header.Set("X-Tableau-Auth", c.authToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrap(err, "failed to perform ping request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return errors.Errorf("ping failed with status %d", resp.StatusCode)
	}

	return nil
}
