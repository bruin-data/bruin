package tableau

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/executor"
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

type DataSourceInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type listDatasourcesResponse struct {
	Datasources struct {
		Datasource []DataSourceInfo `json:"datasource"`
	} `json:"datasources"`
}

type WorkbookInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type listWorkbooksResponse struct {
	Workbooks struct {
		Workbook []WorkbookInfo `json:"workbook"`
	} `json:"workbooks"`
}

type TableauResourceType string

const (
	ResourceDatasources TableauResourceType = "datasources"
	ResourceWorkbooks   TableauResourceType = "workbooks"
)

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
	return c.refreshResource(ctx, "datasources", datasourceID, "datasource")
}

func (c *Client) RefreshWorksheet(ctx context.Context, workbookID string) error {
	return c.refreshResource(ctx, "workbooks", workbookID, "workbook")
}

func (c *Client) pollJobStatus(ctx context.Context, jobID string) error {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	statusURL := fmt.Sprintf("https://%s/api/%s/sites/%s/jobs/%s", c.config.Host, c.config.APIVersion, c.siteID, jobID)
	var writer io.Writer = os.Stdout
	if w := ctx.Value(executor.KeyPrinter); w != nil {
		if wr, ok := w.(io.Writer); ok {
			writer = wr
		}
	}
	if _, err := writer.Write([]byte(fmt.Sprintf("Refresh started asynchronously, waiting for job to complete, job ID: %s\n", jobID))); err != nil {
		return errors.Wrap(err, "failed to write log output")
	}
	for {
		select {
		case <-ctx.Done():
			return errors.New("timed out waiting for Tableau job to complete")
		default:
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
			if err != nil {
				return errors.Wrap(err, "failed to create job status request")
			}
			req.Header.Set("X-Tableau-Auth", c.authToken)
			req.Header.Set("Accept", "application/json")

			resp, err := c.httpClient.Do(req)
			if err != nil {
				return errors.Wrap(err, "failed to perform job status request")
			}
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			var jobResp struct {
				Job struct {
					ID                string `json:"id"`
					Type              string `json:"type"`
					CreatedAt         string `json:"createdAt"`
					StartedAt         string `json:"startedAt"`
					CompletedAt       string `json:"completedAt"`
					FinishCode        string `json:"finishCode"`
					ExtractRefreshJob struct {
						Notes string `json:"notes"`
					} `json:"extractRefreshJob"`
				} `json:"job"`
			}
			if err := json.Unmarshal(body, &jobResp); err != nil {
				return errors.Wrap(err, "failed to decode job status response")
			}

			if jobResp.Job.CompletedAt != "" {
				if jobResp.Job.FinishCode == "0" {
					return nil
				} else {
					errNotes := jobResp.Job.ExtractRefreshJob.Notes
					return errors.Errorf("Tableau job failed: finishCode=%s, notes=%s", jobResp.Job.FinishCode, errNotes)
				}
			}

			time.Sleep(5 * time.Second)
		}
	}
}

func (c *Client) refreshResource(ctx context.Context, resourceType, resourceID, payloadKey string) error {
	if err := c.authenticate(ctx); err != nil {
		return errors.Wrap(err, "failed to authenticate with Tableau")
	}

	refreshURL := fmt.Sprintf("https://%s/api/%s/sites/%s/%s/%s/refresh",
		c.config.Host, c.config.APIVersion, c.siteID, resourceType, resourceID)

	refreshPayload := map[string]interface{}{
		payloadKey: map[string]interface{}{
			"id": resourceID,
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

	if resp.StatusCode == http.StatusAccepted { // 202
		// Parse job ID from response
		var jobResp struct {
			Job struct {
				ID string `json:"id"`
			} `json:"job"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&jobResp); err != nil {
			return errors.Wrap(err, "failed to decode Tableau job response")
		}
		if jobResp.Job.ID == "" {
			return errors.New("missing job ID in Tableau response")
		}
		return c.pollJobStatus(ctx, jobResp.Job.ID)
	}

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

func (c *Client) getTableauResource(ctx context.Context, resource TableauResourceType, out interface{}) error {
	if err := c.authenticate(ctx); err != nil {
		return errors.Wrapf(err, "failed to authenticate during list %s", resource)
	}

	url := fmt.Sprintf("https://%s/api/%s/sites/%s/%s", c.config.Host, c.config.APIVersion, c.siteID, resource)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return errors.Wrapf(err, "failed to create list %s request", resource)
	}

	req.Header.Set("X-Tableau-Auth", c.authToken)
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "failed to perform list %s request", resource)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return errors.Errorf("list %s failed with status %d: %s", resource, resp.StatusCode, string(body))
	}

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return errors.Wrapf(err, "failed to decode list %s response", resource)
	}
	return nil
}

func (c *Client) ListDatasources(ctx context.Context) ([]DataSourceInfo, error) {
	var dsResp listDatasourcesResponse
	if err := c.getTableauResource(ctx, ResourceDatasources, &dsResp); err != nil {
		return nil, err
	}
	return dsResp.Datasources.Datasource, nil
}

func (c *Client) GetDatasource(ctx context.Context) ([]DataSourceInfo, error) {
	datasources, err := c.ListDatasources(ctx)
	if err != nil {
		return nil, err
	}
	return datasources, nil
}

func FindDatasourceIDByName(ctx context.Context, name string, datasources []DataSourceInfo) (string, error) {
	if datasources == nil {
		return "", errors.New("no datasources provided")
	}

	for _, ds := range datasources {
		if strings.EqualFold(ds.Name, name) {
			return ds.ID, nil
		}
	}
	return "", nil
}

func (c *Client) ListWorkbooks(ctx context.Context) ([]WorkbookInfo, error) {
	var wbResp listWorkbooksResponse
	if err := c.getTableauResource(ctx, ResourceWorkbooks, &wbResp); err != nil {
		return nil, err
	}
	return wbResp.Workbooks.Workbook, nil
}

func (c *Client) GetWorkbooks(ctx context.Context) ([]WorkbookInfo, error) {
	workbooks, err := c.ListWorkbooks(ctx)
	if err != nil {
		return nil, err
	}
	return workbooks, nil
}

func FindWorkbookIDByName(ctx context.Context, name string, workbooks []WorkbookInfo) (string, error) {
	if workbooks == nil {
		return "", errors.New("no workbooks provided")
	}
	for _, wb := range workbooks {
		if strings.EqualFold(strings.TrimSpace(wb.Name), strings.TrimSpace(name)) {
			return wb.ID, nil
		}
	}
	return "", nil
}
