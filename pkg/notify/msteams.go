package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// MSTeamsSender sends notifications to Microsoft Teams via webhook.
type MSTeamsSender struct {
	WebhookURL string
	client     *http.Client
}

func NewMSTeamsSender(webhookURL string) *MSTeamsSender {
	return &MSTeamsSender{
		WebhookURL: webhookURL,
		client:     &http.Client{},
	}
}

func (m *MSTeamsSender) Type() string { return "ms_teams" }

func (m *MSTeamsSender) Send(ctx context.Context, payload Payload) error {
	card := m.buildMessageCard(payload)

	jsonBody, err := json.Marshal(card)
	if err != nil {
		return fmt.Errorf("failed to marshal ms teams payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.WebhookURL, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create ms teams request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := m.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send ms teams notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ms teams webhook returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (m *MSTeamsSender) buildMessageCard(p Payload) map[string]any {
	var title string
	if p.Status == "success" {
		title = "&#x2705; Pipeline has finished successfully"
	} else {
		title = "&#x1F53A; Pipeline has failed"
	}

	facts := []map[string]string{}
	if p.Pipeline != "" {
		facts = append(facts, map[string]string{"name": "Pipeline", "value": fmt.Sprintf("`%s`", p.Pipeline)})
	}
	if p.Asset != "" {
		facts = append(facts, map[string]string{"name": "Asset", "value": fmt.Sprintf("`%s`", p.Asset)})
	}
	if p.Column != "" {
		facts = append(facts, map[string]string{"name": "Column", "value": fmt.Sprintf("`%s`", p.Column)})
	}
	if p.Check != "" {
		facts = append(facts, map[string]string{"name": "Check", "value": fmt.Sprintf("`%s`", p.Check)})
	}
	if p.RunID != "" {
		facts = append(facts, map[string]string{"name": "Run ID", "value": fmt.Sprintf("`%s`", p.RunID)})
	}
	if p.Message != "" {
		facts = append(facts, map[string]string{"name": "Message", "value": p.Message})
	}

	summary := title
	if p.Pipeline != "" {
		summary = fmt.Sprintf("Pipeline: `%s`", p.Pipeline)
	}

	return map[string]any{
		"@type":    "MessageCard",
		"@context": "http://schema.org/extensions",
		"summary":  summary,
		"sections": []map[string]any{
			{
				"activityTitle":    title,
				"activitySubtitle": "",
				"facts":            facts,
				"text":             "",
			},
		},
	}
}
