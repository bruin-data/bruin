package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// MSTeamsSender sends notifications to Microsoft Teams via Workflows webhook
// using the Adaptive Card format.
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
	card := m.buildAdaptiveCard(payload)

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

func (m *MSTeamsSender) buildAdaptiveCard(p Payload) map[string]any {
	var title, color string
	if p.Status == "success" {
		title = "\u2705 Pipeline has finished successfully"
		color = "good"
	} else {
		title = "\U0001F53A Pipeline has failed"
		color = "attention"
	}

	bodyItems := []map[string]any{
		{
			"type":   "TextBlock",
			"size":   "Medium",
			"weight": "Bolder",
			"text":   title,
			"color":  color,
		},
	}

	facts := m.buildFacts(p)
	if len(facts) > 0 {
		bodyItems = append(bodyItems, map[string]any{
			"type":  "FactSet",
			"facts": facts,
		})
	}

	return map[string]any{
		"type": "message",
		"attachments": []map[string]any{
			{
				"contentType": "application/vnd.microsoft.card.adaptive",
				"contentUrl":  nil,
				"content": map[string]any{
					"$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
					"type":    "AdaptiveCard",
					"version": "1.4",
					"body":    bodyItems,
				},
			},
		},
	}
}

func (m *MSTeamsSender) buildFacts(p Payload) []map[string]string {
	var facts []map[string]string
	if p.Pipeline != "" {
		facts = append(facts, map[string]string{"title": "Pipeline", "value": p.Pipeline})
	}
	if p.Asset != "" {
		facts = append(facts, map[string]string{"title": "Asset", "value": p.Asset})
	}
	if p.Column != "" {
		facts = append(facts, map[string]string{"title": "Column", "value": p.Column})
	}
	if p.Check != "" {
		facts = append(facts, map[string]string{"title": "Check", "value": p.Check})
	}
	if p.RunID != "" {
		facts = append(facts, map[string]string{"title": "Run ID", "value": p.RunID})
	}
	if p.Message != "" {
		facts = append(facts, map[string]string{"title": "Message", "value": p.Message})
	}
	return facts
}
