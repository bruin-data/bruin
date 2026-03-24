package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// SlackSender sends notifications to Slack using the Web API.
type SlackSender struct {
	APIKey  string
	Channel string
	client  *http.Client
}

func NewSlackSender(apiKey, channel string) *SlackSender {
	return &SlackSender{
		APIKey:  apiKey,
		Channel: channel,
		client:  &http.Client{},
	}
}

func (s *SlackSender) Type() string { return "slack" }

func (s *SlackSender) Send(ctx context.Context, payload Payload) error {
	blocks := s.buildBlocks(payload)

	body := map[string]any{
		"channel":  s.Channel,
		"username": "Bruin",
		"blocks":   blocks,
		"text":     s.summaryText(payload),
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal slack payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://slack.com/api/chat.postMessage", bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create slack request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Authorization", "Bearer "+s.APIKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send slack notification: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)

	var result struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return fmt.Errorf("failed to parse slack response: %w", err)
	}

	if !result.OK {
		return fmt.Errorf("slack API error: %s", result.Error)
	}

	return nil
}

func (s *SlackSender) summaryText(p Payload) string {
	if p.Status == "success" {
		return FormatSuccessMessage(p)
	}
	return FormatFailureMessage(p)
}

func (s *SlackSender) buildBlocks(p Payload) []map[string]any {
	var emoji, title string
	if p.Status == "success" {
		emoji = ":white_check_mark:"
		title = fmt.Sprintf("%s Pipeline has finished successfully.", emoji)
	} else {
		emoji = ":small_red_triangle:"
		title = fmt.Sprintf("%s Pipeline has failed.", emoji)
	}

	blocks := []map[string]any{
		{
			"type": "section",
			"text": map[string]string{
				"type": "mrkdwn",
				"text": title,
			},
		},
	}

	details := ""
	if p.Pipeline != "" {
		details += fmt.Sprintf("*Pipeline:* `%s`\n", p.Pipeline)
	}
	if p.Asset != "" {
		details += fmt.Sprintf("*Asset:* `%s`\n", p.Asset)
	}
	if p.Column != "" {
		details += fmt.Sprintf("*Column:* `%s`\n", p.Column)
	}
	if p.Check != "" {
		details += fmt.Sprintf("*Check:* `%s`\n", p.Check)
	}
	if p.RunID != "" {
		details += fmt.Sprintf("*Run ID:* `%s`\n", p.RunID)
	}
	if p.Message != "" {
		details += fmt.Sprintf("*Message:* %s\n", p.Message)
	}

	if details != "" {
		blocks = append(blocks, map[string]any{
			"type": "section",
			"text": map[string]string{
				"type": "mrkdwn",
				"text": details,
			},
		})
	}

	return blocks
}
