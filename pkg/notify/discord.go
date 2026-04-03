package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// DiscordSender sends notifications to Discord via webhook.
type DiscordSender struct {
	WebhookURL string
	client     *http.Client
}

func NewDiscordSender(webhookURL string) *DiscordSender {
	return &DiscordSender{
		WebhookURL: webhookURL,
		client:     &http.Client{},
	}
}

func (d *DiscordSender) Type() string { return "discord" }

func (d *DiscordSender) Send(ctx context.Context, payload Payload) error {
	message := d.buildMessage(payload)

	body := map[string]any{
		"content":    message,
		"username":   "Bruin",
		"avatar_url": "https://avatars.githubusercontent.com/u/107880688",
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal discord payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.WebhookURL, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create discord request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send discord notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("discord webhook returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func (d *DiscordSender) buildMessage(p Payload) string {
	var emoji string
	if p.Status == "success" {
		emoji = ":white_check_mark:"
	} else {
		emoji = ":small_red_triangle:"
	}

	msg := fmt.Sprintf("**%s ", emoji)
	if p.Status == "success" {
		msg += "Pipeline has finished successfully.**\n"
	} else {
		msg += "Pipeline has failed.**\n"
	}

	if p.Pipeline != "" {
		msg += fmt.Sprintf("**Pipeline:** `%s`\n", p.Pipeline)
	}
	if p.Asset != "" {
		msg += fmt.Sprintf("**Asset:** `%s`\n", p.Asset)
	}
	if p.Column != "" {
		msg += fmt.Sprintf("**Column:** `%s`\n", p.Column)
	}
	if p.Check != "" {
		msg += fmt.Sprintf("**Check:** `%s`\n", p.Check)
	}
	if p.RunID != "" {
		msg += fmt.Sprintf("**Run ID:** `%s`\n", p.RunID)
	}
	if p.Message != "" {
		msg += fmt.Sprintf("**Message:** %s\n", p.Message)
	}

	return msg
}
