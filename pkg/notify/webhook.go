package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// WebhookSender sends notifications to a generic webhook endpoint.
type WebhookSender struct {
	URL      string
	Login    string
	Password string
	client   *http.Client
}

func NewWebhookSender(url, login, password string) *WebhookSender {
	return &WebhookSender{
		URL:      url,
		Login:    login,
		Password: password,
		client:   &http.Client{},
	}
}

func (w *WebhookSender) Type() string { return "webhook" }

func (w *WebhookSender) Send(ctx context.Context, payload Payload) error {
	body := map[string]any{
		"pipeline": payload.Pipeline,
		"asset":    nilIfEmpty(payload.Asset),
		"column":   nilIfEmpty(payload.Column),
		"check":    nilIfEmpty(payload.Check),
		"run_id":   nilIfEmpty(payload.RunID),
		"status":   payload.Status,
		"message":  nilIfEmpty(payload.Message),
	}

	jsonBody, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal webhook payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create webhook request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	if w.Login != "" && w.Password != "" {
		req.SetBasicAuth(w.Login, w.Password)
	}

	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send webhook notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("webhook returned status %d: %s", resp.StatusCode, string(respBody))
	}

	return nil
}

func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
