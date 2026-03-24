package notify

import (
	"context"
	"fmt"
)

// Sender is the interface implemented by all notification senders.
type Sender interface {
	Send(ctx context.Context, payload Payload) error
	Type() string
}

// Payload represents the data to include in a notification.
type Payload struct {
	Pipeline string
	Asset    string
	Column   string
	Check    string
	RunID    string
	Status   string // "success" or "failure"
	Message  string // optional custom message
}

// FormatSuccessMessage returns a standard success message for the given payload.
func FormatSuccessMessage(p Payload) string {
	if p.Message != "" {
		return p.Message
	}
	if p.Pipeline != "" {
		return fmt.Sprintf("Pipeline `%s` has finished successfully.", p.Pipeline)
	}
	return "Pipeline has finished successfully."
}

// FormatFailureMessage returns a standard failure message for the given payload.
func FormatFailureMessage(p Payload) string {
	if p.Message != "" {
		return p.Message
	}
	parts := ""
	if p.Asset != "" {
		parts += fmt.Sprintf("\nAsset: `%s`", p.Asset)
	}
	if p.Column != "" {
		parts += fmt.Sprintf("\nColumn: `%s`", p.Column)
	}
	if p.Check != "" {
		parts += fmt.Sprintf("\nCheck: `%s`", p.Check)
	}
	if p.Pipeline != "" {
		return fmt.Sprintf("Pipeline `%s` has failed.%s", p.Pipeline, parts)
	}
	return fmt.Sprintf("Pipeline has failed.%s", parts)
}
