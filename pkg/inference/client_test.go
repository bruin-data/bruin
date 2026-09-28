package inference

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func response(status int, body string, headers ...http.Header) *http.Response {
	h := make(http.Header)
	if len(headers) > 0 {
		h = headers[0]
	}
	return &http.Response{StatusCode: status, Header: h, Body: io.NopCloser(strings.NewReader(body))}
}

var testStructuredColumns = []outputColumn{{Name: "category", Type: "string", Choices: map[string]string{"ok": "Valid"}}}

func TestCompleteStructuredRetriesRetryableStatuses(t *testing.T) {
	var calls atomic.Int32
	c := Client{Provider: "opencode", Model: "model", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if calls.Add(1) < 3 {
			return response(http.StatusTooManyRequests, "sensitive response", http.Header{"Retry-After": {"0"}}), nil
		}
		return response(http.StatusOK, responseText(`{"category":"ok"}`)), nil
	})}}
	got, err := c.CompleteStructured(t.Context(), "state", "instructions", testStructuredColumns)
	if err != nil || got["category"] != "ok" || calls.Load() != 3 {
		t.Fatalf("got %#v, err %v, calls %d", got, err, calls.Load())
	}
}

func TestCompleteStructuredCancellationDuringBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := Client{Provider: "opencode", Model: "model", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		cancel()
		return response(http.StatusInternalServerError, "secret", http.Header{"Retry-After": {"10"}}), nil
	})}}
	started := time.Now()
	_, err := c.CompleteStructured(ctx, "state", "instructions", testStructuredColumns)
	if !errors.Is(err, context.Canceled) || time.Since(started) > time.Second {
		t.Fatalf("err = %v", err)
	}
}

func TestCompleteStructuredErrorsDoNotLeakSecrets(t *testing.T) {
	tests := []struct {
		name, provider, body string
		status               int
	}{
		{"malformed", "opencode", `{"DO_NOT_LEAK"`, http.StatusOK},
		{"provider error", "opencode", `{"error":{"message":"DO_NOT_LEAK"}}`, http.StatusOK},
		{"incomplete", "opencode", `{"status":"incomplete","secret":"DO_NOT_LEAK"}`, http.StatusOK},
		{"opencode refusal", "opencode", `{"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"refusal","text":"DO_NOT_LEAK"}]}]}`, http.StatusOK},
		{"openrouter truncation", "openrouter", `{"choices":[{"finish_reason":"length","message":{"content":"DO_NOT_LEAK"}}]}`, http.StatusOK},
		{"refusal", "openrouter", `{"choices":[{"finish_reason":"stop","message":{"refusal":"DO_NOT_LEAK"}}]}`, http.StatusOK},
		{"empty", "openrouter", `{"choices":[]}`, http.StatusOK},
		{"anthropic tool", "anthropic", `{"type":"message","role":"assistant","stop_reason":"end_turn","content":[{"type":"tool_use","name":"DO_NOT_LEAK"}]}`, http.StatusOK},
		{"anthropic refusal", "anthropic", `{"type":"message","role":"assistant","stop_reason":"end_turn","stop_details":{"type":"refusal","explanation":"DO_NOT_LEAK"},"content":[{"type":"text","text":"no"}]}`, http.StatusOK},
		{"anthropic truncation", "anthropic", `{"type":"message","stop_reason":"max_tokens","content":[{"type":"text","text":"DO_NOT_LEAK"}]}`, http.StatusOK},
		{"google blocked", "google", `{"promptFeedback":{"blockReason":"SAFETY","secret":"DO_NOT_LEAK"}}`, http.StatusOK},
		{"google blocked candidate", "google", `{"candidates":[{"finishReason":"STOP","safetyRatings":[{"blocked":true}],"content":{"parts":[{"text":"DO_NOT_LEAK"}]}}]}`, http.StatusOK},
		{"google truncation", "google", `{"candidates":[{"finishReason":"MAX_TOKENS","content":{"parts":[{"text":"DO_NOT_LEAK"}]}}]}`, http.StatusOK},
		{"google tool", "google", `{"candidates":[{"finishReason":"STOP","content":{"parts":[{"functionCall":{"name":"DO_NOT_LEAK"}}]}}]}`, http.StatusOK},
		{"http error", "openrouter", "DO_NOT_LEAK", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Client{Provider: tt.provider, Model: "model", APIKey: "key", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return response(tt.status, tt.body), nil
			})}}
			_, err := c.CompleteStructured(t.Context(), "SENSITIVE STATE", "instructions", testStructuredColumns)
			if err == nil || strings.Contains(err.Error(), "DO_NOT_LEAK") || strings.Contains(err.Error(), "SENSITIVE") {
				t.Fatalf("unsafe error: %v", err)
			}
		})
	}
}

func TestCompleteStructuredTransportErrorDoesNotLeakSecrets(t *testing.T) {
	c := Client{Provider: "openai", Model: "model", APIKey: "DO_NOT_LEAK_KEY", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("transport exposed DO_NOT_LEAK_KEY and SENSITIVE STATE")
	})}}
	_, err := c.CompleteStructured(t.Context(), "SENSITIVE STATE", "instructions", testStructuredColumns)
	if err == nil || strings.Contains(err.Error(), "DO_NOT_LEAK") || strings.Contains(err.Error(), "SENSITIVE") {
		t.Fatalf("unsafe error: %v", err)
	}
}
