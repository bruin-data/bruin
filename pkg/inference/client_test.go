package inference

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
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
	t.Parallel()
	for _, tc := range []struct {
		name      string
		status    int
		succeed   bool
		wantCalls int32
	}{
		{"last attempt succeeds", 429, true, 10},
		{"rate limit exhausted", 429, false, 10},
		{"overload exhausted", 529, false, 10},
		{"authentication fails immediately", 401, false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			c := Client{Provider: "opencode", Model: "model", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				if calls.Add(1) == 10 && tc.succeed {
					return response(http.StatusOK, responseText(`{"category":"ok"}`)), nil
				}
				return response(tc.status, "sensitive response", http.Header{"Retry-After": {"0"}}), nil
			})}}
			got, err := c.CompleteStructured(t.Context(), "state", "instructions", testStructuredColumns)
			if tc.succeed {
				require.NoError(t, err)
				require.Equal(t, "ok", got["category"])
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "sensitive")
			}
			require.Equal(t, tc.wantCalls, calls.Load())
		})
	}
}

func TestRetryDelayHeaders(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		headers map[string]string
		want    time.Duration
	}{
		{"uncapped seconds", map[string]string{"Retry-After": "150"}, 150 * time.Second},
		{"fractional seconds", map[string]string{"Retry-After": "1.25"}, 1250 * time.Millisecond},
		{"HTTP date", map[string]string{"Retry-After": now.Add(3 * time.Minute).Format(http.TimeFormat)}, 3 * time.Minute},
		{"past HTTP date", map[string]string{"Retry-After": now.Add(-time.Minute).Format(http.TimeFormat)}, 0},
		{"milliseconds", map[string]string{"Retry-After": "invalid", "Retry-After-Ms": "1250"}, 1250 * time.Millisecond},
		{"retry after takes precedence", map[string]string{"Retry-After": "5", "X-Ratelimit-Remaining-Tokens": "0", "X-Ratelimit-Reset-Tokens": "2m"}, 5 * time.Second},
		{"latest exhausted OpenAI quota", map[string]string{
			"X-Ratelimit-Remaining-Requests": "0", "X-Ratelimit-Reset-Requests": "7s",
			"X-Ratelimit-Remaining-Tokens": "0", "X-Ratelimit-Reset-Tokens": "1m3s",
			"X-Ratelimit-Remaining-Project-Tokens": "100", "X-Ratelimit-Reset-Project-Tokens": "9m",
		}, 63 * time.Second},
		{"Anthropic reset", map[string]string{"Anthropic-Ratelimit-Input-Tokens-Remaining": "0", "Anthropic-Ratelimit-Input-Tokens-Reset": now.Add(45 * time.Second).Format(time.RFC3339)}, 45 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			headers := make(http.Header)
			for key, value := range tc.headers {
				headers.Set(key, value)
			}
			require.Equal(t, tc.want, retryDelay(headers, 0, now))
		})
	}
	for _, value := range []string{"", "invalid", "-1", "999999999999999999999999999999"} {
		for _, bounds := range []struct {
			attempt      int
			lower, upper time.Duration
		}{{0, 500 * time.Millisecond, time.Second}, {3, 4 * time.Second, 8 * time.Second}, {8, 30 * time.Second, time.Minute}} {
			delay := retryDelay(http.Header{"Retry-After": {value}}, bounds.attempt, now)
			require.GreaterOrEqual(t, delay, bounds.lower)
			require.Less(t, delay, bounds.upper)
		}
	}
}

func TestProviderRetryWaitOutlivesAttemptTimeout(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		started := time.Now()
		calls := 0
		c := Client{HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			calls++
			require.NoError(t, req.Context().Err())
			if calls == 1 {
				return response(429, "", http.Header{"Retry-After": {"150"}}), nil
			}
			require.Equal(t, 150*time.Second, time.Since(started))
			return response(200, "ok"), nil
		})}}
		data, err := c.do(t.Context(), "https://example.com", nil)
		require.NoError(t, err)
		require.Equal(t, "ok", string(data))
		require.Equal(t, 2, calls)
	})
}

func TestRetryWaitHonorsCallerDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		started := time.Now()
		calls := 0
		c := Client{HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			calls++
			return response(429, "", http.Header{"Retry-After": {"150"}}), nil
		})}}
		_, err := c.do(ctx, "https://example.com", nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.Equal(t, 30*time.Second, time.Since(started))
		require.Equal(t, 1, calls)
	})
}

func TestCompleteStructuredCancellationDuringBackoff(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
		{"google blocked", providerGoogle, `{"promptFeedback":{"blockReason":"SAFETY","secret":"DO_NOT_LEAK"}}`, http.StatusOK},
		{"google blocked candidate", providerGoogle, `{"candidates":[{"finishReason":"STOP","safetyRatings":[{"blocked":true}],"content":{"parts":[{"text":"DO_NOT_LEAK"}]}}]}`, http.StatusOK},
		{"google truncation", providerGoogle, `{"candidates":[{"finishReason":"MAX_TOKENS","content":{"parts":[{"text":"DO_NOT_LEAK"}]}}]}`, http.StatusOK},
		{"google tool", providerGoogle, `{"candidates":[{"finishReason":"STOP","content":{"parts":[{"functionCall":{"name":"DO_NOT_LEAK"}}]}}]}`, http.StatusOK},
		{"http error", "openrouter", "DO_NOT_LEAK", http.StatusBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
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
	t.Parallel()
	c := Client{Provider: "openai", Model: "model", APIKey: "DO_NOT_LEAK_KEY", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("transport exposed DO_NOT_LEAK_KEY and SENSITIVE STATE")
	})}}
	_, err := c.CompleteStructured(t.Context(), "SENSITIVE STATE", "instructions", testStructuredColumns)
	if err == nil || strings.Contains(err.Error(), "DO_NOT_LEAK") || strings.Contains(err.Error(), "SENSITIVE") {
		t.Fatalf("unsafe error: %v", err)
	}
}
