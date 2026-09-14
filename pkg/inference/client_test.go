package inference

import (
	"context"
	"encoding/json"
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

func TestCompleteWireShapesAndExtraction(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		client        Client
		wantURL       string
		wantAuth      string
		wantTokensKey string
		response      string
		want          string
	}{
		{"opencode", Client{Provider: "opencode", Model: "muse-spark-1.3-contributor-free"}, "https://opencode.ai/zen/v1/responses", "", "max_output_tokens", `{"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":" answer "}]}]}`, "answer"},
		{"openrouter", Client{Provider: "openrouter", Model: "some/model", APIKey: "test-key", MaxOutputTokens: 17}, "https://openrouter.ai/api/v1/chat/completions", "Bearer test-key", "max_tokens", `{"choices":[{"finish_reason":"stop","message":{"content":"answer","refusal":null}}]}`, "answer"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.client.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != tt.wantURL || req.Method != http.MethodPost {
					t.Fatalf("unexpected request: %s %s", req.Method, req.URL)
				}
				if got := req.Header.Get("Authorization"); got != tt.wantAuth {
					t.Fatalf("authorization = %q, want %q", got, tt.wantAuth)
				}
				var body map[string]any
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if body["model"] != tt.client.Model || body[tt.wantTokensKey] != float64(map[bool]int{true: 17, false: 1024}[tt.client.MaxOutputTokens != 0]) {
					t.Fatalf("unexpected body: %#v", body)
				}
				if tt.name == "opencode" {
					if body["input"] != "secret prompt" || body["store"] != false {
						t.Fatalf("unexpected body: %#v", body)
					}
				} else {
					messages := body["messages"].([]any)
					if messages[0].(map[string]any)["content"] != "secret prompt" {
						t.Fatalf("unexpected messages: %#v", messages)
					}
				}
				return response(http.StatusOK, tt.response), nil
			})}
			got, err := tt.client.Complete(context.Background(), "secret prompt")
			if err != nil || got != tt.want {
				t.Fatalf("Complete() = %q, %v", got, err)
			}
		})
	}
}

func TestCompleteDirectProviderWireContracts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, provider, model, wantURL, response string
		check                                    func(*testing.T, *http.Request, map[string]any)
	}{
		{
			name: "openai", provider: "openai", model: "gpt-5-mini", wantURL: "https://api.openai.com/v1/responses",
			response: `{"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"openai answer"}]}]}`,
			check: func(t *testing.T, req *http.Request, body map[string]any) {
				if req.Header.Get("Authorization") != "Bearer direct-secret" || body["input"] != "prompt" || body["max_output_tokens"] != float64(23) || body["store"] != false {
					t.Fatalf("unexpected OpenAI request: headers=%v body=%#v", req.Header, body)
				}
			},
		},
		{
			name: "anthropic", provider: "anthropic", model: "claude-sonnet-4-5", wantURL: "https://api.anthropic.com/v1/messages",
			response: `{"type":"message","role":"assistant","stop_reason":"end_turn","content":[{"type":"text","text":"anthropic answer"}]}`,
			check: func(t *testing.T, req *http.Request, body map[string]any) {
				messages := body["messages"].([]any)
				if req.Header.Get("X-Api-Key") != "direct-secret" || req.Header.Get("Anthropic-Version") != "2023-06-01" || req.Header.Get("Authorization") != "" || body["max_tokens"] != float64(23) || messages[0].(map[string]any)["content"] != "prompt" {
					t.Fatalf("unexpected Anthropic request: headers=%v body=%#v", req.Header, body)
				}
			},
		},
		{
			name: "google", provider: "google", model: "publishers/acme models/gemini?preview", wantURL: "https://generativelanguage.googleapis.com/v1beta/models/publishers%2Facme%20models%2Fgemini%3Fpreview:generateContent",
			response: `{"candidates":[{"finishReason":"STOP","content":{"parts":[{"thought":true,"text":"private"},{"text":"google answer"}]}}]}`,
			check: func(t *testing.T, req *http.Request, body map[string]any) {
				contents := body["contents"].([]any)
				parts := contents[0].(map[string]any)["parts"].([]any)
				config := body["generationConfig"].(map[string]any)
				if req.Header.Get("X-Goog-Api-Key") != "direct-secret" || req.Header.Get("Authorization") != "" || config["maxOutputTokens"] != float64(23) || parts[0].(map[string]any)["text"] != "prompt" {
					t.Fatalf("unexpected Google request: headers=%v body=%#v", req.Header, body)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := Client{Provider: tt.provider, Model: tt.model, APIKey: "direct-secret", MaxOutputTokens: 23}
			client.HTTPClient = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != tt.wantURL {
					t.Fatalf("URL = %q, want %q", req.URL.String(), tt.wantURL)
				}
				var body map[string]any
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if tt.provider != "google" && body["model"] != tt.model {
					t.Fatalf("model = %#v, want %q", body["model"], tt.model)
				}
				tt.check(t, req, body)
				return response(http.StatusOK, tt.response), nil
			})}
			got, err := client.Complete(context.Background(), "prompt")
			if err != nil || got != tt.name+" answer" {
				t.Fatalf("Complete() = %q, %v", got, err)
			}
		})
	}
}

func TestCompleteDirectProvidersRequireKeys(t *testing.T) {
	t.Parallel()
	for _, provider := range []string{"openai", "anthropic", "google"} {
		t.Run(provider, func(t *testing.T) {
			t.Parallel()
			_, err := (&Client{Provider: provider, Model: "model"}).Complete(context.Background(), "prompt")
			if err == nil || !strings.Contains(err.Error(), "key is required") {
				t.Fatalf("error = %v", err)
			}
		})
	}
}

func TestCompleteRetriesRetryableStatuses(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	c := Client{Provider: "opencode", Model: "model", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		if calls.Add(1) < 3 {
			return response(http.StatusTooManyRequests, "sensitive response", http.Header{"Retry-After": {"0"}}), nil
		}
		return response(http.StatusOK, `{"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"ok"}]}]}`), nil
	})}}
	got, err := c.Complete(context.Background(), "prompt")
	if err != nil || got != "ok" || calls.Load() != 3 {
		t.Fatalf("got %q, err %v, calls %d", got, err, calls.Load())
	}
}

func TestCompleteCancellationDuringBackoff(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	c := Client{Provider: "opencode", Model: "model", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		cancel()
		return response(http.StatusInternalServerError, "secret", http.Header{"Retry-After": {"10"}}), nil
	})}}
	started := time.Now()
	_, err := c.Complete(ctx, "prompt")
	if !errors.Is(err, context.Canceled) || time.Since(started) > time.Second {
		t.Fatalf("err = %v", err)
	}
}

func TestCompleteRejectsBadResponsesWithoutLeakingBody(t *testing.T) {
	t.Parallel()
	tests := []struct{ name, provider, body string }{
		{"opencode incomplete", "opencode", `{"status":"incomplete","secret":"DO_NOT_LEAK"}`},
		{"opencode refusal", "opencode", `{"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"refusal","text":"DO_NOT_LEAK"}]}]}`},
		{"openrouter truncation", "openrouter", `{"choices":[{"finish_reason":"length","message":{"content":"DO_NOT_LEAK"}}]}`},
		{"openrouter refusal", "openrouter", `{"choices":[{"finish_reason":"stop","message":{"content":"","refusal":"DO_NOT_LEAK"}}]}`},
		{"malformed", "opencode", `{"DO_NOT_LEAK"`},
		{"error object", "opencode", `{"error":{"message":"DO_NOT_LEAK"}}`},
		{"empty", "openrouter", `{"choices":[]}`},
		{"anthropic tool", "anthropic", `{"type":"message","role":"assistant","stop_reason":"end_turn","content":[{"type":"tool_use","name":"leak","input":{"secret":"DO_NOT_LEAK"}}]}`},
		{"anthropic truncation", "anthropic", `{"type":"message","role":"assistant","stop_reason":"max_tokens","content":[{"type":"text","text":"DO_NOT_LEAK"}]}`},
		{"anthropic refusal", "anthropic", `{"type":"message","role":"assistant","stop_reason":"end_turn","stop_details":{"type":"refusal","explanation":"DO_NOT_LEAK"},"content":[{"type":"text","text":"no"}]}`},
		{"anthropic error", "anthropic", `{"type":"error","error":{"message":"DO_NOT_LEAK"}}`},
		{"google blocked prompt", "google", `{"promptFeedback":{"blockReason":"SAFETY","secret":"DO_NOT_LEAK"}}`},
		{"google blocked candidate", "google", `{"candidates":[{"finishReason":"STOP","safetyRatings":[{"blocked":true}],"content":{"parts":[{"text":"DO_NOT_LEAK"}]}}]}`},
		{"google truncation", "google", `{"candidates":[{"finishReason":"MAX_TOKENS","content":{"parts":[{"text":"DO_NOT_LEAK"}]}}]}`},
		{"google tool", "google", `{"candidates":[{"finishReason":"STOP","content":{"parts":[{"functionCall":{"name":"DO_NOT_LEAK"}}]}}]}`},
		{"google error", "google", `{"error":{"message":"DO_NOT_LEAK"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := Client{Provider: tt.provider, Model: "model", APIKey: "key", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return response(http.StatusOK, tt.body), nil })}}
			_, err := c.Complete(context.Background(), "prompt")
			if err == nil || strings.Contains(err.Error(), "DO_NOT_LEAK") {
				t.Fatalf("unsafe error: %v", err)
			}
		})
	}
}

func TestProviderDefaultAPIKeyEnv(t *testing.T) {
	t.Parallel()
	wants := map[string]string{
		"openai": "OPENAI_API_KEY", "anthropic": "ANTHROPIC_API_KEY", "google": "GOOGLE_API_KEY",
	}
	for provider, want := range wants {
		got, ok := ProviderDefaultAPIKeyEnv(provider)
		if !ok || got != want {
			t.Errorf("ProviderDefaultAPIKeyEnv(%q) = %q, %v", provider, got, ok)
		}
	}
	if _, ok := ProviderDefaultAPIKeyEnv("unknown"); ok {
		t.Error("unknown provider unexpectedly has a default")
	}
}

func TestCompleteHTTPErrorDoesNotLeakResponse(t *testing.T) {
	t.Parallel()
	c := Client{Provider: "openrouter", Model: "model", APIKey: "key", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return response(http.StatusBadRequest, "DO_NOT_LEAK"), nil
	})}}
	_, err := c.Complete(context.Background(), "prompt")
	if err == nil || strings.Contains(err.Error(), "DO_NOT_LEAK") {
		t.Fatalf("unsafe error: %v", err)
	}
}

func TestCompleteTransportErrorDoesNotLeakSecrets(t *testing.T) {
	t.Parallel()
	c := Client{Provider: "openai", Model: "model", APIKey: "DO_NOT_LEAK_KEY", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("transport exposed DO_NOT_LEAK_KEY and prompt")
	})}}
	_, err := c.Complete(context.Background(), "prompt")
	if err == nil || strings.Contains(err.Error(), "DO_NOT_LEAK") || strings.Contains(err.Error(), "prompt") {
		t.Fatalf("unsafe error: %v", err)
	}
}
