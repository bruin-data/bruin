package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPromptCacheBoundaries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ provider, model, marker string }{
		{"openai", "gpt-6-luna", "prompt_cache_breakpoint"},
		{"openai", "gpt-5.6-2026-01-01", "prompt_cache_breakpoint"},
		{"openai", "gpt-5.5", ""},
		{"openai", "custom-model", ""},
		{"opencode", "muse-spark-1.3", ""},
		{"anthropic", "claude-haiku-4-5", "cache_control"},
		{"google", "gemini-3.8-flash", ""},
		{"openrouter", "anthropic/claude-haiku-4.5", "cache_control"},
		{"openrouter", "google/gemini-3.8-flash", "cache_control"},
		{"openrouter", "qwen/qwen3", "cache_control"},
		{"openrouter", "openai/gpt-6-luna", "prompt_cache_breakpoint"},
		{"openrouter", "openai/gpt-4.1", ""},
	} {
		t.Run(tc.provider+"/"+tc.model, func(t *testing.T) {
			t.Parallel()
			var bodies []map[string]any
			c := Client{Provider: tc.provider, Model: tc.model, HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				var body map[string]any
				require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
				bodies = append(bodies, body)
				// These assertions exercise outbound requests, not response parsing.
				return response(http.StatusUnauthorized, ""), nil
			})}}
			columns := []outputColumn{{Name: "category", Type: "string", Prompt: "Classify the row."}}
			for _, row := range []string{"row one", "row two"} {
				_, err := c.CompleteStructured(t.Context(), row, "Shared rubric", columns)
				require.ErrorContains(t, err, "401")
				body := bodies[len(bodies)-1]
				var block map[string]any
				switch tc.provider {
				case "openai", "opencode", "openrouter":
					field, role := "input", "developer"
					if tc.provider == "openrouter" {
						field, role = "messages", "system"
					}
					messages := body[field].([]any)
					require.Len(t, messages, 2)
					prefix := messages[0].(map[string]any)
					require.Equal(t, role, prefix["role"])
					block = prefix["content"].([]any)[0].(map[string]any)
					require.Equal(t, map[string]any{"role": "user", "content": row}, messages[1])
					body[field] = messages[:1]
				case "anthropic":
					block = body["system"].([]any)[0].(map[string]any)
					require.Equal(t, []any{map[string]any{"role": "user", "content": row}}, body["messages"])
					delete(body, "messages")
				case "google":
					block = body["systemInstruction"].(map[string]any)["parts"].([]any)[0].(map[string]any)
					require.Equal(t, []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": row}}}}, body["contents"])
					delete(body, "contents")
				}
				require.Equal(t, "Shared rubric", block["text"])
				if tc.marker == "prompt_cache_breakpoint" {
					require.Equal(t, map[string]any{"mode": "explicit"}, body["prompt_cache_options"])
					require.Equal(t, map[string]any{"mode": "explicit"}, block[tc.marker])
				} else {
					require.NotContains(t, body, "prompt_cache_options")
					require.NotContains(t, block, "prompt_cache_breakpoint")
				}
				if tc.marker == "cache_control" {
					require.Equal(t, map[string]any{"type": "ephemeral"}, block[tc.marker])
				} else {
					require.NotContains(t, block, "cache_control")
				}
			}
			require.Equal(t, bodies[0], bodies[1], "changing row data must not change the shared prefix, schema or routing key")
			if tc.provider == "openrouter" {
				require.Len(t, bodies[0]["session_id"], 64)
				columns[0].Prompt = "A different rubric"
				_, err := c.CompleteStructured(t.Context(), "row one", "Shared rubric", columns)
				require.Error(t, err)
				require.NotEqual(t, bodies[0]["session_id"], bodies[2]["session_id"])
			}
		})
	}
}

func TestIntegerBoundsRemainExact(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		value            string
		minimum, maximum float64
		valid            bool
	}{
		{"2", 1.5, 2.5, true},
		{"1", 1.5, 2.5, false},
		{"3", 1.5, 2.5, false},
		{"-2", -2.5, -1.5, true},
		{"-3", -2.5, -1.5, false},
		{"-1", -2.5, -1.5, false},
		{"9007199254740992", 0, 9007199254740992, true},
		{"9007199254740993", 0, 9007199254740992, false},
	} {
		got, err := validateStructuredResult(`{"value":`+tc.value+`}`, []outputColumn{{Name: "value", Type: "integer", Minimum: &tc.minimum, Maximum: &tc.maximum}})
		if tc.valid {
			require.NoError(t, err)
			require.Equal(t, tc.value, fmt.Sprint(got["value"]))
		} else {
			require.Error(t, err, tc.value)
		}
	}
}

func TestTypeSafeNoulAndScore(t *testing.T) {
	t.Parallel()
	threshold := 0.8
	columns := []outputColumn{
		{Name: "probability", Type: "number", Prompt: "Does this need help?"},
		{Name: "urgent", Type: "boolean", Prompt: "Is it urgent?"},
		{Name: "page", Type: "boolean", Prompt: "Should we page?", Threshold: &threshold},
		{Name: "severity", Type: "number", Prompt: "Rate severity.", Levels: []string{"None", "Moderate", "Severe"}},
	}
	c := Client{Provider: "typesafe", Model: "jev-1.13.0", APIKey: "key", HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
		questions := body["questions"].(map[string]any)
		require.Len(t, questions, 4)
		require.Equal(t, map[string]any{"type": "noul", "instructions": "Shared instructions\n\nDoes this need help?"}, questions["probability"])
		require.Equal(t, map[string]any{"type": "noul", "instructions": "Shared instructions\n\nShould we page?"}, questions["page"])
		require.Equal(t, map[string]any{"type": "score", "instructions": "Shared instructions\n\nRate severity.", "criteria": []any{"None", "Moderate", "Severe"}}, questions["severity"])
		return response(200, `{"answers":{"probability":{"type":"noul","noul":0.8125},"urgent":{"type":"noul","noul":0.4999},"page":{"type":"noul","noul":0.8},"severity":{"type":"score","score":1.25}}}`), nil
	})}}
	got, err := c.CompleteStructured(t.Context(), "state", "Shared instructions", columns)
	require.NoError(t, err)
	require.Equal(t, map[string]any{"probability": 0.8125, "urgent": false, "page": true, "severity": 1.25}, got)

	for _, tc := range []struct {
		value string
		want  bool
	}{{"0.4999", false}, {"0.5", true}, {"1", true}, {"0", false}} {
		got, err := parseTypeSafeStructured([]byte(`{"answers":{"urgent":{"type":"noul","noul":`+tc.value+`}}}`), columns[1:2])
		require.NoError(t, err)
		require.Equal(t, tc.want, got["urgent"])
	}
	for _, kind := range []string{"noul", "score"} {
		column := outputColumn{Name: "value", Type: "number"}
		if kind == "score" {
			column.Levels = []string{"Low", "High"}
		}
		for _, invalid := range []string{"null", `"0.5"`, "true", "-0.01", "1.01", "1e999"} {
			_, err := parseTypeSafeStructured([]byte(fmt.Sprintf(`{"answers":{"value":{"type":%q,%q:%s}}}`, kind, kind, invalid)), []outputColumn{column})
			require.Error(t, err, "%s %s", kind, invalid)
		}
		_, err := parseTypeSafeStructured([]byte(`{"answers":{"value":{"type":"choice","choice":"yes"}}}`), []outputColumn{column})
		require.Error(t, err)
		maximum := 0.7
		column.Maximum = &maximum
		_, err = parseTypeSafeStructured([]byte(fmt.Sprintf(`{"answers":{"value":{"type":%q,%q:0.8}}}`, kind, kind)), []outputColumn{column})
		require.Error(t, err, "explicit numeric bounds must still apply")
	}
}

func TestCompleteStructuredWireContracts(t *testing.T) {
	t.Parallel()
	columns := []outputColumn{{Name: "category", Type: "string", Prompt: "Choose a category", Choices: map[string]string{"a": "Alpha", "b": "Beta"}}}
	tests := []struct {
		provider, url, response string
		check                   func(*testing.T, map[string]any)
	}{
		{"opencode", "https://opencode.ai/zen/v1/responses", responseText(`{"category":"a"}`), func(t *testing.T, body map[string]any) {
			format := body["text"].(map[string]any)["format"].(map[string]any)
			if format["type"] != "json_schema" || format["strict"] != true {
				t.Fatalf("unexpected Responses request: %#v", body)
			}
		}},
		{"openai", "https://api.openai.com/v1/responses", responseText(`{"category":"a"}`), func(t *testing.T, body map[string]any) {
			if body["text"].(map[string]any)["format"].(map[string]any)["type"] != "json_schema" {
				t.Fatalf("unexpected OpenAI request: %#v", body)
			}
		}},
		{"openrouter", "https://openrouter.ai/api/v1/chat/completions", `{"choices":[{"finish_reason":"stop","message":{"content":"{\"category\":\"a\"}"}}]}`, func(t *testing.T, body map[string]any) {
			if body["provider"].(map[string]any)["require_parameters"] != true || body["response_format"].(map[string]any)["type"] != "json_schema" {
				t.Fatalf("unexpected OpenRouter request: %#v", body)
			}
		}},
		{"anthropic", "https://api.anthropic.com/v1/messages", `{"type":"message","role":"assistant","stop_reason":"end_turn","content":[{"type":"text","text":"{\"category\":\"a\"}"}]}`, func(t *testing.T, body map[string]any) {
			if body["output_config"].(map[string]any)["format"].(map[string]any)["type"] != "json_schema" {
				t.Fatalf("unexpected Anthropic request: %#v", body)
			}
		}},
		{"google", "https://generativelanguage.googleapis.com/v1beta/models/model:generateContent", `{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"{\"category\":\"a\"}"}]}}]}`, func(t *testing.T, body map[string]any) {
			config := body["generationConfig"].(map[string]any)
			if config["responseMimeType"] != "application/json" || config["responseJsonSchema"] == nil {
				t.Fatalf("unexpected Google request: %#v", body)
			}
		}},
		{"typesafe", "https://api.typesafe.ai/v1/systemone", `{"answers":{"category":{"type":"choice","choice":"a"}}}`, func(t *testing.T, body map[string]any) {
			question := body["questions"].(map[string]any)["category"].(map[string]any)
			if body["state"] != "private state" || question["type"] != "choice" || question["instructions"] != "shared\n\nChoose a category" || question["criteria"].(map[string]any)["a"] != "Alpha" {
				t.Fatalf("unexpected TypeSafe request: %#v", body)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.provider, func(t *testing.T) {
			t.Parallel()
			model, wantURL := "model", tt.url
			if tt.provider == providerGoogle {
				model = "publishers/acme models/gemini?preview"
				wantURL = "https://generativelanguage.googleapis.com/v1beta/models/publishers%2Facme%20models%2Fgemini%3Fpreview:generateContent"
			}
			client := Client{Provider: tt.provider, Model: model, APIKey: "secret", MaxOutputTokens: 23, HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != wantURL || req.Header.Get("Authorization") != "Bearer secret" && tt.provider != providerAnthropic && tt.provider != providerGoogle {
					t.Fatalf("unexpected request URL or authorization")
				}
				if tt.provider == "anthropic" && (req.Header.Get("X-Api-Key") != "secret" || req.Header.Get("Anthropic-Version") != "2023-06-01") {
					t.Fatalf("unexpected Anthropic authentication headers")
				}
				if tt.provider == providerGoogle && req.Header.Get("X-Goog-Api-Key") != "secret" {
					t.Fatalf("unexpected Google authentication headers")
				}
				var body map[string]any
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if tt.provider != providerGoogle && body["model"] != model {
					t.Fatalf("model = %#v, want %q", body["model"], model)
				}
				switch tt.provider {
				case "opencode", "openai":
					if body["max_output_tokens"] != float64(23) || body["store"] != false {
						t.Fatalf("unexpected Responses options: %#v", body)
					}
				case "openrouter", "anthropic":
					if body["max_tokens"] != float64(23) {
						t.Fatalf("unexpected token limit: %#v", body)
					}
				case providerGoogle:
					if body["generationConfig"].(map[string]any)["maxOutputTokens"] != float64(23) {
						t.Fatalf("unexpected token limit: %#v", body)
					}
				}
				tt.check(t, body)
				return response(http.StatusOK, tt.response), nil
			})}}
			got, err := client.CompleteStructured(context.Background(), "private state", "shared", columns)
			if err != nil || !reflect.DeepEqual(got, map[string]any{"category": "a"}) {
				t.Fatalf("CompleteStructured() = %#v, %v", got, err)
			}
		})
	}
}

func responseText(text string) string {
	return fmt.Sprintf(`{"status":"completed","output":[{"type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":%q}]}]}`, text)
}

func TestValidateStructuredResultTypedAndExactInteger(t *testing.T) {
	t.Parallel()
	minimum, maximum := 1.5, 2.5
	columns := []outputColumn{
		{Name: "kind", Type: "string", Choices: map[string]string{"safe": ""}},
		{Name: "enabled", Type: "boolean"},
		{Name: "count", Type: "integer"},
		{Name: "ratio", Type: "number", Minimum: &minimum, Maximum: &maximum},
	}
	got, err := validateStructuredResult(`{"kind":"safe","enabled":true,"count":9007199254740993,"ratio":2.25}`, columns)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"kind": "safe", "enabled": true, "count": int64(9007199254740993), "ratio": 2.25}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}

func TestValidateStructuredResultRejectsInvalidOutputWithoutLeaks(t *testing.T) {
	t.Parallel()
	zero, ten := 0.0, 10.0
	columns := []outputColumn{{Name: "secret_name", Type: "integer", Minimum: &zero, Maximum: &ten}}
	for _, data := range []string{
		`{"secret_name":null}`, `{"secret_name":"SENSITIVE"}`, `{"secret_name":11}`,
		`{"secret_name":1,"extra":"SENSITIVE"}`, `{"secret_name":1,"secret_name":2}`,
		`{"secret_name":1} trailing`,
	} {
		_, err := validateStructuredResult(data, columns)
		if err == nil || strings.Contains(err.Error(), "SENSITIVE") {
			t.Fatalf("unsafe or absent error for invalid result: %v", err)
		}
	}
}

func TestCompleteStructuredRejectsBadTypeSafeAnswers(t *testing.T) {
	t.Parallel()
	columns := []outputColumn{{Name: "category", Type: "string", Choices: map[string]string{"good": "Good", "bad": "Bad"}}}
	for _, body := range []string{
		`{"answers":{}}`,
		`{"answers":{"category":{"type":"score","choice":"good"}}}`,
		`{"answers":{"category":{"type":"choice","choice":"SENSITIVE"}}}`,
		`{"answers":{"category":{"type":"choice","choice":"good"},"extra":{"type":"choice","choice":"good"}}}`,
	} {
		client := Client{Provider: "typesafe", Model: "model", APIKey: "key", HTTPClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return response(http.StatusOK, body), nil
		})}}
		_, err := client.CompleteStructured(context.Background(), "SENSITIVE STATE", "instructions", columns)
		if err == nil || strings.Contains(err.Error(), "SENSITIVE") {
			t.Fatalf("unsafe or absent error: %v", err)
		}
	}
}

func TestTypeSafeLive(t *testing.T) {
	t.Parallel()
	if os.Getenv("BRUIN_TYPESAFE_LIVE_TEST") != "1" {
		t.Skip("set BRUIN_TYPESAFE_LIVE_TEST=1 to run")
	}
	key := liveAPIKey(t, "typesafe")
	client := Client{Provider: "typesafe", Model: "jev-latest", APIKey: key}
	columns := []outputColumn{{Name: "category", Type: "string", Prompt: "Classify this synthetic object", Choices: map[string]string{"fruit": "An edible fruit", "vehicle": "A mode of transport"}}}
	got, err := client.CompleteStructured(context.Background(), "A synthetic red apple", "Use only the supplied categories.", columns)
	if err != nil {
		t.Fatal(err)
	}
	if got["category"] != "fruit" {
		t.Fatalf("expected fruit, received %v", got["category"])
	}
}

func TestZenStructuredLive(t *testing.T) {
	t.Parallel()
	if os.Getenv("BRUIN_INFERENCE_LIVE_TEST") != "1" {
		t.Skip("set BRUIN_INFERENCE_LIVE_TEST=1 for a paid structured Zen request")
	}
	c := &Client{Provider: "opencode", Model: "muse-spark-1.3", APIKey: liveAPIKey(t, "opencode"), MaxOutputTokens: 2048}
	columns := []outputColumn{
		{Name: "category", Type: "string", Prompt: "Classify the ticket.", Choices: map[string]string{"billing": "Payments and refunds", "technical": "Software problems"}},
		{Name: "urgent", Type: "boolean", Prompt: "Does the customer explicitly request immediate attention?"},
	}
	got, err := c.CompleteStructured(t.Context(), "I was charged twice. Please fix this immediately.", "Analyze this synthetic support ticket.", columns)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, map[string]any{"category": "billing", "urgent": true}) {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestAnthropicBoundsAndChoiceDescriptions(t *testing.T) {
	t.Parallel()
	minimum, maximum := 1.0, 5.0
	columns := []outputColumn{
		{Name: "rank", Type: "integer", Prompt: "Rate severity.", Minimum: &minimum, Maximum: &maximum},
		{Name: "category", Type: "string", Prompt: "Choose the team.", Choices: map[string]string{"a": "Payments", "b": "Software"}},
	}
	c := &Client{Provider: "anthropic", Model: "model", APIKey: "test-key", HTTPClient: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		properties := body["output_config"].(map[string]any)["format"].(map[string]any)["schema"].(map[string]any)["properties"].(map[string]any)
		rank := properties["rank"].(map[string]any)
		if rank["minimum"] != nil || rank["maximum"] != nil || !strings.Contains(rank["description"].(string), "Maximum: 5") {
			t.Fatal("Anthropic bounds must be described, not sent as unsupported schema keys")
		}
		if !strings.Contains(properties["category"].(map[string]any)["description"].(string), "Payments") {
			t.Fatal("choice rubric lost")
		}
		return response(200, `{"type":"message","role":"assistant","stop_reason":"end_turn","content":[{"type":"text","text":"{\"rank\":6,\"category\":\"a\"}"}]}`), nil
	})}}
	if _, err := c.CompleteStructured(t.Context(), "state", "", columns); err == nil {
		t.Fatal("out-of-bounds response accepted")
	}
}
