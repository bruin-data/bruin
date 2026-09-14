package inference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultMaxOutputTokens = 1024
	maxResponseBytes       = 1 << 20
	maxAttempts            = 3
	defaultTimeout         = 120 * time.Second
)

// Client calls a supported inference provider.
type Client struct {
	Provider        string
	Model           string
	APIKey          string
	MaxOutputTokens int
	HTTPClient      *http.Client
}

// Complete sends prompt to the configured provider and returns its completed text response.
func (c *Client) Complete(ctx context.Context, prompt string) (string, error) {
	if c.Model == "" {
		return "", errors.New("inference model is required")
	}
	if c.MaxOutputTokens < 0 {
		return "", errors.New("max output tokens must not be negative")
	}

	tokens := c.MaxOutputTokens
	if tokens == 0 {
		tokens = defaultMaxOutputTokens
	}

	var endpoint string
	var payload any
	switch c.Provider {
	case "opencode":
		endpoint = "https://opencode.ai/zen/v1/responses"
		payload = struct {
			Input           string `json:"input"`
			Model           string `json:"model"`
			MaxOutputTokens int    `json:"max_output_tokens"`
			Store           bool   `json:"store"`
		}{prompt, c.Model, tokens, false}
	case "openai":
		if c.APIKey == "" {
			return "", errors.New("openai API key is required")
		}
		endpoint = "https://api.openai.com/v1/responses"
		payload = struct {
			Input           string `json:"input"`
			Model           string `json:"model"`
			MaxOutputTokens int    `json:"max_output_tokens"`
			Store           bool   `json:"store"`
		}{prompt, c.Model, tokens, false}
	case "openrouter":
		if c.APIKey == "" {
			return "", errors.New("openrouter API key is required")
		}
		endpoint = "https://openrouter.ai/api/v1/chat/completions"
		payload = struct {
			Messages  []chatMessage `json:"messages"`
			Model     string        `json:"model"`
			MaxTokens int           `json:"max_tokens"`
		}{[]chatMessage{{Role: "user", Content: prompt}}, c.Model, tokens}
	case "anthropic":
		if c.APIKey == "" {
			return "", errors.New("anthropic API key is required")
		}
		endpoint = "https://api.anthropic.com/v1/messages"
		payload = struct {
			Messages  []chatMessage `json:"messages"`
			Model     string        `json:"model"`
			MaxTokens int           `json:"max_tokens"`
		}{[]chatMessage{{Role: "user", Content: prompt}}, c.Model, tokens}
	case "google":
		if c.APIKey == "" {
			return "", errors.New("google API key is required")
		}
		endpoint = "https://generativelanguage.googleapis.com/v1beta/models/" + url.PathEscape(c.Model) + ":generateContent"
		googlePayload := struct {
			Contents []struct {
				Role  string `json:"role"`
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"contents"`
			GenerationConfig struct {
				MaxOutputTokens int `json:"maxOutputTokens"`
			} `json:"generationConfig"`
		}{}
		googlePayload.Contents = append(googlePayload.Contents, struct {
			Role  string `json:"role"`
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		}{Role: "user", Parts: []struct {
			Text string `json:"text"`
		}{{Text: prompt}}})
		googlePayload.GenerationConfig.MaxOutputTokens = tokens
		payload = googlePayload
	default:
		return "", fmt.Errorf("unsupported inference provider %q", c.Provider)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return "", errors.New("could not encode inference request")
	}
	response, err := c.do(ctx, endpoint, body)
	if err != nil {
		return "", err
	}

	switch c.Provider {
	case "opencode", "openai":
		return parseOpenCode(response)
	case "openrouter":
		return parseOpenRouter(response)
	case "anthropic":
		return parseAnthropic(response)
	case "google":
		return parseGoogle(response)
	default:
		panic("provider validated above")
	}
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (c *Client) do(ctx context.Context, endpoint string, body []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()

	httpClient := c.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: defaultTimeout}
	}

	for attempt := 0; attempt < maxAttempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, errors.New("could not create inference request")
		}
		req.Header.Set("Content-Type", "application/json")
		switch c.Provider {
		case "anthropic":
			req.Header.Set("x-api-key", c.APIKey)
			req.Header.Set("anthropic-version", "2023-06-01")
		case "google":
			req.Header.Set("x-goog-api-key", c.APIKey)
		default:
			if c.APIKey != "" {
				req.Header.Set("Authorization", "Bearer "+c.APIKey)
			}
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, errors.New("inference request failed")
		}

		responseBody, readErr := readBounded(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return responseBody, nil
		}
		if (resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500) && attempt+1 < maxAttempts {
			if err := waitForRetry(ctx, retryDelay(resp.Header.Get("Retry-After"), attempt)); err != nil {
				return nil, err
			}
			continue
		}
		return nil, fmt.Errorf("inference provider returned HTTP status %d", resp.StatusCode)
	}
	panic("retry loop exhausted")
}

func readBounded(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxResponseBytes+1))
	if err != nil {
		return nil, errors.New("could not read inference response")
	}
	if len(data) > maxResponseBytes {
		return nil, errors.New("inference response is too large")
	}
	return data, nil
}

func retryDelay(value string, attempt int) time.Duration {
	const maxDelay = 10 * time.Second
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds >= 0 {
		return min(time.Duration(seconds)*time.Second, maxDelay)
	}
	if when, err := http.ParseTime(value); err == nil {
		return min(max(time.Until(when), 0), maxDelay)
	}
	return time.Duration(100*(1<<attempt)) * time.Millisecond
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func parseOpenCode(data []byte) (string, error) {
	var response struct {
		Status string          `json:"status"`
		Error  json.RawMessage `json:"error"`
		Output []struct {
			Type    string `json:"type"`
			Role    string `json:"role"`
			Status  string `json:"status"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return "", errors.New("inference provider returned a malformed response")
	}
	if len(response.Error) != 0 && string(response.Error) != "null" {
		return "", errors.New("inference provider returned an error")
	}
	if response.Status != "completed" {
		return "", errors.New("inference response was not completed")
	}
	var parts []string
	for _, output := range response.Output {
		if output.Type != "message" || output.Role != "assistant" || output.Status != "completed" {
			continue
		}
		for _, content := range output.Content {
			if content.Type == "refusal" {
				return "", errors.New("inference provider refused the request")
			}
			if content.Type == "output_text" {
				parts = append(parts, content.Text)
			}
		}
	}
	text := strings.TrimSpace(strings.Join(parts, ""))
	if text == "" {
		return "", errors.New("inference provider returned an empty response")
	}
	return text, nil
}

func parseOpenRouter(data []byte) (string, error) {
	var response struct {
		Error   json.RawMessage `json:"error"`
		Choices []struct {
			FinishReason string `json:"finish_reason"`
			Message      struct {
				Content string          `json:"content"`
				Refusal json.RawMessage `json:"refusal"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return "", errors.New("inference provider returned a malformed response")
	}
	if len(response.Error) != 0 && string(response.Error) != "null" {
		return "", errors.New("inference provider returned an error")
	}
	if len(response.Choices) == 0 {
		return "", errors.New("inference provider returned an empty response")
	}
	choice := response.Choices[0]
	if len(choice.Message.Refusal) != 0 && string(choice.Message.Refusal) != "null" && string(choice.Message.Refusal) != `""` {
		return "", errors.New("inference provider refused the request")
	}
	if choice.FinishReason != "stop" {
		return "", errors.New("inference response was not completed")
	}
	text := strings.TrimSpace(choice.Message.Content)
	if text == "" {
		return "", errors.New("inference provider returned an empty response")
	}
	return text, nil
}

func parseAnthropic(data []byte) (string, error) {
	var response struct {
		Type        string          `json:"type"`
		Role        string          `json:"role"`
		StopReason  string          `json:"stop_reason"`
		Error       json.RawMessage `json:"error"`
		StopDetails struct {
			Type string `json:"type"`
		} `json:"stop_details"`
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return "", errors.New("inference provider returned a malformed response")
	}
	if response.Type == "error" || (len(response.Error) != 0 && string(response.Error) != "null") {
		return "", errors.New("inference provider returned an error")
	}
	if response.Type != "message" || response.Role != "assistant" || response.StopReason != "end_turn" {
		return "", errors.New("inference response was not completed")
	}
	if response.StopDetails.Type == "refusal" {
		return "", errors.New("inference provider refused the request")
	}
	var parts []string
	for _, content := range response.Content {
		if content.Type != "text" {
			return "", errors.New("inference provider returned unsupported content")
		}
		parts = append(parts, content.Text)
	}
	text := strings.TrimSpace(strings.Join(parts, ""))
	if text == "" {
		return "", errors.New("inference provider returned an empty response")
	}
	return text, nil
}

func parseGoogle(data []byte) (string, error) {
	var response struct {
		Error          json.RawMessage `json:"error"`
		PromptFeedback struct {
			BlockReason string `json:"blockReason"`
		} `json:"promptFeedback"`
		Candidates []struct {
			FinishReason  string `json:"finishReason"`
			SafetyRatings []struct {
				Blocked bool `json:"blocked"`
			} `json:"safetyRatings"`
			Content struct {
				Parts []struct {
					Text    *string `json:"text"`
					Thought bool    `json:"thought"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if err := json.Unmarshal(data, &response); err != nil {
		return "", errors.New("inference provider returned a malformed response")
	}
	if len(response.Error) != 0 && string(response.Error) != "null" {
		return "", errors.New("inference provider returned an error")
	}
	if response.PromptFeedback.BlockReason != "" {
		return "", errors.New("inference provider blocked the request")
	}
	if len(response.Candidates) != 1 || response.Candidates[0].FinishReason != "STOP" {
		return "", errors.New("inference response was not completed")
	}
	candidate := response.Candidates[0]
	for _, rating := range candidate.SafetyRatings {
		if rating.Blocked {
			return "", errors.New("inference provider blocked the response")
		}
	}
	var parts []string
	for _, part := range candidate.Content.Parts {
		if part.Thought {
			continue
		}
		if part.Text == nil {
			return "", errors.New("inference provider returned unsupported content")
		}
		parts = append(parts, *part.Text)
	}
	text := strings.TrimSpace(strings.Join(parts, ""))
	if text == "" {
		return "", errors.New("inference provider returned an empty response")
	}
	return text, nil
}
