package inference

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/url"
	"sort"
	"strings"
)

// CompleteStructured asks the provider for one schema-constrained object and
// validates the object before returning it. Configuration and credentials must
// already have been validated by readConfig and resolveGroups.
func (c *Client) CompleteStructured(ctx context.Context, state, instructions string, columns []outputColumn) (map[string]any, error) {
	tokens := c.MaxOutputTokens
	if tokens == 0 {
		tokens = defaultMaxOutputTokens
	}
	schema := structuredSchema(columns)
	prompt := instructions
	if prompt != "" {
		prompt += "\n\n"
	}
	prompt += state

	var endpoint string
	var payload any
	switch c.Provider {
	case providerOpenCode, providerOpenAI:
		endpoint = "https://opencode.ai/zen/v1/responses"
		if c.Provider == providerOpenAI {
			endpoint = "https://api.openai.com/v1/responses"
		}
		payload = map[string]any{
			"model": c.Model, "input": prompt, "max_output_tokens": tokens, "store": false,
			"text": map[string]any{"format": map[string]any{"type": "json_schema", "name": "inference_result", "strict": true, "schema": schema}},
		}
	case providerOpenRouter:
		endpoint = "https://openrouter.ai/api/v1/chat/completions"
		payload = map[string]any{
			"model": c.Model, "messages": []chatMessage{{Role: "user", Content: prompt}}, "max_tokens": tokens,
			"response_format": map[string]any{"type": "json_schema", "json_schema": map[string]any{"name": "inference_result", "strict": true, "schema": schema}},
			"provider":        map[string]any{"require_parameters": true},
		}
	case providerAnthropic:
		// Anthropic's schema subset does not accept numeric bounds. Keep them
		// in field descriptions and enforce them locally on every response.
		for _, raw := range schema["properties"].(map[string]any) {
			property := raw.(map[string]any)
			delete(property, "minimum")
			delete(property, "maximum")
		}
		endpoint = "https://api.anthropic.com/v1/messages"
		payload = map[string]any{
			"model": c.Model, "messages": []chatMessage{{Role: "user", Content: prompt}}, "max_tokens": tokens,
			"output_config": map[string]any{"format": map[string]any{"type": "json_schema", "schema": schema}},
		}
	case providerGoogle:
		endpoint = "https://generativelanguage.googleapis.com/v1beta/models/" + url.PathEscape(c.Model) + ":generateContent"
		payload = map[string]any{
			"contents":         []any{map[string]any{"role": "user", "parts": []any{map[string]any{"text": prompt}}}},
			"generationConfig": map[string]any{"maxOutputTokens": tokens, "responseMimeType": "application/json", "responseJsonSchema": schema},
		}
	case providerTypeSafe:
		questions := make(map[string]any, len(columns))
		for _, column := range columns {
			kind := column.typeSafeKind()
			questionInstructions := strings.TrimSpace(strings.Join([]string{instructions, column.Prompt}, "\n\n"))
			question := map[string]any{"type": kind, "instructions": questionInstructions}
			switch kind {
			case kindChoice:
				question["criteria"] = column.Choices
			case kindScore:
				question["criteria"] = column.Levels
			}
			questions[column.Name] = question
		}
		endpoint = "https://api.typesafe.ai/v1/systemone"
		payload = map[string]any{"model": c.Model, "state": state, "questions": questions}
	default:
		return nil, fmt.Errorf("unsupported inference provider %q", c.Provider)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, errors.New("could not encode inference request")
	}
	response, err := c.do(ctx, endpoint, body)
	if err != nil {
		return nil, err
	}
	if c.Provider == providerTypeSafe {
		return parseTypeSafeStructured(response, columns)
	}
	var data string
	switch c.Provider {
	case providerOpenCode, providerOpenAI:
		data, err = parseOpenCode(response)
	case providerOpenRouter:
		data, err = parseOpenRouter(response)
	case providerAnthropic:
		data, err = parseAnthropic(response)
	case providerGoogle:
		data, err = parseGoogle(response)
	}
	if err != nil {
		return nil, err
	}
	return validateStructuredResult(data, columns)
}

func structuredSchema(columns []outputColumn) map[string]any {
	properties := make(map[string]any, len(columns))
	required := make([]string, 0, len(columns))
	for _, column := range columns {
		property := map[string]any{"type": column.Type}
		description := column.Prompt
		if len(column.Choices) != 0 {
			choices := make([]string, 0, len(column.Choices))
			for choice := range column.Choices {
				choices = append(choices, choice)
			}
			sort.Strings(choices)
			property["enum"] = choices
			var rubric strings.Builder
			for _, choice := range choices {
				fmt.Fprintf(&rubric, "\n%q: %s", choice, column.Choices[choice])
			}
			description += rubric.String()
		}
		if column.Minimum != nil {
			property["minimum"] = *column.Minimum
			description += fmt.Sprintf("\nMinimum: %g", *column.Minimum)
		}
		if column.Maximum != nil {
			property["maximum"] = *column.Maximum
			description += fmt.Sprintf("\nMaximum: %g", *column.Maximum)
		}
		property["description"] = description
		properties[column.Name] = property
		required = append(required, column.Name)
	}
	return map[string]any{"type": "object", "properties": properties, "required": required, "additionalProperties": false}
}

// validateStructuredResult validates provider or cached structured JSON. It
// deliberately does not include provider-returned values in errors.
func validateStructuredResult(data string, columns []outputColumn) (map[string]any, error) {
	value, err := decodeUniqueJSON(strings.NewReader(data))
	if err != nil {
		return nil, errors.New("inference provider returned malformed structured output")
	}
	object, ok := value.(map[string]any)
	if !ok || len(object) != len(columns) {
		return nil, errors.New("inference provider returned unexpected structured output fields")
	}
	for _, column := range columns {
		value, exists := object[column.Name]
		if !exists || value == nil {
			return nil, errors.New("inference provider returned a missing or null structured output field")
		}
		switch column.Type {
		case colTypeString:
			text, ok := value.(string)
			if !ok {
				return nil, errors.New("inference provider returned a structured output field with the wrong type")
			}
			if len(column.Choices) != 0 {
				if _, ok := column.Choices[text]; !ok {
					return nil, errors.New("inference provider returned a structured output field outside its allowed choices")
				}
			}
		case colTypeBoolean:
			_, ok := value.(bool)
			if !ok {
				return nil, errors.New("inference provider returned a structured output field with the wrong type")
			}
		case colTypeInteger:
			number, ok := value.(json.Number)
			if !ok {
				return nil, errors.New("inference provider returned a structured output field with the wrong type")
			}
			integer, err := number.Int64()
			if err != nil || (column.Minimum != nil && new(big.Float).SetInt64(integer).Cmp(new(big.Float).SetFloat64(*column.Minimum)) < 0) ||
				(column.Maximum != nil && new(big.Float).SetInt64(integer).Cmp(new(big.Float).SetFloat64(*column.Maximum)) > 0) {
				return nil, errors.New("inference provider returned an invalid integer structured output field")
			}
			object[column.Name] = integer
		case colTypeNumber:
			number, ok := value.(json.Number)
			if !ok {
				return nil, errors.New("inference provider returned a structured output field with the wrong type")
			}
			floating, err := number.Float64()
			if err != nil || math.IsInf(floating, 0) || math.IsNaN(floating) || !withinBounds(floating, column) {
				return nil, errors.New("inference provider returned an invalid numeric structured output field")
			}
			object[column.Name] = floating
		}
	}
	return object, nil
}

func withinBounds(value float64, column outputColumn) bool {
	return (column.Minimum == nil || value >= *column.Minimum) && (column.Maximum == nil || value <= *column.Maximum)
}

func decodeUniqueJSON(reader io.Reader) (any, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	value, err := decodeUniqueValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing JSON data")
	}
	return value, nil
}

func decodeUniqueValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return token, nil
	}
	switch delimiter {
	case '{':
		object := make(map[string]any)
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("invalid object key")
			}
			if _, exists := object[key]; exists {
				return nil, errors.New("duplicate object key")
			}
			object[key], err = decodeUniqueValue(decoder)
			if err != nil {
				return nil, err
			}
		}
		_, err := decoder.Token()
		return object, err
	case '[':
		var array []any
		for decoder.More() {
			item, err := decodeUniqueValue(decoder)
			if err != nil {
				return nil, err
			}
			array = append(array, item)
		}
		_, err := decoder.Token()
		return array, err
	default:
		return nil, errors.New("unexpected JSON delimiter")
	}
}

func parseTypeSafeStructured(data []byte, columns []outputColumn) (map[string]any, error) {
	decoded, err := decodeUniqueJSON(bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("inference provider returned a malformed response")
	}
	root, ok := decoded.(map[string]any)
	if !ok {
		return nil, errors.New("inference provider returned a malformed response")
	}
	answers, ok := root["answers"].(map[string]any)
	if !ok || len(answers) != len(columns) {
		return nil, errors.New("inference provider returned unexpected answer fields")
	}
	for _, column := range columns {
		kind := column.typeSafeKind()
		answer, exists := answers[column.Name]
		object, ok := answer.(map[string]any)
		if !exists || !ok || object["type"] != kind {
			return nil, errors.New("inference provider returned an invalid TypeSafe answer type")
		}
		if kind == kindChoice {
			label, ok := object[kindChoice].(string)
			if !ok {
				return nil, errors.New("inference provider returned an invalid choice answer")
			}
			if _, exists := column.Choices[label]; !exists {
				return nil, errors.New("inference provider returned a choice outside the configured choices")
			}
			answers[column.Name] = label
			continue
		}
		number, ok := object[kind].(json.Number)
		if !ok {
			return nil, errors.New("inference provider returned a nonnumeric TypeSafe answer")
		}
		value, err := number.Float64()
		maximum := 1.0
		if kind == kindScore {
			maximum = float64(len(column.Levels) - 1)
		}
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > maximum || !withinBounds(value, column) {
			return nil, errors.New("inference provider returned an out-of-range TypeSafe answer")
		}
		if column.Type == colTypeBoolean {
			threshold := 0.5
			if column.Threshold != nil {
				threshold = *column.Threshold
			}
			answers[column.Name] = value >= threshold
		} else {
			answers[column.Name] = value
		}
	}
	return answers, nil
}
