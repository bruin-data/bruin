package enhance

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// ParseClaudeResponse extracts and validates suggestions from Claude's response.
func ParseClaudeResponse(response string) (*EnhancementSuggestions, error) {
	if response == "" {
		return nil, errors.New("empty response from Claude")
	}

	// Extract JSON from response (Claude might include markdown or explanation text)
	jsonStr := extractJSON(response)
	if jsonStr == "" {
		return nil, errors.Errorf("no valid JSON found in Claude response: %s", truncateString(response, 200))
	}

	var suggestions EnhancementSuggestions
	if err := json.Unmarshal([]byte(jsonStr), &suggestions); err != nil {
		return nil, errors.Wrapf(err, "failed to parse Claude response as JSON: %s", truncateString(jsonStr, 200))
	}

	// Validate and filter invalid suggestions
	if err := validateAndFilterSuggestions(&suggestions); err != nil {
		return nil, errors.Wrap(err, "invalid suggestions")
	}

	return &suggestions, nil
}

// extractJSON finds and extracts JSON object from a string that may contain other text.
func extractJSON(response string) string {
	// Try to find JSON object - look for outermost { }
	response = strings.TrimSpace(response)

	// If response starts with {, try to parse as-is
	if strings.HasPrefix(response, "{") {
		// Find matching closing brace
		depth := 0
		inString := false
		escaped := false

		for i, ch := range response {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' && inString {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = !inString
				continue
			}
			if inString {
				continue
			}
			if ch == '{' {
				depth++
			} else if ch == '}' {
				depth--
				if depth == 0 {
					return response[:i+1]
				}
			}
		}
	}

	// Try to extract JSON from markdown code block
	codeBlockRegex := regexp.MustCompile("```(?:json)?\\s*([\\s\\S]*?)```")
	matches := codeBlockRegex.FindStringSubmatch(response)
	if len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}

	// Fallback: find first { and last }
	start := strings.Index(response, "{")
	end := strings.LastIndex(response, "}")
	if start != -1 && end != -1 && end > start {
		return response[start : end+1]
	}

	return ""
}

// validateAndFilterSuggestions validates suggestions and removes invalid ones.
func validateAndFilterSuggestions(s *EnhancementSuggestions) error {
	if s == nil {
		return errors.New("nil suggestions")
	}

	// Validate and filter column checks
	for colName, checks := range s.ColumnChecks {
		validChecks := make([]CheckSuggestion, 0, len(checks))
		for _, check := range checks {
			if ValidCheckTypes[check.Name] {
				// Validate value based on check type
				if isValidCheckValue(check.Name, check.Value) {
					validChecks = append(validChecks, check)
				}
			}
		}
		if len(validChecks) > 0 {
			s.ColumnChecks[colName] = validChecks
		} else {
			delete(s.ColumnChecks, colName)
		}
	}

	// Trim whitespace from descriptions
	s.AssetDescription = strings.TrimSpace(s.AssetDescription)
	for colName, desc := range s.ColumnDescriptions {
		s.ColumnDescriptions[colName] = strings.TrimSpace(desc)
		if s.ColumnDescriptions[colName] == "" {
			delete(s.ColumnDescriptions, colName)
		}
	}

	// Filter empty tags
	validTags := make([]string, 0, len(s.SuggestedTags))
	for _, tag := range s.SuggestedTags {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			validTags = append(validTags, tag)
		}
	}
	s.SuggestedTags = validTags

	// Trim owner
	s.SuggestedOwner = strings.TrimSpace(s.SuggestedOwner)

	// Filter empty domains
	validDomains := make([]string, 0, len(s.SuggestedDomains))
	for _, domain := range s.SuggestedDomains {
		domain = strings.TrimSpace(domain)
		if domain != "" {
			validDomains = append(validDomains, domain)
		}
	}
	s.SuggestedDomains = validDomains

	return nil
}

// isValidCheckValue validates that the value is appropriate for the check type.
func isValidCheckValue(checkName string, value interface{}) bool {
	switch checkName {
	case "not_null", "unique", "positive", "negative", "non_negative":
		// These checks don't require a value
		return true
	case "min", "max":
		// These require a numeric value
		if value == nil {
			return false
		}
		switch value.(type) {
		case float64, int, int64:
			return true
		case map[string]interface{}:
			// Handle {"value": N} format
			m := value.(map[string]interface{})
			_, ok := m["value"]
			return ok
		}
		return false
	case "accepted_values":
		// Requires an array of values
		if value == nil {
			return false
		}
		switch v := value.(type) {
		case []interface{}:
			return len(v) > 0
		case map[string]interface{}:
			// Handle {"value": [...]} format
			if arr, ok := v["value"]; ok {
				if arrVal, ok := arr.([]interface{}); ok {
					return len(arrVal) > 0
				}
			}
		}
		return false
	case "pattern":
		// Requires a string pattern
		if value == nil {
			return false
		}
		switch v := value.(type) {
		case string:
			return v != ""
		case map[string]interface{}:
			// Handle {"value": "pattern"} format
			if pattern, ok := v["value"]; ok {
				if patternStr, ok := pattern.(string); ok {
					return patternStr != ""
				}
			}
		}
		return false
	}
	return true
}

func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
