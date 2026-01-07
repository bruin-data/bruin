package enhance

import (
	"github.com/bruin-data/bruin/pkg/pipeline"
)

// EnhancementSuggestions holds all AI-suggested improvements for an asset.
type EnhancementSuggestions struct {
	AssetDescription   string                       `json:"asset_description,omitempty"`
	ColumnDescriptions map[string]string            `json:"column_descriptions,omitempty"`
	ColumnChecks       map[string][]CheckSuggestion `json:"column_checks,omitempty"`
	SuggestedTags      []string                     `json:"suggested_tags,omitempty"`
	SuggestedOwner     string                       `json:"suggested_owner,omitempty"`
	SuggestedDomains   []string                     `json:"suggested_domains,omitempty"`
	CustomChecks       []CustomCheckSuggestion      `json:"custom_checks,omitempty"`
}

// CheckSuggestion represents a suggested column check.
type CheckSuggestion struct {
	Name      string      `json:"name"`
	Value     interface{} `json:"value,omitempty"`
	Reasoning string      `json:"reasoning,omitempty"`
}

// CustomCheckSuggestion represents a suggested custom check.
type CustomCheckSuggestion struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Query       string `json:"query"`
	Value       int64  `json:"value"`
	Reasoning   string `json:"reasoning,omitempty"`
}

// ValidCheckTypes lists all valid column check types.
var ValidCheckTypes = map[string]bool{
	"not_null":        true,
	"unique":          true,
	"positive":        true,
	"negative":        true,
	"non_negative":    true,
	"min":             true,
	"max":             true,
	"accepted_values": true,
	"pattern":         true,
}

// IsEmpty returns true if no suggestions were made.
func (s *EnhancementSuggestions) IsEmpty() bool {
	if s == nil {
		return true
	}
	return s.AssetDescription == "" &&
		len(s.ColumnDescriptions) == 0 &&
		len(s.ColumnChecks) == 0 &&
		len(s.SuggestedTags) == 0 &&
		s.SuggestedOwner == "" &&
		len(s.SuggestedDomains) == 0 &&
		len(s.CustomChecks) == 0
}

// ApplySuggestions applies approved suggestions to the asset.
func ApplySuggestions(asset *pipeline.Asset, suggestions *EnhancementSuggestions) {
	if suggestions == nil || asset == nil {
		return
	}

	// Apply asset description if empty
	if suggestions.AssetDescription != "" && asset.Description == "" {
		asset.Description = suggestions.AssetDescription
	}

	// Apply column descriptions
	for colName, desc := range suggestions.ColumnDescriptions {
		for i := range asset.Columns {
			if asset.Columns[i].Name == colName && asset.Columns[i].Description == "" {
				asset.Columns[i].Description = desc
			}
		}
	}

	// Apply column checks
	for colName, checks := range suggestions.ColumnChecks {
		for i := range asset.Columns {
			if asset.Columns[i].Name == colName {
				for _, check := range checks {
					if !hasCheck(asset.Columns[i].Checks, check.Name) {
						newCheck := createColumnCheck(check)
						asset.Columns[i].Checks = append(asset.Columns[i].Checks, newCheck)
					}
				}
			}
		}
	}

	// Apply tags (merge, don't replace)
	for _, tag := range suggestions.SuggestedTags {
		if !containsString(asset.Tags, tag) {
			asset.Tags = append(asset.Tags, tag)
		}
	}

	// Apply owner if not set
	if suggestions.SuggestedOwner != "" && asset.Owner == "" {
		asset.Owner = suggestions.SuggestedOwner
	}

	// Apply domains (merge)
	for _, domain := range suggestions.SuggestedDomains {
		if !containsString(asset.Domains, domain) {
			asset.Domains = append(asset.Domains, domain)
		}
	}

	// Apply custom checks
	for _, customCheck := range suggestions.CustomChecks {
		if !hasCustomCheck(asset.CustomChecks, customCheck.Name) {
			newCheck := pipeline.CustomCheck{
				Name:        customCheck.Name,
				Description: customCheck.Description,
				Query:       customCheck.Query,
				Value:       customCheck.Value,
			}
			asset.CustomChecks = append(asset.CustomChecks, newCheck)
		}
	}
}

func hasCheck(checks []pipeline.ColumnCheck, name string) bool {
	for _, c := range checks {
		if c.Name == name {
			return true
		}
	}
	return false
}

func hasCustomCheck(checks []pipeline.CustomCheck, name string) bool {
	for _, c := range checks {
		if c.Name == name {
			return true
		}
	}
	return false
}

func containsString(slice []string, s string) bool {
	for _, item := range slice {
		if item == s {
			return true
		}
	}
	return false
}

func createColumnCheck(suggestion CheckSuggestion) pipeline.ColumnCheck {
	check := pipeline.ColumnCheck{
		Name: suggestion.Name,
	}

	// Handle value based on check type
	if suggestion.Value != nil {
		switch v := suggestion.Value.(type) {
		case float64:
			check.Value = pipeline.ColumnCheckValue{Int: intPtr(int(v))}
		case int:
			check.Value = pipeline.ColumnCheckValue{Int: intPtr(v)}
		case int64:
			check.Value = pipeline.ColumnCheckValue{Int: intPtr(int(v))}
		case string:
			check.Value = pipeline.ColumnCheckValue{String: stringPtr(v)}
		case []interface{}:
			stringSlice := make([]string, 0, len(v))
			for _, item := range v {
				if s, ok := item.(string); ok {
					stringSlice = append(stringSlice, s)
				}
			}
			check.Value = pipeline.ColumnCheckValue{StringArray: &stringSlice}
		}
	}

	return check
}

func intPtr(i int) *int {
	return &i
}

func stringPtr(s string) *string {
	return &s
}
