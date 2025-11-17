package python

import (
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

var BruinToIngestrStrategyMap = map[pipeline.MaterializationStrategy]string{
	pipeline.MaterializationStrategyCreateReplace: "replace",
	"replace":                                    "replace",
	pipeline.MaterializationStrategyAppend:       "append",
	pipeline.MaterializationStrategyMerge:        "merge",
	pipeline.MaterializationStrategyDeleteInsert: "delete+insert",
}

// SupportedPythonMaterializationStrategies lists all materialization strategies supported by Python assets.
var SupportedPythonMaterializationStrategies = []pipeline.MaterializationStrategy{
	pipeline.MaterializationStrategyCreateReplace,
	"replace",
	pipeline.MaterializationStrategyAppend,
	pipeline.MaterializationStrategyMerge,
	pipeline.MaterializationStrategyDeleteInsert,
}

// IsPythonMaterializationStrategySupported checks if a given strategy is supported for Python assets.
func IsPythonMaterializationStrategySupported(strategy pipeline.MaterializationStrategy) bool {
	_, exists := BruinToIngestrStrategyMap[strategy]
	return exists
}

// TranslateBruinStrategyToIngestr converts a Bruin materialization strategy to its ingestr equivalent.
func TranslateBruinStrategyToIngestr(strategy pipeline.MaterializationStrategy) (string, bool) {
	ingestrStrategy, exists := BruinToIngestrStrategyMap[strategy]
	return ingestrStrategy, exists
}

// GetSupportedPythonStrategiesString returns a comma-separated string of supported Python materialization strategies.
func GetSupportedPythonStrategiesString() string {
	strategies := make([]string, 0, len(SupportedPythonMaterializationStrategies))
	for _, s := range SupportedPythonMaterializationStrategies {
		strategies = append(strategies, string(s))
	}
	return strings.Join(strategies, ", ")
}

// SupportedIngestrStrategies lists all incremental strategies supported by ingestr.
var SupportedIngestrStrategies = []string{
	"replace",
	"append",
	"merge",
	"delete+insert",
}

// IsIngestrStrategySupported checks if a given strategy string is supported by ingestr.
func IsIngestrStrategySupported(strategy string) bool {
	for _, s := range SupportedIngestrStrategies {
		if s == strategy {
			return true
		}
	}
	return false
}

// GetSupportedIngestrStrategiesString returns a comma-separated string of supported ingestr strategies.
func GetSupportedIngestrStrategiesString() string {
	return strings.Join(SupportedIngestrStrategies, ", ")
}
