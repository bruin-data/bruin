package python

import (
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

var BruinToIngestrStrategyMap = map[pipeline.MaterializationStrategy]string{
	pipeline.MaterializationStrategyCreateReplace: "replace",
	pipeline.MaterializationStrategyAppend:        "append",
	pipeline.MaterializationStrategyMerge:         "merge",
	pipeline.MaterializationStrategyDeleteInsert:  "delete+insert",
}

var bruinToIngestrMaterializationStrategyMap = map[pipeline.MaterializationStrategy]string{
	pipeline.MaterializationStrategyCreateReplace:  "replace",
	pipeline.MaterializationStrategyAppend:         "append",
	pipeline.MaterializationStrategyMerge:          "merge",
	pipeline.MaterializationStrategyDeleteInsert:   "delete+insert",
	pipeline.MaterializationStrategyTruncateInsert: "truncate+insert",
}

// SupportedPythonMaterializationStrategies lists all materialization strategies supported by Python assets.
var SupportedPythonMaterializationStrategies = []pipeline.MaterializationStrategy{
	pipeline.MaterializationStrategyCreateReplace,
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

// TranslateBruinMaterializationStrategyToIngestr converts Bruin materialization strategy names
// to ingestr incremental strategy names.
func TranslateBruinMaterializationStrategyToIngestr(strategy pipeline.MaterializationStrategy) (string, bool) {
	ingestrStrategy, exists := bruinToIngestrMaterializationStrategyMap[strategy]
	return ingestrStrategy, exists
}

func IsIngestrMaterializationStrategySupported(strategy pipeline.MaterializationStrategy) bool {
	_, exists := bruinToIngestrMaterializationStrategyMap[strategy]
	return exists
}

func GetSupportedIngestrMaterializationStrategiesString() string {
	strategies := make([]string, 0, len(bruinToIngestrMaterializationStrategyMap))
	for _, s := range []pipeline.MaterializationStrategy{
		pipeline.MaterializationStrategyCreateReplace,
		pipeline.MaterializationStrategyAppend,
		pipeline.MaterializationStrategyMerge,
		pipeline.MaterializationStrategyDeleteInsert,
		pipeline.MaterializationStrategyTruncateInsert,
	} {
		strategies = append(strategies, string(s))
	}
	return strings.Join(strategies, ", ")
}

func IsIngestrIncrementalKeyStrategy(strategy string) bool {
	switch strategy {
	case "append", "merge", "delete+insert":
		return true
	default:
		return false
	}
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
	"truncate+insert",
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
