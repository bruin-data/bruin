package python

import (
	"slices"
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

// SupportedIngestrStrategies lists the incremental strategies ingestr accepts for
// any destination. Reverse-ETL-only ones live in ReverseETLIngestrStrategies.
var SupportedIngestrStrategies = []string{
	"replace",
	"append",
	"merge",
	"delete+insert",
	"truncate+insert",
}

// ReverseETLIngestrStrategies are accepted only for reverse-ETL destinations;
// ingestr rejects them elsewhere (gated on the destination's IsReverseETL marker).
var ReverseETLIngestrStrategies = []string{
	"update",
	"delete",
}

// ReverseETLIngestrDestinations write to an API rather than a table, each mapped to the
// incremental strategies it accepts. ingestr refuses --full-refresh for all of them.
var ReverseETLIngestrDestinations = map[string][]string{
	"attio":      {"merge", "update", "append", "delete", "replace"},
	"clevertap":  {"merge", "append"},
	"hubspot":    {"merge", "update", "append", "delete", "replace"},
	"salesforce": {"merge", "update", "append", "delete", "replace"},
}

// reverseETLTableStrategies narrows a destination's strategies for tables that take only one.
var reverseETLTableStrategies = map[string]map[string][]string{
	"clevertap": {"profiles": {"merge"}, "profile": {"merge"}, "events": {"append"}, "event": {"append"}},
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

func IsReverseETLIngestrStrategy(strategy string) bool {
	for _, s := range ReverseETLIngestrStrategies {
		if s == strategy {
			return true
		}
	}
	return false
}

func IsReverseETLIngestrDestination(destination string) bool {
	_, ok := ReverseETLIngestrDestinations[destination]
	return ok
}

// ReverseETLStrategies returns the strategies a reverse-ETL destination accepts for the table.
func ReverseETLStrategies(destination, table string) []string {
	if s, ok := reverseETLTableStrategies[destination][reverseETLObject(table)]; ok {
		return s
	}
	return ReverseETLIngestrDestinations[destination]
}

func ReverseETLDestinationSupportsStrategy(destination, table, strategy string) bool {
	return slices.Contains(ReverseETLStrategies(destination, table), strategy)
}

// reverseETLObject is the object a destination table names: "events?ts=time" -> "events".
func reverseETLObject(table string) string {
	table, _, _ = strings.Cut(table, "?")
	if i := strings.LastIndex(table, "."); i >= 0 {
		table = table[i+1:]
	}
	return strings.ToLower(strings.TrimSpace(table))
}

// GetSupportedIngestrStrategiesString returns a comma-separated string of supported ingestr strategies.
func GetSupportedIngestrStrategiesString() string {
	return strings.Join(SupportedIngestrStrategies, ", ")
}
