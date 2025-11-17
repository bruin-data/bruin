package python

import "github.com/bruin-data/bruin/pkg/pipeline"

var BruinToIngestrStrategyMap = map[pipeline.MaterializationStrategy]string{
	pipeline.MaterializationStrategyCreateReplace:  "replace",
	pipeline.MaterializationStrategyAppend:         "append",
	pipeline.MaterializationStrategyMerge:          "merge",
	pipeline.MaterializationStrategyDeleteInsert:   "delete+insert",
}


var SupportedPythonMaterializationStrategies = []pipeline.MaterializationStrategy{
	pipeline.MaterializationStrategyCreateReplace,
	pipeline.MaterializationStrategyAppend,
	pipeline.MaterializationStrategyMerge,
	pipeline.MaterializationStrategyDeleteInsert,
}

func IsPythonMaterializationStrategySupported(strategy pipeline.MaterializationStrategy) bool {
	_, exists := BruinToIngestrStrategyMap[strategy]
	return exists
}

func TranslateBruinStrategyToIngestr(strategy pipeline.MaterializationStrategy) (string, bool) {
	ingestrStrategy, exists := BruinToIngestrStrategyMap[strategy]
	return ingestrStrategy, exists
}
