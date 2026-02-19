package python

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

func TestIsPythonMaterializationStrategySupported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy pipeline.MaterializationStrategy
		want     bool
	}{
		{name: "create+replace", strategy: pipeline.MaterializationStrategyCreateReplace, want: true},
		{name: "append", strategy: pipeline.MaterializationStrategyAppend, want: true},
		{name: "merge", strategy: pipeline.MaterializationStrategyMerge, want: true},
		{name: "delete+insert", strategy: pipeline.MaterializationStrategyDeleteInsert, want: true},
		{name: "time_interval", strategy: pipeline.MaterializationStrategyTimeInterval, want: true},
		{name: "scd2_by_time is unsupported", strategy: pipeline.MaterializationStrategySCD2ByTime, want: false},
		{name: "ddl is unsupported", strategy: pipeline.MaterializationStrategyDDL, want: false},
		{name: "empty is unsupported", strategy: pipeline.MaterializationStrategyNone, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, IsPythonMaterializationStrategySupported(tt.strategy))
		})
	}
}

func TestTranslateBruinStrategyToIngestr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		strategy    pipeline.MaterializationStrategy
		wantIngestr string
		wantExists  bool
	}{
		{name: "create+replace maps to replace", strategy: pipeline.MaterializationStrategyCreateReplace, wantIngestr: "replace", wantExists: true},
		{name: "append maps to append", strategy: pipeline.MaterializationStrategyAppend, wantIngestr: "append", wantExists: true},
		{name: "merge maps to merge", strategy: pipeline.MaterializationStrategyMerge, wantIngestr: "merge", wantExists: true},
		{name: "delete+insert maps to delete+insert", strategy: pipeline.MaterializationStrategyDeleteInsert, wantIngestr: "delete+insert", wantExists: true},
		{name: "time_interval maps to delete+insert", strategy: pipeline.MaterializationStrategyTimeInterval, wantIngestr: "delete+insert", wantExists: true},
		{name: "unsupported strategy returns false", strategy: pipeline.MaterializationStrategySCD2ByTime, wantIngestr: "", wantExists: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, exists := TranslateBruinStrategyToIngestr(tt.strategy)
			assert.Equal(t, tt.wantExists, exists)
			assert.Equal(t, tt.wantIngestr, got)
		})
	}
}
