package cmd

import (
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClean(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		s    string
		want string
	}{
		{
			name: "empty",
			s:    "\u001B[0m\u001B[94m[2023-12-05 19:11:25] [hello_python] >> [2023-12-05 19:11:25 - INFO] Starting extractor: gcs_bucket_files",
			want: "[2023-12-05 19:11:25] [hello_python] >> [2023-12-05 19:11:25 - INFO] Starting extractor: gcs_bucket_files",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equalf(t, tt.want, Clean(tt.s), "Clean(%v)", tt.s)
		})
	}
}

func BenchmarkClean(b *testing.B) {
	for i := 0; i < b.N; i++ {
		// Clean("\u001B[0m\u001B[94m[2023-12-05 19:11:25] [hello_python] >> [2023-12-05 19:11:25 - INFO] Starting extractor: gcs_bucket_files")
		Clean("\u001B[0m\u001B[94m[2023-12-05 19:11:26] [hello_python] >>   File \"/Users/burak/.bruin/virtualenvs/77ef9663d804ac96afe6fb2a10d2b2b817a07fd82875759af8910b4fe31a7149/lib/python3.9/site-packages/google/api_core/page_iterator.py\", line 208, in _items_iter")
	}
}

func TestExcludeAssetsByTag(t *testing.T) {
	// Mock pipeline setup
	mockPipeline := &pipeline.Pipeline{
		Assets: []*pipeline.Asset{
			{Name: "asset1", Tags: []string{"tag1", "tag2"}},
			{Name: "asset2", Tags: []string{"tag1"}},
			{Name: "asset3", Tags: []string{"tag2"}},
			{Name: "asset4", Tags: []string{"tag3"}},
			{Name: "asset5", Tags: []string{"tag2", "tag2"}},
		},
	}
	// Mock scheduler setup
	mockScheduler := scheduler.NewScheduler(nil, mockPipeline)

	tests := []struct {
		name             string
		assetsByTag      []*pipeline.Asset
		excludeTag       string
		expectedExcluded int
	}{
		{
			name:             "Empty assetsByTag, valid excludeTag",
			assetsByTag:      []*pipeline.Asset{},
			excludeTag:       "tag2",
			expectedExcluded: 3, // Exclude globally
		},
		{
			name: "Subset of assets with excludeTag",
			assetsByTag: []*pipeline.Asset{
				{Name: "asset1", Tags: []string{"tag1", "tag2"}},
				{Name: "asset2", Tags: []string{"tag1"}},
			},
			excludeTag:       "tag2",
			expectedExcluded: 1, // Exclude from the subset
		},
		{
			name: "Subset of assets without excludeTag",
			assetsByTag: []*pipeline.Asset{
				{Name: "asset2", Tags: []string{"tag1"}},
			},
			excludeTag:       "tag2",
			expectedExcluded: 0, // Nothing to exclude
		},
		{
			name: "Empty excludeTag",
			assetsByTag: []*pipeline.Asset{
				{Name: "asset1", Tags: []string{"tag1", "tag2"}},
			},
			excludeTag:       "",
			expectedExcluded: 0, // Nothing happens when excludeTag is empty
		},
		{
			name:             "Empty pipeline",
			assetsByTag:      []*pipeline.Asset{},
			excludeTag:       "tag1",
			expectedExcluded: 2,
		},

		{
			name: "Asset with duplicate tags",
			assetsByTag: []*pipeline.Asset{
				{Name: "asset1", Tags: []string{"tag1", "tag1"}},
			},
			excludeTag:       "tag1",
			expectedExcluded: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run the function
			excludedCount := ExcludeAssetsByTag(tt.excludeTag, mockPipeline, mockScheduler, tt.assetsByTag)

			// Assert the count of excluded assets
			assert.Equal(t, tt.expectedExcluded, excludedCount)
		})

	}
}
