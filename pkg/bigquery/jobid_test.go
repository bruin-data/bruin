package bigquery

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
)

func TestApplyJobIDPrefix(t *testing.T) {
	t.Parallel()

	t.Run("no-op when context has no query type", func(t *testing.T) {
		t.Parallel()
		bqQuery := &bigquery.Query{}
		applyJobIDPrefix(context.Background(), bqQuery)
		assert.Empty(t, bqQuery.JobID)
		assert.False(t, bqQuery.AddJobIDSuffix)
	})

	t.Run("sets bruin_<type> prefix and enables suffix", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			queryType string
			wantID    string
		}{
			{query.QueryTypeMain, "bruin_main"},
			{query.QueryTypeColumn, "bruin_column"},
			{query.QueryTypeCustom, "bruin_custom"},
			{query.QueryTypeSensor, "bruin_sensor"},
			{query.QueryTypeQuery, "bruin_query"},
			{query.QueryTypeDiff, "bruin_diff"},
			{query.QueryTypeImport, "bruin_import"},
			{query.QueryTypePatch, "bruin_patch"},
			{query.QueryTypeDryRun, "bruin_dryrun"},
			{query.QueryTypePing, "bruin_ping"},
			{query.QueryTypeSchema, "bruin_schema"},
			{query.QueryTypeEnhance, "bruin_enhance"},
		}
		for _, tc := range cases {
			tc := tc
			t.Run(tc.queryType, func(t *testing.T) {
				t.Parallel()
				ctx := query.WithQueryType(context.Background(), tc.queryType)
				bqQuery := &bigquery.Query{}
				applyJobIDPrefix(ctx, bqQuery)
				assert.Equal(t, tc.wantID, bqQuery.JobID)
				assert.True(t, bqQuery.AddJobIDSuffix)
			})
		}
	})
}
