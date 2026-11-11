package bigquery

import (
	"context"
	"strings"
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

	t.Run("sets a bruin_<type>-<random> job ID", func(t *testing.T) {
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
			t.Run(tc.queryType, func(t *testing.T) {
				t.Parallel()
				ctx := query.WithQueryType(context.Background(), tc.queryType)
				bqQuery := &bigquery.Query{}
				applyJobIDPrefix(ctx, bqQuery)
				assert.True(t, strings.HasPrefix(bqQuery.JobID, tc.wantID+"-"), bqQuery.JobID)
				assert.Greater(t, len(bqQuery.JobID), len(tc.wantID)+1)
				assert.False(t, bqQuery.AddJobIDSuffix)
			})
		}
	})

	t.Run("generates a new ID on each call", func(t *testing.T) {
		t.Parallel()
		ctx := query.WithQueryType(context.Background(), query.QueryTypeMain)
		first, second := &bigquery.Query{}, &bigquery.Query{}
		applyJobIDPrefix(ctx, first)
		applyJobIDPrefix(ctx, second)
		assert.NotEqual(t, first.JobID, second.JobID)
	})
}
