package bigquery

import (
	"context"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/stretchr/testify/assert"
)

func TestApplyJobID(t *testing.T) {
	t.Parallel()

	t.Run("no-op when context has no query type", func(t *testing.T) {
		t.Parallel()
		bqQuery := &bigquery.Query{}
		applyJobID(context.Background(), bqQuery)
		assert.Empty(t, bqQuery.JobID)
		assert.False(t, bqQuery.AddJobIDSuffix)
	})

	t.Run("sets a unique bruin_<type>-<random> ID", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			queryType  string
			wantPrefix string
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
				first, second := &bigquery.Query{}, &bigquery.Query{}
				applyJobID(ctx, first)
				applyJobID(ctx, second)
				assert.Regexp(t, "^"+tc.wantPrefix+"-[A-Z2-7]{26}$", first.JobID)
				assert.NotEqual(t, first.JobID, second.JobID)
				assert.False(t, first.AddJobIDSuffix)
			})
		}
	})
}
