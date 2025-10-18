package bigquery

import (
	"context"
	"errors"
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
)

// Mock interfaces.
type mockConnectionGetter struct {
	mock.Mock
}

func (m *mockConnectionGetter) GetConnection(name string) any {
	args := m.Called(name)
	return args.Get(0)
}

type mockQueryExtractor struct {
	mock.Mock
}

type mockDryRunnerQuerier struct {
	mock.Mock
}

func (m *mockDryRunnerQuerier) QueryDryRun(ctx context.Context, queryObj *query.Query) (*bigquery.QueryStatistics, error) {
	args := m.Called(ctx, queryObj)
	return args.Get(0).(*bigquery.QueryStatistics), args.Error(1)
}

func (m *mockQueryExtractor) ExtractQueriesFromString(filepath string) ([]*query.Query, error) {
	args := m.Called(filepath)
	return args.Get(0).([]*query.Query), args.Error(1)
}

func TestDryRunner_DryRun(t *testing.T) {
	t.Parallel()

	t.Run("non-BigQuery asset type should error", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type: pipeline.AssetTypeSnowflakeQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT * FROM test_table",
			},
		}

		pipeline := &pipeline.Pipeline{}

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.Background(), *pipeline, asset, &config.Config{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "asset-metadata is only available for BigQuery SQL assets")
		assert.Nil(t, result)
	})

	t.Run("query extractor error", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type: pipeline.AssetTypeBigqueryQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Content: "INVALID SQL",
			},
		}

		pipeline := &pipeline.Pipeline{}

		queryExtractor.On("ExtractQueriesFromString", "INVALID SQL").Return([]*query.Query{}, errors.New("syntax error"))

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.Background(), *pipeline, asset, &config.Config{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "syntax error")
		assert.Nil(t, result)

		queryExtractor.AssertExpectations(t)
	})

	t.Run("no queries found", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type: pipeline.AssetTypeBigqueryQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Content: "",
			},
		}

		pipeline := &pipeline.Pipeline{}

		queryExtractor.On("ExtractQueriesFromString", "").Return([]*query.Query{}, nil)

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.Background(), *pipeline, asset, &config.Config{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no query found in asset")
		assert.Nil(t, result)

		queryExtractor.AssertExpectations(t)
	})

	t.Run("pipeline connection name error - no default connection", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type: pipeline.AssetTypeBigqueryQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT * FROM test_table",
			},
		}

		pipeline := &pipeline.Pipeline{
			DefaultConnections: map[string]string{},
		}

		queryExtractor.On("ExtractQueriesFromString", "SELECT * FROM test_table").Return([]*query.Query{
			{Query: "SELECT * FROM test_table"},
		}, nil)
		connGetter.On("GetConnection", "gcp-default").Return("not-a-bigquery-client")

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.Background(), *pipeline, asset, &config.Config{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolved connection is not BigQuery")
		assert.Nil(t, result)

		connGetter.AssertExpectations(t)
		queryExtractor.AssertExpectations(t)
	})

	t.Run("connection is not BigQuery", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type: pipeline.AssetTypeBigqueryQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT * FROM test_table",
			},
		}

		pipeline := &pipeline.Pipeline{
			DefaultConnections: map[string]string{
				"google_cloud_platform": "test-connection",
			},
		}

		queryExtractor.On("ExtractQueriesFromString", "SELECT * FROM test_table").Return([]*query.Query{
			{Query: "SELECT * FROM test_table"},
		}, nil)
		connGetter.On("GetConnection", "test-connection").Return("not-a-bigquery-client")

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.Background(), *pipeline, asset, &config.Config{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolved connection is not BigQuery")
		assert.Nil(t, result)

		connGetter.AssertExpectations(t)
		queryExtractor.AssertExpectations(t)
	})

	t.Run("asset with explicit connection", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type:       pipeline.AssetTypeBigqueryQuery,
			Connection: "explicit-connection",
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT * FROM test_table",
			},
		}

		pipeline := &pipeline.Pipeline{
			DefaultConnections: map[string]string{
				"google_cloud_platform": "default-connection",
			},
		}

		mockDryRunnerQuerier := &mockDryRunnerQuerier{}
		mockDryRunnerQuerier.On("QueryDryRun", mock.Anything, mock.AnythingOfType("*query.Query")).Return(&bigquery.QueryStatistics{
			StatementType: "SELECT",
		}, nil)

		queryExtractor.On(
			"ExtractQueriesFromString",
			"SELECT * FROM test_table",
		).
			Return(
				[]*query.Query{{Query: "SELECT * FROM test_table"}},
				nil,
			)

		connGetter.On("GetConnection", "explicit-connection").Return(mockDryRunnerQuerier)

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.Background(), *pipeline, asset, &config.Config{})

		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, "SELECT", result["bigquery"].(*bigquery.QueryStatistics).StatementType)

		connGetter.AssertExpectations(t)
		queryExtractor.AssertExpectations(t)
		mockDryRunnerQuerier.AssertExpectations(t)
	})
}

func TestDryRunner_DryRun_EdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("nil context", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type: pipeline.AssetTypeBigqueryQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT * FROM test_table",
			},
		}

		pipeline := &pipeline.Pipeline{
			DefaultConnections: map[string]string{
				"google_cloud_platform": "test-connection",
			},
		}

		queryExtractor.On("ExtractQueriesFromString", "SELECT * FROM test_table").Return([]*query.Query{
			{Query: "SELECT * FROM test_table"},
		}, nil)
		connGetter.On("GetConnection", "test-connection").Return("not-a-bigquery-client")

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.TODO(), *pipeline, asset, &config.Config{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolved connection is not BigQuery")
		assert.Nil(t, result)

		connGetter.AssertExpectations(t)
		queryExtractor.AssertExpectations(t)
	})

	t.Run("nil config", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type: pipeline.AssetTypeBigqueryQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT * FROM test_table",
			},
		}

		pipeline := &pipeline.Pipeline{
			DefaultConnections: map[string]string{
				"google_cloud_platform": "test-connection",
			},
		}

		queryExtractor.On("ExtractQueriesFromString", "SELECT * FROM test_table").Return([]*query.Query{
			{Query: "SELECT * FROM test_table"},
		}, nil)
		connGetter.On("GetConnection", "test-connection").Return("not-a-bigquery-client")

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.Background(), *pipeline, asset, nil)

		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolved connection is not BigQuery")
		assert.Nil(t, result)

		connGetter.AssertExpectations(t)
		queryExtractor.AssertExpectations(t)
	})

	t.Run("empty query content", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type: pipeline.AssetTypeBigqueryQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Content: "",
			},
		}

		pipeline := &pipeline.Pipeline{
			DefaultConnections: map[string]string{
				"google_cloud_platform": "test-connection",
			},
		}

		queryExtractor.On("ExtractQueriesFromString", "").Return([]*query.Query{}, nil)

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.Background(), *pipeline, asset, &config.Config{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "no query found in asset")
		assert.Nil(t, result)

		queryExtractor.AssertExpectations(t)
	})

	t.Run("multiple queries - uses first query", func(t *testing.T) {
		t.Parallel()
		connGetter := &mockConnectionGetter{}
		queryExtractor := &mockQueryExtractor{}

		asset := pipeline.Asset{
			Type: pipeline.AssetTypeBigqueryQuery,
			ExecutableFile: pipeline.ExecutableFile{
				Content: "SELECT 1; SELECT 2;",
			},
		}

		pipeline := &pipeline.Pipeline{
			DefaultConnections: map[string]string{
				"google_cloud_platform": "test-connection",
			},
		}

		queryExtractor.On("ExtractQueriesFromString", "SELECT 1; SELECT 2;").Return([]*query.Query{
			{Query: "SELECT 1"},
			{Query: "SELECT 2"},
		}, nil)
		connGetter.On("GetConnection", "test-connection").Return("not-a-bigquery-client")

		dryRunner := &DryRunner{
			ConnectionGetter: connGetter,
			QueryExtractor:   queryExtractor,
		}

		result, err := dryRunner.DryRun(context.Background(), *pipeline, asset, &config.Config{})

		require.Error(t, err)
		assert.Contains(t, err.Error(), "resolved connection is not BigQuery")
		assert.Nil(t, result)

		connGetter.AssertExpectations(t)
		queryExtractor.AssertExpectations(t)
	})
}
