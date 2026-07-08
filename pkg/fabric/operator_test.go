package fabric

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockFabricExtractor struct {
	mock.Mock
}

func (m *mockFabricExtractor) ExtractQueriesFromString(content string) ([]*query.Query, error) {
	res := m.Called(content)
	return res.Get(0).([]*query.Query), res.Error(1)
}

func (m *mockFabricExtractor) CloneForAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) (query.QueryExtractor, error) {
	res := m.Called(ctx, p, asset)
	return res.Get(0).(query.QueryExtractor), res.Error(1)
}

func (m *mockFabricExtractor) ReextractQueriesFromSlice(content []string) ([]string, error) {
	res := m.Called(content)
	return res.Get(0).([]string), res.Error(1)
}

type mockFabricMaterializer struct {
	mock.Mock
}

func (m *mockFabricMaterializer) Render(asset *pipeline.Asset, query string) (string, error) {
	res := m.Called(asset, query)
	return res.Get(0).(string), res.Error(1)
}

type mockFabricClient struct {
	mock.Mock
}

func (m *mockFabricClient) CreateSchemaIfNotExist(ctx context.Context, asset *pipeline.Asset) error {
	res := m.Called(ctx, asset)
	return res.Error(0)
}

func (m *mockFabricClient) RunQueryWithoutResult(ctx context.Context, q *query.Query) error {
	res := m.Called(ctx, q)
	return res.Error(0)
}

type mockFabricConnectionGetter struct {
	mock.Mock
}

func (m *mockFabricConnectionGetter) GetConnection(name string) any {
	res := m.Called(name)
	return res.Get(0)
}

func TestBasicOperator_RunTaskCreatesSchemaForMaterializedAsset(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "bruin_test.Products",
		Type: pipeline.AssetTypeFabricQuery,
		Materialization: pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		},
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "products.sql",
			Content: "SELECT 1",
		},
	}
	renderedQuery := "CREATE TABLE [bruin_test].[Products] AS\nSELECT 1"

	extractor := new(mockFabricExtractor)
	extractor.On("CloneForAsset", mock.Anything, mock.Anything, asset).Return(extractor, nil)
	extractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{{Query: "SELECT 1"}}, nil)

	materializer := new(mockFabricMaterializer)
	materializer.On("Render", asset, "SELECT 1").Return(renderedQuery, nil)

	client := new(mockFabricClient)
	client.On("CreateSchemaIfNotExist", mock.Anything, asset).Return(nil).Once()
	client.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: renderedQuery}).Return(nil).Once()

	connections := new(mockFabricConnectionGetter)
	connections.On("GetConnection", "fabric-default").Return(client)

	operator := BasicOperator{
		connection:   connections,
		extractor:    extractor,
		materializer: materializer,
	}

	err := operator.RunTask(t.Context(), &pipeline.Pipeline{}, asset)

	require.NoError(t, err)
	client.AssertExpectations(t)
}

func TestBasicOperator_RunTaskSkipsSchemaForNonMaterializedAsset(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "bruin_test.Products",
		Type: pipeline.AssetTypeFabricQuery,
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "products.sql",
			Content: "SELECT 1",
		},
	}

	extractor := new(mockFabricExtractor)
	extractor.On("CloneForAsset", mock.Anything, mock.Anything, asset).Return(extractor, nil)
	extractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{{Query: "SELECT 1"}}, nil)

	materializer := new(mockFabricMaterializer)
	materializer.On("Render", asset, "SELECT 1").Return("SELECT 1", nil)

	client := new(mockFabricClient)
	client.On("RunQueryWithoutResult", mock.Anything, &query.Query{Query: "SELECT 1"}).Return(nil).Once()

	connections := new(mockFabricConnectionGetter)
	connections.On("GetConnection", "fabric-default").Return(client)

	operator := BasicOperator{
		connection:   connections,
		extractor:    extractor,
		materializer: materializer,
	}

	err := operator.RunTask(t.Context(), &pipeline.Pipeline{}, asset)

	require.NoError(t, err)
	client.AssertNotCalled(t, "CreateSchemaIfNotExist", mock.Anything, asset)
	client.AssertExpectations(t)
}

func TestBasicOperator_RunTaskReturnsSchemaCreationError(t *testing.T) {
	t.Parallel()

	asset := &pipeline.Asset{
		Name: "bruin_test.Products",
		Type: pipeline.AssetTypeFabricQuery,
		Materialization: pipeline.Materialization{
			Type: pipeline.MaterializationTypeTable,
		},
		ExecutableFile: pipeline.ExecutableFile{
			Path:    "products.sql",
			Content: "SELECT 1",
		},
	}

	extractor := new(mockFabricExtractor)
	extractor.On("CloneForAsset", mock.Anything, mock.Anything, asset).Return(extractor, nil)
	extractor.On("ExtractQueriesFromString", "SELECT 1").Return([]*query.Query{{Query: "SELECT 1"}}, nil)

	materializer := new(mockFabricMaterializer)
	materializer.On("Render", asset, "SELECT 1").Return("CREATE TABLE [bruin_test].[Products] AS\nSELECT 1", nil)

	client := new(mockFabricClient)
	client.On("CreateSchemaIfNotExist", mock.Anything, asset).Return(errors.New("schema failed")).Once()

	connections := new(mockFabricConnectionGetter)
	connections.On("GetConnection", "fabric-default").Return(client)

	operator := BasicOperator{
		connection:   connections,
		extractor:    extractor,
		materializer: materializer,
	}

	err := operator.RunTask(t.Context(), &pipeline.Pipeline{}, asset)

	require.ErrorContains(t, err, "schema failed")
	client.AssertNotCalled(t, "RunQueryWithoutResult", mock.Anything, mock.Anything)
	client.AssertExpectations(t)
}
