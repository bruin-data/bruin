package fabric

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockFabricSelector struct {
	mock.Mock
}

func (m *mockFabricSelector) Select(ctx context.Context, q *query.Query) ([][]interface{}, error) {
	args := m.Called(ctx, q)
	return args.Get(0).([][]interface{}), args.Error(1)
}

func TestRelationshipsCheckQuotesIdentifiers(t *testing.T) {
	t.Parallel()

	expectedQuery := &query.Query{Query: "SELECT COUNT_BIG(*) FROM [sales data].[order] bruin_relationship_child WHERE bruin_relationship_child.[select] IS NOT NULL AND bruin_relationship_child.[select] NOT IN (SELECT bruin_relationship_parent.[customer id] FROM [sales data].[customer-table] bruin_relationship_parent WHERE bruin_relationship_parent.[customer id] IS NOT NULL)"}
	selector := new(mockFabricSelector)
	selector.On("Select", mock.Anything, expectedQuery).Return([][]interface{}{{0}}, nil).Once()

	connections := new(mockFabricConnectionGetter)
	connections.On("GetConnection", "fabric-default").Return(selector).Once()

	instance := &scheduler.ColumnCheckInstance{
		AssetInstance: &scheduler.AssetInstance{
			Asset: &pipeline.Asset{
				Name: "sales data.order",
				Type: pipeline.AssetTypeFabricQuery,
			},
			Pipeline: &pipeline.Pipeline{
				Name: "test",
				DefaultConnections: map[string]string{
					"fabric": "fabric-default",
				},
			},
		},
		Column: &pipeline.Column{
			Name:       "select",
			ForeignKey: &pipeline.ColumnReference{Table: "sales data.customer-table", Column: "customer id"},
		},
		Check: &pipeline.ColumnCheck{Name: "relationships"},
	}

	require.NoError(t, (&RelationshipsCheck{conn: connections}).Check(t.Context(), instance))
	selector.AssertExpectations(t)
	connections.AssertExpectations(t)
}
