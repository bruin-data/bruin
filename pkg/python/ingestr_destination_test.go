package python

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
)

type mockConnectionTypes map[string]string

func (m mockConnectionTypes) GetConnectionType(name string) string {
	return m[name]
}

func TestInferIngestrDestination(t *testing.T) {
	t.Parallel()

	connections := mockConnectionTypes{
		"bigquery-prod": "google_cloud_platform",
		"pg":            "postgres",
		"hubspot-prod":  "hubspot",
		"sheets":        "google_sheets",
		"shopify-prod":  "shopify",
		"mysql-prod":    "mysql",
	}

	tests := []struct {
		name            string
		asset           *pipeline.Asset
		wantDestination string
		wantSet         bool
	}{
		{
			name: "infers bigquery from the asset connection",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeIngestr,
				Connection: "bigquery-prod",
				Parameters: pipeline.ParameterMap{"source_connection": "athena", "source_table": "events"},
			},
			wantDestination: "bigquery",
			wantSet:         true,
		},
		{
			name: "infers from destination_connection",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeIngestr,
				Parameters: pipeline.ParameterMap{"destination_connection": "pg"},
			},
			wantDestination: "postgres",
			wantSet:         true,
		},
		{
			name: "infers a reverse-ETL destination",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeIngestr,
				Connection: "hubspot-prod",
				Parameters: pipeline.ParameterMap{},
			},
			wantDestination: "hubspot",
			wantSet:         true,
		},
		{
			name: "infers gsheets from a google_sheets connection",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeIngestr,
				Connection: "sheets",
			},
			wantDestination: "gsheets",
			wantSet:         true,
		},
		{
			name: "keeps an explicit destination",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeIngestr,
				Connection: "bigquery-prod",
				Parameters: pipeline.ParameterMap{"destination": "gsheets"},
			},
			wantDestination: "gsheets",
			wantSet:         true,
		},
		{
			name: "leaves a connection type that is not an ingestr destination alone",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeIngestr,
				Connection: "shopify-prod",
				Parameters: pipeline.ParameterMap{},
			},
		},
		{
			name: "leaves an unknown connection alone",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeIngestr,
				Connection: "missing",
				Parameters: pipeline.ParameterMap{},
			},
		},
		{
			name: "leaves an asset without any connection alone",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeIngestr,
				Parameters: pipeline.ParameterMap{},
			},
		},
		{
			name: "ignores non-ingestr assets",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeBigqueryQuery,
				Connection: "bigquery-prod",
				Parameters: pipeline.ParameterMap{},
			},
		},
		{
			name: "infers the destination of an inference asset from its connection",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeInference,
				Connection: "pg",
				Parameters: pipeline.ParameterMap{"provider": "openai"},
			},
			wantDestination: "postgres",
			wantSet:         true,
		},
		{
			name: "overrides a destination that disagrees with the inference connection",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeInference,
				Connection: "bigquery-prod",
				Parameters: pipeline.ParameterMap{"destination": "postgres"},
			},
			wantDestination: "bigquery",
			wantSet:         true,
		},
		{
			name: "drops a user-set destination when the inference connection has none",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeInference,
				Connection: "mysql-prod",
				Parameters: pipeline.ParameterMap{"destination": "postgres"},
			},
		},
		{
			name: "leaves an inference asset without a connection alone",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeInference,
				Parameters: pipeline.ParameterMap{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &pipeline.Pipeline{Assets: []*pipeline.Asset{tt.asset}}
			InferIngestrDestination(tt.asset, p, connections)

			destination, ok := tt.asset.Parameters.GetString("destination")
			assert.Equal(t, tt.wantSet, ok)
			assert.Equal(t, tt.wantDestination, destination)
		})
	}
}

func TestIngestrDestinationsByConnectionTypeIsUnambiguous(t *testing.T) {
	t.Parallel()

	// Two destinations on one connection type would make the inference depend on map order.
	seen := map[string]string{}
	for destination, assetType := range pipeline.IngestrTypeConnectionMapping {
		connectionType, ok := pipeline.AssetTypeConnectionMapping[assetType]
		if !ok {
			continue
		}
		if other, exists := seen[connectionType]; exists {
			t.Errorf("connection type %q maps to both %q and %q", connectionType, other, destination)
		}
		seen[connectionType] = destination
	}
}

type countingConnectionTypes struct {
	types mockConnectionTypes
	calls map[string]int
}

func (c *countingConnectionTypes) GetConnectionType(name string) string {
	c.calls[name]++
	return c.types[name]
}

func TestInferIngestrDestinationsLooksUpEachConnectionOnce(t *testing.T) {
	t.Parallel()

	connections := &countingConnectionTypes{types: mockConnectionTypes{"pg": "postgres"}, calls: map[string]int{}}
	assets := []*pipeline.Asset{
		{Name: "a", Type: pipeline.AssetTypeIngestr, Connection: "pg"},
		{Name: "b", Type: pipeline.AssetTypeIngestr, Connection: "pg"},
		{Name: "c", Type: pipeline.AssetTypeIngestr, Connection: "pg"},
	}

	InferIngestrDestinations(&pipeline.Pipeline{Assets: assets}, connections)

	for _, asset := range assets {
		destination, _ := asset.Parameters.GetString("destination")
		assert.Equal(t, "postgres", destination)
	}
	assert.Equal(t, 1, connections.calls["pg"])
}
