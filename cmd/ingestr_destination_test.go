package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bruin-data/bruin/pkg/lint"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticConnectionTypes map[string]string

func (s staticConnectionTypes) GetConnectionType(name string) string {
	return s[name]
}

func TestIngestrDestinationBuilderInfersAfterVariantRendering(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, ".git"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "assets"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "pipeline.yml"), []byte(`name: "{{ var.client }}_pipe"
variables:
  client:
    type: string
    default: alpha
  dest:
    type: string
    default: warehouse
variants:
  alpha:
    client: alpha
    dest: warehouse
  beta:
    client: beta
    dest: crm
`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "assets", "push.asset.yml"), []byte(`name: contacts
type: ingestr
connection: "{{ var.dest }}"
parameters:
  source_connection: src
  source_table: main.people
  incremental_strategy: update
`), 0o600))

	connections := staticConnectionTypes{"warehouse": "duckdb", "crm": "hubspot"}

	tests := []struct {
		variant         string
		wantDestination string
		wantIssues      int
	}{
		// `update` is only valid for reverse-ETL destinations such as HubSpot.
		{variant: "beta", wantDestination: "hubspot", wantIssues: 0},
		{variant: "alpha", wantDestination: "duckdb", wantIssues: 1},
	}

	for _, tt := range tests {
		t.Run(tt.variant, func(t *testing.T) {
			t.Parallel()

			builder := &ingestrDestinationBuilder{
				inner:       &variantInjectingBuilder{inner: DefaultPipelineBuilder, variantName: tt.variant},
				connections: connections,
			}

			p, err := builder.CreatePipelineFromPath(t.Context(), filepath.Join(dir, "pipeline.yml"), pipeline.WithMutate())
			require.NoError(t, err)
			require.Len(t, p.Assets, 1)

			asset := p.Assets[0]
			destination, _ := asset.Parameters.GetString("destination")
			assert.Equal(t, tt.wantDestination, destination)

			issues, err := lint.EnsureIngestrAssetIsValidForASingleAsset(t.Context(), p, asset)
			require.NoError(t, err)
			assert.Len(t, issues, tt.wantIssues)
		})
	}
}
