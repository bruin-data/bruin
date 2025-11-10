package mysql

import (
	"strings"
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMaterializer_Render(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		task        *pipeline.Asset
		query       string
		want        string
		wantErr     bool
		fullRefresh bool
	}{
		{
			name:  "no materialization, return raw query",
			task:  &pipeline.Asset{},
			query: "SELECT 1",
			want:  "SELECT 1",
		},
		{
			name: "materialize to a view",
			task: &pipeline.Asset{
				Name: "my_view",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeView,
				},
			},
			query: "SELECT 1",
			want:  "CREATE OR REPLACE VIEW `my_view` AS\nSELECT 1",
		},
		{
			name: "materialize to a table with create-replace strategy",
			task: &pipeline.Asset{
				Name: "my_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyCreateReplace,
				},
			},
			query: "SELECT 1",
			want: `START TRANSACTION;
DROP TABLE IF EXISTS ` + "`my_table`" + `; 
CREATE TABLE ` + "`my_table`" + ` AS SELECT 1;
COMMIT;`,
		},
		{
			name: "materialize to a table with append strategy",
			task: &pipeline.Asset{
				Name: "my_table",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query: "SELECT 1",
			want:  "INSERT INTO `my_table` SELECT 1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := NewMaterializer(tt.fullRefresh)
			got, err := m.Render(tt.task, tt.query)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, strings.TrimSpace(tt.want), strings.TrimSpace(got))
		})
	}
}

func TestQuoteIdentifier(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		identifier string
		want       string
	}{
		{
			name:       "simple identifier",
			identifier: "table",
			want:       "`table`",
		},
		{
			name:       "schema.table identifier",
			identifier: "schema.table",
			want:       "`schema`.`table`",
		},
		{
			name:       "database.schema.table identifier",
			identifier: "database.schema.table",
			want:       "`database`.`schema`.`table`",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := QuoteIdentifier(tt.identifier)
			assert.Equal(t, tt.want, got)
		})
	}
}
