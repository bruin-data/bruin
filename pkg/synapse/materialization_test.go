package synapse

import (
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
		want        []string
		wantErr     bool
		fullRefresh bool
	}{
		{
			name:  "no materialization, return raw query",
			task:  &pipeline.Asset{},
			query: "SELECT 1",
			want:  []string{"SELECT 1"},
		},
		{
			name: "materialize to a view",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeView,
				},
			},
			query: "SELECT 1",
			want: []string{
				"^DROP VIEW IF EXISTS my\\.asset;$",
				"^CREATE VIEW my\\.asset AS SELECT 1;$",
			},
		},
		{
			name: "materialize to a table, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			query: "SELECT 1",
			want: []string{"^SELECT tmp\\.\\* INTO #bruin_tmp_.+ FROM \\(SELECT 1\\) AS tmp;\n" +
				"IF OBJECT_ID\\('my\\.asset', 'U'\\) IS NOT NULL DROP TABLE my\\.asset;\n" +
				"SELECT \\* INTO my\\.asset FROM #bruin_tmp_.+;\n" +
				"DROP TABLE #bruin_tmp_.+;$"},
		},
		{
			name: "materialize to a table with cluster is unsupported",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:      pipeline.MaterializationTypeTable,
					Strategy:  pipeline.MaterializationStrategyCreateReplace,
					ClusterBy: []string{"event_type", "event_name"},
				},
			},
			query:   "SELECT 1",
			want:    []string{},
			wantErr: true,
		},
		{
			name: "materialize to a table with append and full refresh results in create_replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query:       "SELECT 1",
			fullRefresh: true,
			want: []string{"^SELECT tmp\\.\\* INTO #bruin_tmp_.+ FROM \\(SELECT 1\\) AS tmp;\n" +
				"IF OBJECT_ID\\('my\\.asset', 'U'\\) IS NOT NULL DROP TABLE my\\.asset;\n" +
				"SELECT \\* INTO my\\.asset FROM #bruin_tmp_.+;\n" +
				"DROP TABLE #bruin_tmp_.+;$"},
		},
		{
			name: "incremental strategies require the incremental_key to be set",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "incremental strategies require the incremental_key to be set",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyDeleteInsert,
				},
			},
			query:   "SELECT 1",
			wantErr: true,
		},
		{
			name: "delete+insert builds a proper transaction",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1",
			want: []string{
				"^SELECT alias\\.\\* INTO __bruin_tmp_.+ AS alias;$",
				"^DELETE FROM my\\.asset WHERE dt in \\(SELECT DISTINCT dt FROM __bruin_tmp_.+\\);$",
				"^INSERT INTO my\\.asset SELECT \\* FROM __bruin_tmp_.+;$",
				"^DROP TABLE __bruin_tmp_.+;$",
			},
		},
		{
			name: "merge without columns",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{},
			},
			query:   "SELECT 1 as id",
			wantErr: true,
		},
		{
			name: "merge without primary keys",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int"},
				},
			},
			query:   "SELECT 1 as id",
			wantErr: true,
		},
		{
			name: "merge with primary keys",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
				Columns: []pipeline.Column{
					{Name: "id", Type: "int", PrimaryKey: true},
					{Name: "name", Type: "varchar", PrimaryKey: false, UpdateOnMerge: true},
				},
			},
			query: "SELECT 1 as id, 'abc' as name",
			want: []string{"^MERGE INTO my\\.asset target\n" +
				"USING \\(SELECT 1 as id, 'abc' as name\\) source ON target\\.id = source.id\n" +
				"WHEN MATCHED THEN UPDATE SET target\\.name = source\\.name\n" +
				"WHEN NOT MATCHED THEN INSERT\\(id, name\\) VALUES\\(id, name\\);$"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := NewMaterializer(tt.fullRefresh)
			render, err := m.Render(tt.task, tt.query)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			for index, want := range tt.want {
				assert.Regexp(t, want, render[index])
			}
		})
	}
}
