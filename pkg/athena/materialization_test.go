package athena

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
			want:  []string{"CREATE OR REPLACE VIEW my.asset AS\nSELECT 1"},
		},
		{
			name: "materialize to a table, no partition or cluster, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type: pipeline.MaterializationTypeTable,
				},
			},
			query: "SELECT 1",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS SELECT 1",
				"DROP TABLE IF EXISTS my.asset",
				"ALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset",
			},
		},
		{
			name: "materialize to a table, with partition, default to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:        pipeline.MaterializationTypeTable,
					PartitionBy: "some_column",
				},
			},
			query: "SELECT 1 as some_column",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi', partitioning = ARRAY['some_column']) AS SELECT 1 as some_column",
				"DROP TABLE IF EXISTS my.asset",
				"ALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset",
			},
		},
		{
			name: "materialize to a table, full refresh defaults to create+replace",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyMerge,
				},
			},
			fullRefresh: true,
			query:       "SELECT 1",
			want: []string{
				"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS SELECT 1",
				"DROP TABLE IF EXISTS my.asset",
				"ALTER TABLE __bruin_tmp_abcefghi RENAME TO my.asset",
			},
		},
		{
			name: "materialize to a table with append",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:     pipeline.MaterializationTypeTable,
					Strategy: pipeline.MaterializationStrategyAppend,
				},
			},
			query: "SELECT 1",
			want:  []string{"INSERT INTO my.asset SELECT 1"},
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
			name: "delete+insert",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1",
			want:  []string{"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS SELECT 1\n", "\nDELETE FROM my.asset WHERE dt in (SELECT DISTINCT dt FROM __bruin_tmp_abcefghi)", "INSERT INTO my.asset SELECT * FROM __bruin_tmp_abcefghi", "DROP TABLE IF EXISTS __bruin_tmp_abcefghi"},
		},
		{
			name: "delete+insert semicolon comment out",
			task: &pipeline.Asset{
				Name: "my.asset",
				Materialization: pipeline.Materialization{
					Type:           pipeline.MaterializationTypeTable,
					Strategy:       pipeline.MaterializationStrategyDeleteInsert,
					IncrementalKey: "dt",
				},
			},
			query: "SELECT 1" +
				" --this is a comment",
			want: []string{"CREATE TABLE __bruin_tmp_abcefghi WITH (table_type='ICEBERG', is_external=false, location='s3://bucket/__bruin_tmp_abcefghi') AS SELECT 1 --this is a comment\n", "\nDELETE FROM my.asset WHERE dt in (SELECT DISTINCT dt FROM __bruin_tmp_abcefghi)", "INSERT INTO my.asset SELECT * FROM __bruin_tmp_abcefghi", "DROP TABLE IF EXISTS __bruin_tmp_abcefghi"},
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
			want:  []string{"MERGE INTO my.asset target USING (SELECT 1 as id, 'abc' as name) source ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name WHEN NOT MATCHED THEN INSERT(id, name) VALUES(source.id, source.name)"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			m := NewMaterializer(tt.fullRefresh)
			render, err := m.Render(tt.task, tt.query, "s3://bucket")

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.want, render)
			}
		})
	}
}
