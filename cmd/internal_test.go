package cmd

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func TestInternalParse_Run(t *testing.T) {
	t.Parallel()
	tests := []struct {
		pipeline     *pipeline.Pipeline
		beforeAssets *pipeline.Asset
		afterAssets  *pipeline.Asset
		err          error
		want         error
	}{
		{
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					&pipeline.Asset{
						Name: "employees",
						Columns: []pipeline.Column{
							{
								Name:       "id",
								Type:       "str",
								PrimaryKey: true,
							},
							{
								Name: "name",
								Type: "str",
							},
							{
								Name: "age",
								Type: "int64",
							},
						},
					},
				},
			},
			beforeAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: "select * from employees",
				},
				Upstreams: []pipeline.Upstream{
					{
						Value: "employees",
					},
				},
			},
			afterAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: "select * from employees",
				},
				Columns: []pipeline.Column{
					{
						Name:       "id",
						Type:       "str",
						PrimaryKey: true,
					},
					{
						Name: "name",
						Type: "str",
					},
					{
						Name: "age",
						Type: "int64",
					},
				},
				Upstreams: []pipeline.Upstream{
					{
						Value: "employees",
					},
				},
			},
			err:  nil,
			want: nil,
		},
		{
			pipeline: &pipeline.Pipeline{
				Assets: []*pipeline.Asset{
					&pipeline.Asset{
						Name: "table1",
						Columns: []pipeline.Column{
							{
								Name: "a",
								Type: "str",
							},
							{
								Name: "b",
								Type: "int64",
							},
						},
					},
					&pipeline.Asset{
						Name: "table2",
						Columns: []pipeline.Column{
							{
								Name: "a",
								Type: "str",
							},
							{
								Name: "c",
								Type: "str",
							},
						},
					},
				},
			},
			beforeAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: `
					with t1 as (
        select *
        from table1
        join table2
            using(a)
    ),
    t2 as (
        select *
        from table2
        left join table1
            using(a)
    )
    select t1.*, t2.b as b2, t2.c as c2
    from t1
    join t2
        using(a)
					`,
				},
				Columns: []pipeline.Column{},
				Upstreams: []pipeline.Upstream{
					{
						Value: "table1",
					},
					{
						Value: "table2",
					},
				},
			},
			afterAssets: &pipeline.Asset{
				Name: "example",
				ExecutableFile: pipeline.ExecutableFile{
					Content: `
					with t1 as (
        select *
        from table1
        join table2
            using(a)
    ),
    t2 as (
        select *
        from table2
        left join table1
            using(a)
    )
    select t1.*, t2.b as b2, t2.c as c2
    from t1
    join t2
        using(a)
					`,
				},
				Columns: []pipeline.Column{
					{
						Name: "a",
						Type: "str",
					},
					{
						Name: "b",
						Type: "int64",
					},
					{
						Name: "c",
						Type: "str",
					},
					{
						Name: "b2",
						Type: "int64",
					},
					{
						Name: "c2",
						Type: "str",
					},
				},
				Upstreams: []pipeline.Upstream{
					{
						Value: "table1",
					},
					{
						Value: "table2",
					},
				},
			},
			err:  nil,
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.beforeAssets.Name, func(t *testing.T) {
			err := ParseLineage(tt.pipeline, tt.beforeAssets)
			if err != tt.want {
				t.Errorf("ParseLineage() error = %v, want %v", err, tt.want)
			}

			if tt.afterAssets != nil {

				if len(tt.beforeAssets.Columns) != len(tt.afterAssets.Columns) {
					t.Errorf("Column count mismatch: got %d, want %d",
						len(tt.beforeAssets.Columns), len(tt.afterAssets.Columns))
				}

				for i, col := range tt.beforeAssets.Columns {
					wantCol := tt.afterAssets.Columns[i]
					if col.Name != wantCol.Name || col.Type != wantCol.Type {
						t.Errorf("Column mismatch at index %d: got %+v, want %+v",
							i, col, wantCol)
					}
				}

				if len(tt.beforeAssets.Upstreams) != len(tt.afterAssets.Upstreams) {
					t.Errorf("Upstream count mismatch: got %d, want %d",
						len(tt.beforeAssets.Upstreams), len(tt.afterAssets.Upstreams))
				}

				for i, upstream := range tt.beforeAssets.Upstreams {
					if upstream.Value != tt.afterAssets.Upstreams[i].Value {
						t.Errorf("Upstream mismatch at index %d: got %s, want %s",
							i, upstream.Value, tt.afterAssets.Upstreams[i].Value)
					}
				}
			}
		})
	}
}
