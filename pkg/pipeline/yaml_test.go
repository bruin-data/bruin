package pipeline_test

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestCreateTaskFromYamlDefinition(t *testing.T) {
	t.Parallel()

	type args struct {
		filePath string
	}

	tests := []struct {
		name    string
		args    args
		want    *pipeline.Asset
		wantErr bool
		err     error
	}{
		{
			name: "fails for paths that do not exist",
			args: args{
				filePath: "testdata/some/path/that/doesnt/exist",
			},
			wantErr: true,
		},
		{
			name: "fails for non-yaml files",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "task1", "hello.sql"),
			},
			wantErr: true,
		},
		{
			name: "reads a valid simple file",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "task1", "task.yml"),
			},
			want: &pipeline.Asset{
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bq.sql",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sql",
					Path:    path.AbsPathForTests(t, filepath.Join("testdata", "yaml", "task1", "hello.sql")),
					Content: mustRead(t, filepath.Join("testdata", "yaml", "task1", "hello.sql")),
				},
				Parameters: pipeline.ParameterMap{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				Upstreams: []pipeline.Upstream{
					{
						Type:    "asset",
						Value:   "gcs-to-bq",
						Columns: make([]pipeline.DependsColumn, 0),
						Mode:    pipeline.UpstreamModeFull,
					},
				},
				Materialization: pipeline.Materialization{
					Type:                 pipeline.MaterializationTypeTable,
					Strategy:             pipeline.MaterializationStrategyCreateReplace,
					ClusterBy:            []string{"key1", "key2"},
					PartitionBy:          "dt",
					IncrementalKey:       "dt",
					IncrementalPredicate: "target.dt >= DATE '2026-07-01'",
				},
				Hooks: pipeline.Hooks{
					Pre:  []pipeline.Hook{{Query: "select 1"}},
					Post: []pipeline.Hook{{Query: "select 2"}},
				},
				CustomChecks: make([]pipeline.CustomCheck, 0),
				Columns: []pipeline.Column{
					{
						Name:        "col1",
						Description: "column one",
						Type:        "string",
						Upstreams:   make([]*pipeline.UpstreamColumn, 0),
						Checks: []pipeline.ColumnCheck{
							{
								ID:   "1cde48c4bee4ad881c5d315dbfd136c708bfe4522ca7b74997017302b38ba763",
								Name: "unique",
							},
							{
								ID:   "7838b56dee5c090c3c04c356c8c1c249be0830efb1df36eb427b91eeb905875a",
								Name: "not_null",
							},
							{
								ID:   "8656942c1be105de34e9c1f500ad64b34f2c0ecf31e762ac6319e6b8cf9bbdcd",
								Name: "accepted_values",
								Value: pipeline.ColumnCheckValue{
									StringArray: &[]string{"a", "b", "c"},
								},
							},
							{
								ID:   "ea4d2fa734a840d95f0d1a65cfc451d8f8e9d121ea972c5d2c6bf365a6a24b74",
								Name: "min",
								Value: pipeline.ColumnCheckValue{
									Int: &[]int{3}[0],
								},
							},
						},
					},
					{
						Name:        "col2",
						Description: "column two",
						Checks:      []pipeline.ColumnCheck{},
						Upstreams:   make([]*pipeline.UpstreamColumn, 0),
					},
				},
			},
		},
		{
			name: "nested runfile paths work correctly",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "task-with-nested", "task.yml"),
			},
			want: &pipeline.Asset{
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sh",
					Path:    path.AbsPathForTests(t, filepath.Join("testdata", "yaml", "task-with-nested", "some", "dir", "hello.sh")),
					Content: mustRead(t, filepath.Join("testdata", "yaml", "task-with-nested", "some", "dir", "hello.sh")),
				},
				Parameters: pipeline.ParameterMap{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				Upstreams: []pipeline.Upstream{
					{
						Type:    "asset",
						Value:   "gcs-to-bq",
						Columns: make([]pipeline.DependsColumn, 0),
						Mode:    pipeline.UpstreamModeFull,
					},
				},
				Columns:      make([]pipeline.Column, 0),
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "top-level runfile paths are still joined correctly",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "task-with-toplevel-runfile", "task.yml"),
			},
			want: &pipeline.Asset{
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "hello.sh",
					Path:    path.AbsPathForTests(t, filepath.Join("testdata", "yaml", "task-with-toplevel-runfile", "hello.sh")),
					Content: mustRead(t, filepath.Join("testdata", "yaml", "task-with-toplevel-runfile", "hello.sh")),
				},
				Parameters: pipeline.ParameterMap{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				Upstreams: []pipeline.Upstream{
					{
						Type:    "asset",
						Value:   "gcs-to-bq",
						Columns: make([]pipeline.DependsColumn, 0),
						Mode:    pipeline.UpstreamModeFull,
					},
				},
				Columns:      make([]pipeline.Column, 0),
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "the ones with missing runfile are ignored",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "task-with-no-runfile", "task.yml"),
			},
			want: &pipeline.Asset{
				ID:          "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
				Name:        "hello-world",
				Description: "This is a hello world task",
				Type:        "bash",
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "task.yml",
					Path:    path.AbsPathForTests(t, filepath.Join("testdata", "yaml", "task-with-no-runfile", "task.yml")),
					Content: mustReadWithoutReplacement(t, filepath.Join("testdata", "yaml", "task-with-no-runfile", "task.yml")),
				},
				Parameters: pipeline.ParameterMap{
					"param1": "value1",
					"param2": "value2",
				},
				Connection: "conn1",
				Secrets:    []pipeline.SecretMapping{},
				Upstreams: []pipeline.Upstream{
					{
						Type:    "asset",
						Value:   "gcs-to-bq",
						Columns: make([]pipeline.DependsColumn, 0),
						Mode:    pipeline.UpstreamModeFull,
					},
				},
				Columns:      make([]pipeline.Column, 0),
				CustomChecks: make([]pipeline.CustomCheck, 0),
			},
		},
		{
			name: "depends can be a single string",
			args: args{
				filePath: filepath.Join("testdata", "yaml", "random-structure", "task.yml"),
			},
			want: &pipeline.Asset{
				ID:           "afa27b44d43b02a9fea41d13cedc2e4016cfcf87c5dbf990e593669aa8ce286d",
				Name:         "hello-world",
				Type:         "type1",
				Secrets:      []pipeline.SecretMapping{},
				Columns:      []pipeline.Column{},
				CustomChecks: []pipeline.CustomCheck{},
				ExecutableFile: pipeline.ExecutableFile{
					Name:    "task.yml",
					Path:    path.AbsPathForTests(t, filepath.Join("testdata", "yaml", "random-structure", "task.yml")),
					Content: mustRead(t, filepath.Join("testdata", "yaml", "random-structure", "task.yml")) + "\n",
				},
				Upstreams: []pipeline.Upstream{
					{
						Type:    "asset",
						Value:   "task1",
						Columns: []pipeline.DependsColumn{},
						Mode:    pipeline.UpstreamModeFull,
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			creator := pipeline.CreateTaskFromYamlDefinition(afero.NewOsFs())
			got, err := creator(tt.args.filePath)
			if tt.wantErr {
				require.Error(t, err)
				if tt.err != nil {
					require.EqualError(t, err, tt.err.Error())
				}
			} else {
				require.NoError(t, err)
			}

			if got != nil {
				got.ExecutableFile.Content = strings.ReplaceAll(got.ExecutableFile.Content, "\r\n", "\n")
			}
			if tt.want != nil {
				tt.want.ExecutableFile.Content = strings.ReplaceAll(tt.want.ExecutableFile.Content, "\r\n", "\n")
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestUpstreams(t *testing.T) {
	t.Parallel()

	// Create the task from the YAML definition
	creator := pipeline.CreateTaskFromYamlDefinition(afero.NewOsFs())
	got, err := creator(filepath.Join("testdata", "yaml", "upstream.yml"))
	require.NoError(t, err)

	// Normalize the line endings in the actual content
	got.ExecutableFile.Content = strings.ReplaceAll(got.ExecutableFile.Content, "\r\n", "\n")

	// Define the expected result, normalizing the content
	expected := &pipeline.Asset{
		ID:           "5e51ec24663355d3b76b287f2c5eca1bfa17ac01da6134dbd1251c3b6ee99b56",
		Name:         "upstream.something",
		Secrets:      make([]pipeline.SecretMapping, 0),
		Columns:      make([]pipeline.Column, 0),
		CustomChecks: make([]pipeline.CustomCheck, 0),
		ExecutableFile: pipeline.ExecutableFile{
			Name:    "upstream.yml",
			Path:    path.AbsPathForTests(t, filepath.Join("testdata", "yaml", "upstream.yml")),
			Content: strings.ReplaceAll(mustRead(t, filepath.Join("testdata", "yaml", "upstream.yml")), "\r\n", "\n"),
		},
		Upstreams: []pipeline.Upstream{
			{
				Type:    "asset",
				Value:   "some_asset",
				Columns: make([]pipeline.DependsColumn, 0),
				Mode:    pipeline.UpstreamModeFull,
			},
			{
				Type:    "uri",
				Value:   "bigquery://project.database/schema",
				Columns: make([]pipeline.DependsColumn, 0),
				Mode:    pipeline.UpstreamModeFull,
			},
			{
				Type:    "asset",
				Value:   "some_other_asset",
				Columns: make([]pipeline.DependsColumn, 0),
				Mode:    pipeline.UpstreamModeFull,
			},
			{
				Type:    "asset",
				Value:   "other_asset",
				Columns: []pipeline.DependsColumn{{Name: "col1", Usage: ""}, {Name: "col2", Usage: ""}},
				Mode:    pipeline.UpstreamModeFull,
			},
			{
				Type:    "asset",
				Value:   "other_asset2",
				Columns: []pipeline.DependsColumn{{Name: "col3", Usage: ""}, {Name: "col4", Usage: "CLAUSE"}},
				Mode:    pipeline.UpstreamModeFull,
			},
			{
				Type:    "asset",
				Value:   "yet_another_asset",
				Columns: []pipeline.DependsColumn{{Name: "col5", Usage: ""}, {Name: "col6", Usage: "CLAUSE"}},
				Mode:    pipeline.UpstreamModeFull,
			},
		},
	}

	// Compare the expected and actual results
	require.Equal(t, expected, got)
}

func TestConvertYamlToTask_FullRefreshRestrictedAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content string
	}{
		{
			name: "existing refresh_restricted field",
			content: `
name: dataset.asset
type: duckdb.sql
refresh_restricted: true
`,
		},
		{
			name: "full_refresh_restricted alias",
			content: `
name: dataset.asset
type: duckdb.sql
full_refresh_restricted: true
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			task, err := pipeline.ConvertYamlToTask([]byte(strings.TrimSpace(tt.content)))
			require.NoError(t, err)
			require.NotNil(t, task.RefreshRestricted)
			require.True(t, *task.RefreshRestricted)
		})
	}
}

func TestCreateTaskFromFileComments_FullRefreshRestrictedAlias(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	err := afero.WriteFile(fs, "asset.sql", []byte(strings.TrimSpace(`
-- @bruin.name: dataset.asset
-- @bruin.type: duckdb.sql
-- @bruin.full_refresh_restricted: true
select 1
`)), 0o644)
	require.NoError(t, err)

	creator := pipeline.CreateTaskFromFileComments(fs)
	task, err := creator("asset.sql")
	require.NoError(t, err)
	require.NotNil(t, task.RefreshRestricted)
	require.True(t, *task.RefreshRestricted)
}

func TestConvertYamlToTask_Routing(t *testing.T) {
	t.Parallel()

	task, err := pipeline.ConvertYamlToTask([]byte(strings.TrimSpace(`
name: dataset.asset
type: python
routing:
  egress_gateway: wg-shared-ams3
`)))
	require.NoError(t, err)
	require.Equal(t, &pipeline.RoutingConfig{EgressGateway: "wg-shared-ams3"}, task.Routing)
}

func TestConvertYamlToTask_Timeout(t *testing.T) {
	t.Parallel()

	task, err := pipeline.ConvertYamlToTask([]byte(strings.TrimSpace(`
name: dataset.asset
type: python
timeout: 1h30m
`)))
	require.NoError(t, err)
	require.Equal(t, 90*time.Minute, task.Timeout.Duration())

	content, err := task.FormatContent()
	require.NoError(t, err)
	require.Contains(t, string(content), "timeout: 1h30m")
}

func TestConvertYamlToTask_InvalidTimeout(t *testing.T) {
	t.Parallel()

	_, err := pipeline.ConvertYamlToTask([]byte(strings.TrimSpace(`
name: dataset.asset
type: python
timeout: ninety minutes
`)))
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot unmarshal")
}

func TestConvertYamlToTask_Enabled(t *testing.T) {
	t.Parallel()

	task, err := pipeline.ConvertYamlToTask([]byte(strings.TrimSpace(`
name: dataset.asset
type: python
enabled: false
`)))
	require.NoError(t, err)
	require.NotNil(t, task.Enabled)
	require.False(t, task.IsEnabled())

	templatedTask, err := pipeline.ConvertYamlToTask([]byte(strings.TrimSpace(`
name: dataset.templated_asset
type: python
enabled: "{{ var.asset_enabled }}"
`)))
	require.NoError(t, err)
	require.NotNil(t, templatedTask.Enabled)
	require.Equal(t, "{{ var.asset_enabled }}", templatedTask.Enabled.Template)

	defaultTask, err := pipeline.ConvertYamlToTask([]byte(strings.TrimSpace(`
name: dataset.default_asset
type: python
`)))
	require.NoError(t, err)
	require.Nil(t, defaultTask.Enabled)
	require.True(t, defaultTask.IsEnabled())
}

func TestConvertYamlToTask_SourceColumn(t *testing.T) {
	t.Parallel()

	task, err := pipeline.ConvertYamlToTask([]byte(strings.TrimSpace(`
name: dataset.contacts
type: ingestr
columns:
  - name: first_name
    source_column: fname
    type: string
    mask: hash
  - name: email
    type: string
	`)))
	require.NoError(t, err)
	require.Len(t, task.Columns, 2)
	require.Equal(t, "first_name", task.Columns[0].Name)
	require.Equal(t, "fname", task.Columns[0].SourceColumn)
	require.Equal(t, "hash", task.Columns[0].Mask)
	require.Empty(t, task.Columns[1].SourceColumn)
	require.Empty(t, task.Columns[1].Mask)
}

func TestConvertYamlToTask_ColumnMetadata(t *testing.T) {
	t.Parallel()

	task, err := pipeline.ConvertYamlToTask([]byte(strings.TrimSpace(`
name: dataset.orders
type: bq.sql
columns:
  - name: id
    type: integer
    primary_key: true
  - name: customer_id
    type: integer
    foreign_key:
      table: customers
      column: id
  - name: amount
    type: numeric
    precision: 10
    scale: 2
    default: "0"
  - name: name
    type: varchar
    length: 255
    collation: en_US
`)))
	require.NoError(t, err)
	require.Len(t, task.Columns, 4)

	require.Nil(t, task.Columns[0].ForeignKey)

	require.Equal(t, &pipeline.ColumnReference{Table: "customers", Column: "id"}, task.Columns[1].ForeignKey)

	require.Equal(t, 10, *task.Columns[2].Precision)
	require.Equal(t, 2, *task.Columns[2].Scale)
	require.Equal(t, "0", task.Columns[2].Default)

	require.Equal(t, 255, *task.Columns[3].Length)
	require.Equal(t, "en_US", task.Columns[3].Collation)
}

func TestCreateTaskFromFileComments_Routing(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	err := afero.WriteFile(fs, "asset.sql", []byte(strings.TrimSpace(`
-- @bruin.name: dataset.asset
-- @bruin.type: duckdb.sql
-- @bruin.routing.egress_gateway: wg-shared-ams3
select 1
`)), 0o644)
	require.NoError(t, err)

	creator := pipeline.CreateTaskFromFileComments(fs)
	task, err := creator("asset.sql")
	require.NoError(t, err)
	require.Equal(t, &pipeline.RoutingConfig{EgressGateway: "wg-shared-ams3"}, task.Routing)
}

func TestCreateTaskFromFileComments_Enabled(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	err := afero.WriteFile(fs, "asset.sql", []byte(strings.TrimSpace(`
-- @bruin.name: dataset.asset
-- @bruin.type: duckdb.sql
-- @bruin.enabled: false
select 1
`)), 0o644)
	require.NoError(t, err)

	creator := pipeline.CreateTaskFromFileComments(fs)
	task, err := creator("asset.sql")
	require.NoError(t, err)
	require.NotNil(t, task.Enabled)
	require.False(t, task.IsEnabled())

	err = afero.WriteFile(fs, "templated.sql", []byte(strings.TrimSpace(`
-- @bruin.name: dataset.templated_asset
-- @bruin.type: duckdb.sql
-- @bruin.enabled: {{ var.asset_enabled }}
select 1
`)), 0o644)
	require.NoError(t, err)

	templatedTask, err := creator("templated.sql")
	require.NoError(t, err)
	require.NotNil(t, templatedTask.Enabled)
	require.Equal(t, "{{ var.asset_enabled }}", templatedTask.Enabled.Template)
}

func TestCheckRetries(t *testing.T) {
	t.Parallel()

	creator := pipeline.CreateTaskFromYamlDefinition(afero.NewOsFs())
	got, err := creator(filepath.Join("testdata", "yaml", "check-retries", "task.yml"))
	require.NoError(t, err)

	// asset-level retries is parsed from the definition.
	require.NotNil(t, got.Retries)
	require.Equal(t, 4, *got.Retries)

	require.Len(t, got.Columns, 1)
	checks := got.Columns[0].Checks
	require.Len(t, checks, 2)

	// "unique" has an explicit retries override.
	require.Equal(t, "unique", checks[0].Name)
	require.NotNil(t, checks[0].Retries)
	require.Equal(t, 2, *checks[0].Retries)

	// "not_null" has no retries; it stays nil so consumers can default to the asset value.
	require.Equal(t, "not_null", checks[1].Name)
	require.Nil(t, checks[1].Retries)

	require.Len(t, got.CustomChecks, 2)
	require.NotNil(t, got.CustomChecks[0].Retries)
	require.Equal(t, 3, *got.CustomChecks[0].Retries)
	require.Nil(t, got.CustomChecks[1].Retries)
}

func TestUnitTestsParsing(t *testing.T) {
	t.Parallel()

	content := []byte(strings.TrimSpace(`
name: analytics.daily_revenue
type: duckdb.sql
materialization:
  type: table
unit_tests:
  - name: refunds_excluded_from_revenue
    description: refunded orders must not count toward revenue
    inputs:
      - asset: analytics.orders
        rows:
          - {id: 1, status: paid, amount: 100}
          - {id: 2, status: refunded, amount: 999}
    expected:
      rows:
        - {revenue: 100}
      match: subset
  - name: row_count_is_one
    fixtures: [base_orders]
    inputs:
      - asset: analytics.orders
        rows:
          - {id: 1}
    expected:
      count: 1
`))

	got, err := pipeline.ConvertYamlToTask(content)
	require.NoError(t, err)
	require.Len(t, got.UnitTests, 2)

	first := got.UnitTests[0]
	require.Equal(t, "refunds_excluded_from_revenue", first.Name)
	require.Equal(t, "refunded orders must not count toward revenue", first.Description)

	require.Len(t, first.Inputs, 1)
	require.Equal(t, "analytics.orders", first.Inputs[0].Asset)
	require.Len(t, first.Inputs[0].Rows, 2)
	require.Equal(t, "paid", first.Inputs[0].Rows[0]["status"])
	require.Equal(t, "refunded", first.Inputs[0].Rows[1]["status"])

	require.Len(t, first.Expected.Rows, 1)
	require.Equal(t, "subset", first.Expected.Match)

	second := got.UnitTests[1]
	require.Equal(t, "row_count_is_one", second.Name)
	require.Equal(t, []string{"base_orders"}, second.Fixtures)
	require.NotNil(t, second.Expected.Count)
	require.Equal(t, int64(1), *second.Expected.Count)
}

func TestUnitTestsParsing_CTEsAndExecutionTime(t *testing.T) {
	t.Parallel()

	content := []byte(strings.TrimSpace(`
name: analytics.report
type: duckdb.sql
unit_tests:
  - name: with_cte_and_frozen_time
    execution_time: "2023-01-01 12:05:03"
    inputs:
      - asset: analytics.orders
        rows:
          - {id: 1, status: paid}
    expected:
      ctes:
        paid:
          rows:
            - {id: 1}
          match: exact
      rows:
        - {id: 1}
`))

	got, err := pipeline.ConvertYamlToTask(content)
	require.NoError(t, err)
	require.Len(t, got.UnitTests, 1)

	ut := got.UnitTests[0]
	require.Equal(t, "2023-01-01 12:05:03", ut.ExecutionTime)
	require.Contains(t, ut.Expected.CTEs, "paid")
	require.Equal(t, "exact", ut.Expected.CTEs["paid"].Match)
	require.Len(t, ut.Expected.CTEs["paid"].Rows, 1)
	require.EqualValues(t, 1, ut.Expected.CTEs["paid"].Rows[0]["id"])
}

func TestPipelineFixturesParsing(t *testing.T) {
	t.Parallel()

	// A pipeline-level fixtures: block. Strict decoding mirrors what `bruin
	// validate` runs over pipeline.yml, so this also proves the new key is not
	// flagged as unknown.
	content := []byte(strings.TrimSpace(`
name: analytics
fixtures:
  - name: base_orders
    asset: analytics.orders
    rows:
      - {id: 1, status: paid, amount: 100}
      - {id: 2, status: refunded, amount: 999}
`))

	var pl pipeline.Pipeline
	require.NoError(t, path.ConvertYamlToObjectStrict(content, &pl))
	require.Len(t, pl.Fixtures, 1)
	require.Equal(t, "base_orders", pl.Fixtures[0].Name)
	require.Equal(t, "analytics.orders", pl.Fixtures[0].Asset)
	require.Len(t, pl.Fixtures[0].Rows, 2)
	require.Equal(t, "paid", pl.Fixtures[0].Rows[0]["status"])
}

func TestValidateAssetYAML_UnitTests(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()

	valid := strings.TrimSpace(`
/* @bruin
name: analytics.daily_revenue
type: duckdb.sql
unit_tests:
  - name: row_count_is_one
    fixtures: [base_orders]
    execution_time: "2023-01-01 12:05:03"
    inputs:
      - asset: analytics.orders
        rows:
          - {id: 1}
    expected:
      count: 1
      ctes:
        paid:
          rows:
            - {id: 1}
@bruin */

SELECT 1
`)
	require.NoError(t, afero.WriteFile(fs, "valid.sql", []byte(valid), 0o644))

	// `bruin validate` strict-decodes the @bruin block; the new unit_tests key must be accepted.
	require.NoError(t, pipeline.ValidateAssetYAML(fs, "valid.sql", pipeline.CommentTask))

	invalid := strings.TrimSpace(`
/* @bruin
name: analytics.daily_revenue
type: duckdb.sql
unit_tests:
  - name: row_count_is_one
    not_a_real_field: oops
    expected:
      count: 1
@bruin */

SELECT 1
`)
	require.NoError(t, afero.WriteFile(fs, "invalid.sql", []byte(invalid), 0o644))

	// Strict validation also covers the nested unit-test schema, so typos are caught.
	err := pipeline.ValidateAssetYAML(fs, "invalid.sql", pipeline.CommentTask)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not_a_real_field")
}

func TestConvertYamlToTask_Hooks(t *testing.T) {
	t.Parallel()

	type expectation struct {
		pre  []pipeline.Hook
		post []pipeline.Hook
	}

	tests := []struct {
		name    string
		content string
		want    expectation
		wantErr bool
	}{
		{
			name: "pre hooks only",
			content: `
hooks:
  pre:
    - query: "select 1"
`,
			want: expectation{
				pre: []pipeline.Hook{{Query: "select 1"}},
			},
		},
		{
			name: "post hooks only",
			content: `
hooks:
  post:
    - query: "select 2"
`,
			want: expectation{
				post: []pipeline.Hook{{Query: "select 2"}},
			},
		},
		{
			name:    "no hooks",
			content: ``,
			want:    expectation{},
		},
		{
			name: "invalid hooks shape",
			content: `
hooks:
  - query: "select 1"
`,
			wantErr: true,
		},
		{
			name: "invalid hook entry type",
			content: `
hooks:
  pre:
    - "select 1"
`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := strings.TrimSpace(tt.content)
			if body != "" {
				body = "\n" + body
			}
			content := []byte(strings.TrimSpace("name: dataset.player_stats\ntype: duckdb.sql" + body))

			task, err := pipeline.ConvertYamlToTask(content)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want.pre, task.Hooks.Pre)
			require.Equal(t, tt.want.post, task.Hooks.Post)
		})
	}
}
