package query

import (
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockNoOpRenderer struct {
	mock.Mock
}

func (m *mockNoOpRenderer) Render(template string) (string, error) {
	args := m.Called(template)
	if args.Get(0) == "default" {
		return template, nil
	}

	return args.String(0), args.Error(1)
}

func TestFileExtractor_ExtractQueriesFromString(t *testing.T) {
	t.Parallel()

	noOpRenderer := func(mr renderer) {
		mr.(*mockNoOpRenderer).On("Render", mock.Anything).Return("default", nil)
	}

	tests := []struct {
		name          string
		setupRenderer func(mr renderer)
		content       string
		want          []*Query
		wantErr       bool
	}{
		{
			name:          "only variables, no query",
			content:       "set variable1 = asd; set variable2 = 123;",
			setupRenderer: noOpRenderer,
			want:          make([]*Query, 0),
		},
		{
			name:          "single query",
			content:       "select * from users;",
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: "select * from users",
				},
			},
		},
		{
			name:    "single query, rendered properly",
			content: "select * from users-{{ds}};",
			setupRenderer: func(mr renderer) {
				mr.(*mockNoOpRenderer).
					On("Render", mock.Anything).
					Return("select * from users-2022-01-01", nil)
			},
			want: []*Query{
				{
					Query: "select * from users-2022-01-01",
				},
			},
		},
		{
			name: "multiple queries, multiline",
			content: `select * from users;
		;;
									select name from countries;;
									`,
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: "select * from users",
				},
				{
					Query: "select name from countries",
				},
			},
		},
		{
			name: "multiple queries, multiline, starts with a comment",
			content: `
		-- here's some comment
		select * from users;
		;;
									select name from countries;;
									`,
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: "select * from users",
				},
				{
					Query: "select name from countries",
				},
			},
		},
		{
			name: "multiple queries, multiline, comments in the middle",
			content: `
		-- here's some comment
		select * from users;
		;;
		-- here's some other comment
			-- and a nested one event
/*
some random query between comments;
*/
		select name from countries;;
									`,
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: "select * from users",
				},
				{
					Query: "select name from countries",
				},
			},
		},
		{
			name: "multiple queries, multiline, variable definitions are collected",
			content: `
		-- here's some comment
set analysis_period_days = 21;
		select * from users;
		;;
set analysis_start_date = dateadd(days, -($analysis_period_days - 1), $analysis_end_date);
set min_level_req = 22;
		-- here's some other comment
			-- and a nested one event
		
		select name from countries;;
									`,
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					VariableDefinitions: []string{
						"set analysis_period_days = 21",
					},
					Query: "select * from users",
				},
				{
					VariableDefinitions: []string{
						"set analysis_period_days = 21",
						"set analysis_start_date = dateadd(days, -($analysis_period_days - 1), $analysis_end_date)",
						"set min_level_req = 22",
					},
					Query: "select name from countries",
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fs := afero.NewMemMapFs()
			mr := new(mockNoOpRenderer)
			if tt.setupRenderer != nil {
				tt.setupRenderer(mr)
			}

			f := FileQuerySplitterExtractor{
				Fs:       fs,
				Renderer: mr,
			}

			got, err := f.ExtractQueriesFromString(tt.content)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
			mr.AssertExpectations(t)
		})
	}
}

func TestWholeFileExtractor_ExtractQueriesFromString(t *testing.T) {
	t.Parallel()

	noOpRenderer := func(mr renderer) {
		mr.(*mockNoOpRenderer).On("Render", mock.Anything).Return("default", nil)
	}

	tests := []struct {
		name          string
		setupRenderer func(mr renderer)
		content       string
		want          []*Query
		wantErr       bool
	}{
		{
			name:          "only variables, no query",
			content:       "set variable1 = asd; set variable2 = 123;",
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: "set variable1 = asd; set variable2 = 123;",
				},
			},
		},
		{
			name:          "single query",
			content:       "select * from users;",
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: "select * from users;",
				},
			},
		},
		{
			name:    "single query, rendered properly",
			content: "select * from users-{{ds}};",
			setupRenderer: func(mr renderer) {
				mr.(*mockNoOpRenderer).
					On("Render", mock.Anything).
					Return("select * from users-2022-01-01", nil)
			},
			want: []*Query{
				{
					Query: "select * from users-2022-01-01",
				},
			},
		},
		{
			name: "multiple queries, multiline",
			content: `  select * from users;
		;;
									select name from countries;;
`,
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: `select * from users;
		;;
									select name from countries;;`,
				},
			},
		},
		{
			name: "multiple queries, multiline, starts with a comment",
			content: `
		-- here's some comment
		select * from users;
		;;
									select name from countries;;
									`,
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: `-- here's some comment
		select * from users;
		;;
									select name from countries;;`,
				},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mr := new(mockNoOpRenderer)
			if tt.setupRenderer != nil {
				tt.setupRenderer(mr)
			}

			f := WholeFileExtractor{
				Renderer: mr,
			}

			got, err := f.ExtractQueriesFromString(tt.content)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
			mr.AssertExpectations(t)
		})
	}
}

func TestQuery_ToExplainQuery(t *testing.T) {
	t.Parallel()

	type fields struct {
		VariableDefinitions []string
		Query               string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "no variable definitions",
			fields: fields{
				Query: "select * from users",
			},
			want: "EXPLAIN select * from users;",
		},
		{
			name: "query already has an EXPLAIN prefix",
			fields: fields{
				Query: "EXPLAIN select * from users",
			},
			want: "EXPLAIN select * from users;",
		},
		{
			name: "query is a USE statement, cannot be explained, should be kept the same",
			fields: fields{
				Query: "USE select * from users",
			},
			want: "USE select * from users;",
		},
		{
			name: "no variable definitions",
			fields: fields{
				VariableDefinitions: []string{
					"set analysis_period_days = 21",
				},
				Query: "select * from users",
			},
			want: `set analysis_period_days = 21;
EXPLAIN select * from users;`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := Query{
				VariableDefinitions: tt.fields.VariableDefinitions,
				Query:               tt.fields.Query,
			}

			assert.Equal(t, tt.want, e.ToExplainQuery())
		})
	}
}

func TestQuery_ToDryRunQuery(t *testing.T) {
	t.Parallel()

	type fields struct {
		VariableDefinitions []string
		Query               string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			name: "no variable definitions",
			fields: fields{
				Query: "select * from users",
			},
			want: "select * from users;",
		},
		{
			name: "no variable definitions",
			fields: fields{
				VariableDefinitions: []string{
					"set analysis_period_days = 21",
				},
				Query: "select * from users",
			},
			want: `set analysis_period_days = 21;
select * from users;`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := Query{
				VariableDefinitions: tt.fields.VariableDefinitions,
				Query:               tt.fields.Query,
			}

			assert.Equal(t, tt.want, e.ToDryRunQuery())
		})
	}
}
