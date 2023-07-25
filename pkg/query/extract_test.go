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

func (m *mockNoOpRenderer) Render(template string) string {
	args := m.Called(template)
	if args.Get(0) == "default" {
		return template
	}

	return args.String(0)
}

func TestFileExtractor_ExtractQueriesFromFile(t *testing.T) {
	t.Parallel()

	noOpRenderer := func(mr renderer) {
		mr.(*mockNoOpRenderer).On("Render", mock.Anything).Return("default")
	}

	tests := []struct {
		name            string
		setupFilesystem func(t *testing.T, fs afero.Fs)
		setupRenderer   func(mr renderer)
		path            string
		want            []*Query
		wantErr         bool
	}{
		{
			name:    "file doesnt exist, fail",
			path:    "somefile.txt",
			want:    nil,
			wantErr: true,
		},
		{
			name: "only variables, no query",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				err := afero.WriteFile(fs, "somefile.txt", []byte("set variable1 = asd; set variable2 = 123;"), 0o644)
				require.NoError(t, err)
			},
			setupRenderer: noOpRenderer,
			want:          make([]*Query, 0),
		},
		{
			name: "single query",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				err := afero.WriteFile(fs, "somefile.txt", []byte("select * from users;"), 0o644)
				require.NoError(t, err)
			},
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: "select * from users",
				},
			},
		},
		{
			name: "single query, rendered properly",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				err := afero.WriteFile(fs, "somefile.txt", []byte("select * from users-{{ds}};"), 0o644)
				require.NoError(t, err)
			},
			setupRenderer: func(mr renderer) {
				mr.(*mockNoOpRenderer).
					On("Render", mock.Anything).
					Return("select * from users-2022-01-01")
			},
			want: []*Query{
				{
					Query: "select * from users-2022-01-01",
				},
			},
		},
		{
			name: "multiple queries, multiline",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				query := `select * from users;
		;;
									select name from countries;;
									`
				err := afero.WriteFile(fs, "somefile.txt", []byte(query), 0o644)
				require.NoError(t, err)
			},
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
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				query := `
		-- here's some comment
		select * from users;
		;;
									select name from countries;;
									`
				err := afero.WriteFile(fs, "somefile.txt", []byte(query), 0o644)
				require.NoError(t, err)
			},
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
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				query := `
		-- here's some comment
		select * from users;
		;;
		-- here's some other comment
			-- and a nested one event
/*
some random query between comments;
*/
		select name from countries;;
									`
				err := afero.WriteFile(fs, "somefile.txt", []byte(query), 0o644)
				require.NoError(t, err)
			},
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
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				query := `
		-- here's some comment
set analysis_period_days = 21;
		select * from users;
		;;
set analysis_start_date = dateadd(days, -($analysis_period_days - 1), $analysis_end_date);
set min_level_req = 22;
		-- here's some other comment
			-- and a nested one event
		
		select name from countries;;
									`
				err := afero.WriteFile(fs, "somefile.txt", []byte(query), 0o644)
				require.NoError(t, err)
			},
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
			if tt.setupFilesystem != nil {
				tt.setupFilesystem(t, fs)
			}

			mr := new(mockNoOpRenderer)
			if tt.setupRenderer != nil {
				tt.setupRenderer(mr)
			}

			f := FileQuerySplitterExtractor{
				Fs:       fs,
				Renderer: mr,
			}

			got, err := f.ExtractQueriesFromFile(tt.path)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.want, got)
			mr.AssertExpectations(t)
		})
	}
}

func TestWholeFileExtractor_ExtractQueriesFromFile(t *testing.T) {
	t.Parallel()

	noOpRenderer := func(mr renderer) {
		mr.(*mockNoOpRenderer).On("Render", mock.Anything).Return("default")
	}

	tests := []struct {
		name            string
		setupFilesystem func(t *testing.T, fs afero.Fs)
		setupRenderer   func(mr renderer)
		path            string
		want            []*Query
		wantErr         bool
	}{
		{
			name:    "file doesnt exist, fail",
			path:    "somefile.txt",
			want:    nil,
			wantErr: true,
		},
		{
			name: "only variables, no query",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				err := afero.WriteFile(fs, "somefile.txt", []byte("set variable1 = asd; set variable2 = 123;"), 0o644)
				require.NoError(t, err)
			},
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: "set variable1 = asd; set variable2 = 123;",
				},
			},
		},
		{
			name: "single query",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				err := afero.WriteFile(fs, "somefile.txt", []byte("select * from users;"), 0o644)
				require.NoError(t, err)
			},
			setupRenderer: noOpRenderer,
			want: []*Query{
				{
					Query: "select * from users;",
				},
			},
		},
		{
			name: "single query, rendered properly",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				err := afero.WriteFile(fs, "somefile.txt", []byte("select * from users-{{ds}};"), 0o644)
				require.NoError(t, err)
			},
			setupRenderer: func(mr renderer) {
				mr.(*mockNoOpRenderer).
					On("Render", mock.Anything).
					Return("select * from users-2022-01-01")
			},
			want: []*Query{
				{
					Query: "select * from users-2022-01-01",
				},
			},
		},
		{
			name: "multiple queries, multiline",
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				query := `  select * from users;
		;;
									select name from countries;;
`
				err := afero.WriteFile(fs, "somefile.txt", []byte(query), 0o644)
				require.NoError(t, err)
			},
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
			path: "somefile.txt",
			setupFilesystem: func(t *testing.T, fs afero.Fs) {
				query := `
		-- here's some comment
		select * from users;
		;;
									select name from countries;;
									`
				err := afero.WriteFile(fs, "somefile.txt", []byte(query), 0o644)
				require.NoError(t, err)
			},
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

			fs := afero.NewMemMapFs()
			if tt.setupFilesystem != nil {
				tt.setupFilesystem(t, fs)
			}

			mr := new(mockNoOpRenderer)
			if tt.setupRenderer != nil {
				tt.setupRenderer(mr)
			}

			f := WholeFileExtractor{
				Fs:       fs,
				Renderer: mr,
			}

			got, err := f.ExtractQueriesFromFile(tt.path)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
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
