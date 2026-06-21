package query

import (
	"context"
	"testing"

	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/pipeline"
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

//nolint:ireturn
func (m *mockNoOpRenderer) CloneForAsset(ctx context.Context, p *pipeline.Pipeline, asset *pipeline.Asset) (jinja.RendererInterface, error) {
	args := m.Called(ctx, asset)
	return args.Get(0).(jinja.RendererInterface), args.Error(1)
}

func (m *mockNoOpRenderer) RenderAsset(t *pipeline.Asset) (*pipeline.Asset, error) {
	args := m.Called(t)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*pipeline.Asset), args.Error(1)
}

func TestFileExtractor_ExtractQueriesFromString(t *testing.T) {
	t.Parallel()

	noOpRenderer := func(mr jinja.RendererInterface) {
		mr.(*mockNoOpRenderer).On("Render", mock.Anything).Return("default", nil)
	}

	tests := []struct {
		name          string
		setupRenderer func(mr jinja.RendererInterface)
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
			setupRenderer: func(mr jinja.RendererInterface) {
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

func TestOracleScriptExtractor_ExtractQueriesFromString(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
create table users (id number);

BEGIN
  IF 1 = 1 THEN
    NULL;
  END IF;
  app_etl.rebuild_index('USERS');
END;
/

insert into users values ('semi;colon');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "create table users (id number);"},
		{Query: "BEGIN\n  IF 1 = 1 THEN\n    NULL;\n  END IF;\n  app_etl.rebuild_index('USERS');\nEND;"},
		{Query: "insert into users values ('semi;colon');"},
	}, got)
	mr.AssertExpectations(t)
}

func TestOracleScriptExtractor_IgnoresPLSQLKeywordsInStringLiterals(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
BEGIN
  v_msg := 'Marking BEGIN of run';
END;
/

insert into audit_log values ('done');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "BEGIN\n  v_msg := 'Marking BEGIN of run';\nEND;"},
		{Query: "insert into audit_log values ('done');"},
	}, got)
	mr.AssertExpectations(t)
}

func TestOracleScriptExtractor_IgnoresPLSQLKeywordsInDoubleQuotedIdentifiers(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
create or replace procedure rebuild_users as
  v_end date;
begin
  select max("END") into v_end from users;
  null;
end rebuild_users;
/

insert into audit_log values ('done');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "create or replace procedure rebuild_users as\n  v_end date;\nbegin\n  select max(\"END\") into v_end from users;\n  null;\nend rebuild_users;"},
		{Query: "insert into audit_log values ('done');"},
	}, got)
	mr.AssertExpectations(t)
}

func TestOracleScriptExtractor_HandlesTrailingLineCommentBeforePLSQLTerminator(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
BEGIN
  NULL;
END -- closes block
;
/

insert into audit_log values ('done');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "BEGIN\n  NULL;\nEND -- closes block\n;"},
		{Query: "insert into audit_log values ('done');"},
	}, got)
	mr.AssertExpectations(t)
}

func TestOracleScriptExtractor_DoesNotTreatBeginPrefixIdentifierAsPLSQLBlock(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
BEGINDATE_CALC('USERS');
insert into audit_log values ('done');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "BEGINDATE_CALC('USERS');"},
		{Query: "insert into audit_log values ('done');"},
	}, got)
	mr.AssertExpectations(t)
}

func TestOracleScriptExtractor_KeepsPLSQLDDLAsSingleQuery(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
create or replace procedure rebuild_users as
begin
  execute immediate 'truncate table USERS_STAGE';
  if 1 = 1 then
    null;
  end if;
end rebuild_users;
/

insert into audit_log values ('done');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "create or replace procedure rebuild_users as\nbegin\n  execute immediate 'truncate table USERS_STAGE';\n  if 1 = 1 then\n    null;\n  end if;\nend rebuild_users;"},
		{Query: "insert into audit_log values ('done');"},
	}, got)
	mr.AssertExpectations(t)
}

func TestOracleScriptExtractor_KeepsPackageBodyAsSingleQuery(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
create or replace package body pkg_users as
  procedure rebuild as
  begin
    null;
  end;

  function user_count return number as
  begin
    return 1;
  end user_count;
end pkg_users;
/

insert into audit_log values ('done');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "create or replace package body pkg_users as\n  procedure rebuild as\n  begin\n    null;\n  end;\n\n  function user_count return number as\n  begin\n    return 1;\n  end user_count;\nend pkg_users;"},
		{Query: "insert into audit_log values ('done');"},
	}, got)
	mr.AssertExpectations(t)
}

func TestOracleScriptExtractor_KeepsPackageBodyWithInitializationAsSingleQuery(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
create or replace package body pkg as
  procedure p as
  begin
    null;
  end;
begin
  null;
end pkg;
/

insert into audit_log values ('done');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "create or replace package body pkg as\n  procedure p as\n  begin\n    null;\n  end;\nbegin\n  null;\nend pkg;"},
		{Query: "insert into audit_log values ('done');"},
	}, got)
	mr.AssertExpectations(t)
}

func TestOracleScriptExtractor_SplitsPlainCreateTypeSpec(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
create type user_row as object (
  id number,
  name varchar2(100)
);

insert into audit_log values ('done');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "create type user_row as object (\n  id number,\n  name varchar2(100)\n);"},
		{Query: "insert into audit_log values ('done');"},
	}, got)
	mr.AssertExpectations(t)
}

func TestWholeFileExtractor_ExtractQueriesFromString(t *testing.T) {
	t.Parallel()

	noOpRenderer := func(mr jinja.RendererInterface) {
		mr.(*mockNoOpRenderer).On("Render", mock.Anything).Return("default", nil)
	}

	tests := []struct {
		name          string
		setupRenderer func(mr jinja.RendererInterface)
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
			setupRenderer: func(mr jinja.RendererInterface) {
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
