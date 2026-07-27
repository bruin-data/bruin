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

func TestFileExtractor_PreservesSessionStatements(t *testing.T) {
	t.Parallel()

	renderer := new(mockNoOpRenderer)
	renderer.On("Render", mock.Anything).Return("default", nil)
	extractor := FileQuerySplitterExtractor{
		Renderer:                  renderer,
		PreserveSessionStatements: true,
	}

	got, err := extractor.ExtractQueriesFromString(`
USE analytics;
SET spark.sql.shuffle.partitions = 8;
DECLARE threshold INT DEFAULT 10;
SELECT threshold;
`)
	require.NoError(t, err)
	require.Equal(t, []*Query{
		{Query: "USE analytics"},
		{Query: "SET spark.sql.shuffle.partitions = 8"},
		{Query: "DECLARE threshold INT DEFAULT 10"},
		{Query: "SELECT threshold"},
	}, got)
	renderer.AssertExpectations(t)
}

func TestSplitQueriesPreservingSessionStatementsRemovesComments(t *testing.T) {
	t.Parallel()

	got := SplitQueriesPreservingSessionStatements(`
-- select the namespace
USE analytics;
/* configure this session */
SET spark.sql.shuffle.partitions = 8;
SELECT 1;
`)

	require.Equal(t, []*Query{
		{Query: "USE analytics"},
		{Query: "SET spark.sql.shuffle.partitions = 8"},
		{Query: "SELECT 1"},
	}, got)
}

func TestFileExtractorRemovesTrailingCommentWithoutNewline(t *testing.T) {
	t.Parallel()

	renderer := new(mockNoOpRenderer)
	renderer.On("Render", mock.Anything).Return("default", nil)
	extractor := FileQuerySplitterExtractor{
		Renderer:                  renderer,
		PreserveSessionStatements: true,
	}

	got, err := extractor.ExtractQueriesFromString("SELECT 1;\n-- trailing comment")
	require.NoError(t, err)
	require.Equal(t, []*Query{{Query: "SELECT 1"}}, got)
	renderer.AssertExpectations(t)
}

func TestStripSQLComments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want string
		// noBackslashEscapes runs the case as a dialect where a backslash inside a
		// string literal is data rather than an escape.
		noBackslashEscapes bool
	}{
		{
			name: "line comment with its newline",
			in:   "SELECT 1 -- pick one\nFROM t",
			want: "SELECT 1\nFROM t",
		},
		{
			name: "trailing line comment without a newline",
			in:   "SELECT 1\n-- trailing",
			want: "SELECT 1\n\n",
		},
		{
			name: "block comment",
			in:   "SELECT /* pick\none */ 1",
			want: "SELECT\n 1",
		},
		{
			name: "double dash inside a string literal is data",
			in:   "INSERT INTO audit VALUES ('run -- 1')",
			want: "INSERT INTO audit VALUES ('run -- 1')",
		},
		{
			name: "block comment opener inside a string literal is data",
			in:   "SELECT 'a /* b' AS c",
			want: "SELECT 'a /* b' AS c",
		},
		{
			name: "doubled quote inside a string literal",
			in:   "SELECT 'it''s -- fine' -- comment",
			want: "SELECT 'it''s -- fine'\n",
		},
		{
			name: "double dash inside a quoted identifier is data",
			in:   `SELECT "a -- b" FROM t`,
			want: `SELECT "a -- b" FROM t`,
		},
		{
			name: "double dash inside a backtick identifier is data",
			in:   "SELECT `a -- b` FROM t",
			want: "SELECT `a -- b` FROM t",
		},
		{
			name: "double dash inside an Oracle q-quote is data",
			in:   "SELECT q'[run -- 1]' FROM t",
			want: "SELECT q'[run -- 1]' FROM t",
		},
		{
			name: "double dash not preceded by whitespace is not a comment",
			in:   "SELECT 5--1 AS x",
			want: "SELECT 5--1 AS x",
		},
		{
			name: "unterminated block comment keeps its payload",
			in:   "SELECT 1;\n/* unterminated\nSELECT 2",
			want: "SELECT 1;\n/* unterminated\nSELECT 2",
		},
		{
			name: "unterminated string literal keeps its payload",
			in:   "SELECT 'unterminated -- x",
			want: "SELECT 'unterminated\n",
		},
		{
			name: "backslash-escaped quote still gets later comments stripped",
			in:   "SELECT regexp_replace(name, '\\'', '') AS n -- strip; keep\nFROM t",
			want: "SELECT regexp_replace(name, '\\'', '') AS n\nFROM t",
		},
		{
			// A backslash escape inside the literal must not leave a phantom run
			// open, or the comment survives and splitQueries cuts at its `;`.
			name: "comment after a backslash-escaped literal is stripped",
			in:   "SELECT 'it\\'s' -- note; here\nUNION ALL SELECT 'ok'",
			want: "SELECT 'it\\'s'\nUNION ALL SELECT 'ok'",
		},
		{
			name: "block comment after a backslash-escaped literal is stripped",
			in:   "SELECT E'a\\'b' /* blk; split */ , 'c' FROM t",
			want: "SELECT E'a\\'b'\n , 'c' FROM t",
		},
		{
			name: "doubled backslash does not escape the closing quote",
			in:   "SELECT 'a\\\\' -- c\nFROM t",
			want: "SELECT 'a\\\\'\nFROM t",
		},
		{
			name: "backslash inside a backtick identifier is data",
			in:   "SELECT `a\\` -- c\nFROM t",
			want: "SELECT `a\\`\nFROM t",
		},
		{
			// Trino, Dremio, Oracle and standards-conforming Postgres take the
			// backslash literally, so `ESCAPE '\'` closes its literal and the
			// comment after it still gets stripped.
			name:               "trailing backslash closes the literal without backslash escapes",
			in:                 "SELECT x LIKE '%a' ESCAPE '\\' -- c; d\nAND y = 'z'",
			want:               "SELECT x LIKE '%a' ESCAPE '\\'\nAND y = 'z'",
			noBackslashEscapes: true,
		},
		{
			name:               "windows path literal without backslash escapes",
			in:                 "SELECT 'C:\\temp\\' AS p -- note; here\nFROM t",
			want:               "SELECT 'C:\\temp\\' AS p\nFROM t",
			noBackslashEscapes: true,
		},
		{
			// Postgres needs both behaviours in one statement: the backslash is
			// data in the plain literal but an escape in the E'' one.
			name:               "postgres escape-string literal still escapes",
			in:                 "SELECT E'it\\'s' AS a, 'C:\\' AS b -- note; here\nFROM t",
			want:               "SELECT E'it\\'s' AS a, 'C:\\' AS b\nFROM t",
			noBackslashEscapes: true,
		},
		{
			name:               "lowercase postgres escape-string literal",
			in:                 "SELECT e'a\\'b' /* blk; split */ , 'c' FROM t",
			want:               "SELECT e'a\\'b'\n , 'c' FROM t",
			noBackslashEscapes: true,
		},
		{
			name:               "identifier ending in e before a literal is not an escape string",
			in:                 "SELECT queue'a\\' AS x -- c\nFROM t",
			want:               "SELECT queue'a\\' AS x\nFROM t",
			noBackslashEscapes: true,
		},
		{
			name: "unterminated block comment does not disable later stripping",
			in:   "SELECT 1 /* a -- b\nFROM t -- drop me\nWHERE x",
			want: "SELECT 1 /* a\nFROM t\nWHERE x",
		},
		{
			name: "identifier ending in q before a literal is not a q-quote",
			in:   "SELECT max_q'abc' AS x -- c\nFROM t",
			want: "SELECT max_q'abc' AS x\nFROM t",
		},
		{
			name: "form feed counts as whitespace before a line comment",
			in:   "SELECT 1\f-- c\nFROM t",
			want: "SELECT 1\f\nFROM t",
		},
		{
			// Stripping runs before rendering, so an apostrophe in a Jinja
			// comment would otherwise be scanned as the start of a literal and
			// swallow the real comment below it.
			// Left for the renderer to delete, so that its `{#-`/`-#}` whitespace
			// controls still apply and a comment inside a token does not gain a
			// newline through the middle of it.
			name: "jinja comment containing an apostrophe",
			in:   "{# don't touch #}\nSELECT a -- keep; only\nFROM t WHERE s = 'y'",
			want: "{# don't touch #}\nSELECT a\nFROM t WHERE s = 'y'",
		},
		{
			name: "jinja comment inside a token is not broken up",
			in:   "SELECT co{# inline #}l FROM t -- c",
			want: "SELECT co{# inline #}l FROM t\n",
		},
		{
			name: "jinja expressions and blocks are left alone",
			in:   "SELECT {{ start_date }} -- c\n{% if x %}AND y{% endif %}",
			want: "SELECT {{ start_date }}\n{% if x %}AND y{% endif %}",
		},
		{
			name: "unterminated jinja comment keeps its payload",
			in:   "SELECT 1 {# unterminated\nFROM t -- drop me\nWHERE x",
			want: "SELECT 1 {# unterminated\nFROM t\nWHERE x",
		},
		{
			name: "postgres json path operators are not jinja comments",
			in:   "SELECT data #> '{a,b}' -- c\nFROM t",
			want: "SELECT data #> '{a,b}'\nFROM t",
		},
		{
			name: "repeated unterminated block comments still strip later comments",
			in:   "/* /* /* SELECT 1 -- drop me\nFROM t",
			want: "/* /* /* SELECT 1\nFROM t",
		},
		{
			name: "CRLF line comment",
			in:   "SELECT 1 -- pick one\r\nFROM t",
			want: "SELECT 1\nFROM t",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, test.want, stripSQLComments(test.in, !test.noBackslashEscapes))
		})
	}
}

// A post-hook is TrimSpace'd and joined without a trailing newline before every
// operator re-parses it, so a `--` inside a string literal on the last line must
// survive intact.
func TestFileExtractorKeepsDoubleDashInsideStringLiteral(t *testing.T) {
	t.Parallel()

	renderer := new(mockNoOpRenderer)
	renderer.On("Render", mock.Anything).Return("default", nil)
	extractor := FileQuerySplitterExtractor{Renderer: renderer}

	statement := "INSERT INTO audit VALUES ('run -- 1')"
	got, err := extractor.ExtractQueriesFromString(statement)
	require.NoError(t, err)
	require.Equal(t, []*Query{{Query: statement}}, got)
	renderer.AssertExpectations(t)
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

func TestOracleScriptExtractor_IgnoresPLSQLKeywordsInQQuotedStringLiterals(t *testing.T) {
	t.Parallel()

	mr := new(mockNoOpRenderer)
	mr.On("Render", mock.Anything).Return("default", nil)

	f := OracleScriptExtractor{
		Renderer: mr,
	}

	got, err := f.ExtractQueriesFromString(`
BEGIN
  v_msg := q'[BEGIN isn't END; still text]';
END;
/

insert into audit_log values ('done');
`)
	require.NoError(t, err)

	assert.Equal(t, []*Query{
		{Query: "BEGIN\n  v_msg := q'[BEGIN isn't END; still text]';\nEND;"},
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
