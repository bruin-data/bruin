package objectpattern

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContainsWildcard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "asterisk", value: "prefix/*.csv", want: true},
		{name: "brace pattern", value: "prefix/{a,b}.csv", want: true},
		{name: "both forms", value: "prefix/{a,b}/*.csv", want: true},
		{name: "asterisk only", value: "*", want: true},
		{name: "opening brace only", value: "{", want: true},
		{name: "literal object", value: "prefix/file.csv", want: false},
		{name: "empty string", value: "", want: false},
		{name: "question mark", value: "prefix/file?.csv", want: false},
		{name: "closing brace only", value: "prefix/file}.csv", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, ContainsWildcard(tt.value))
		})
	}
}

func TestExtractPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "asterisk after slash", value: "prefix/*.csv", want: "prefix/"},
		{name: "asterisk in middle", value: "a/b/*/c.csv", want: "a/b/"},
		{name: "brace after slash", value: "data/{a,b}.csv", want: "data/"},
		{name: "wildcard at start", value: "*.csv", want: ""},
		{name: "literal object", value: "prefix/file.csv", want: "prefix/"},
		{name: "deep path", value: "a/b/c/d/*.parquet", want: "a/b/c/d/"},
		{name: "wildcard in directory", value: "logs/2024-*/*.log", want: "logs/"},
		{name: "brace before slash", value: "{a,b}/file.csv", want: ""},
		{name: "empty string", value: "", want: ""},
		{name: "multiple wildcards", value: "a/*/b/*.csv", want: "a/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, ExtractPrefix(tt.value))
		})
	}
}

func TestWildcardToRegex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		want    string
		matches []string
		rejects []string
	}{
		{
			name:    "asterisk stays within path segment",
			pattern: "prefix/*.csv",
			want:    `^prefix/[^/]*\.csv$`,
			matches: []string{"prefix/file.csv", "prefix/.csv"},
			rejects: []string{"prefix/sub/file.csv", "other/file.csv"},
		},
		{
			name:    "brace alternatives",
			pattern: "data/{foo,bar}.csv",
			want:    `^data/(foo|bar)\.csv$`,
			matches: []string{"data/foo.csv", "data/bar.csv"},
			rejects: []string{"data/baz.csv", "data/foobar.csv"},
		},
		{
			name:    "alternative whitespace is trimmed",
			pattern: "data/{foo, bar}.csv",
			want:    `^data/(foo|bar)\.csv$`,
			matches: []string{"data/foo.csv", "data/bar.csv"},
			rejects: []string{"data/ bar.csv"},
		},
		{
			name:    "asterisk inside alternatives",
			pattern: "logs/{access*,error*}.log",
			want:    `^logs/(access[^/]*|error[^/]*)\.log$`,
			matches: []string{"logs/access_2024.log", "logs/error_critical.log"},
			rejects: []string{"logs/debug.log", "logs/access/2024.log"},
		},
		{
			name:    "literal regex characters are escaped",
			pattern: "exact/file.name+[1].txt",
			want:    `^exact/file\.name\+\[1\]\.txt$`,
			matches: []string{"exact/file.name+[1].txt"},
			rejects: []string{"exact/fileXname1.txt"},
		},
		{
			name:    "asterisk only",
			pattern: "*",
			want:    `^[^/]*$`,
			matches: []string{"file.csv", "anything"},
			rejects: []string{"path/file.csv"},
		},
		{
			name:    "unclosed brace is literal",
			pattern: "prefix/{abc.csv",
			want:    `^prefix/\{abc\.csv$`,
			matches: []string{"prefix/{abc.csv"},
			rejects: []string{"prefix/abc.csv"},
		},
		{
			name:    "multiple braces",
			pattern: "{a,b}/{c,d}.txt",
			want:    `^(a|b)/(c|d)\.txt$`,
			matches: []string{"a/c.txt", "b/d.txt", "a/d.txt", "b/c.txt"},
			rejects: []string{"a/e.txt", "c/c.txt"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := WildcardToRegex(tt.pattern)
			assert.Equal(t, tt.want, got)

			re, err := regexp.Compile(got)
			require.NoError(t, err)
			for _, value := range tt.matches {
				assert.True(t, re.MatchString(value), "expected %q to match %q", value, tt.pattern)
			}
			for _, value := range tt.rejects {
				assert.False(t, re.MatchString(value), "expected %q not to match %q", value, tt.pattern)
			}
		})
	}
}
