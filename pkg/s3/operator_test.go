package s3

import (
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContainsWildcard(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want bool
	}{
		{name: "asterisk", key: "prefix/*.csv", want: true},
		{name: "brace_pattern", key: "prefix/{a,b}.csv", want: true},
		{name: "both_asterisk_and_brace", key: "prefix/{a,b}/*.csv", want: true},
		{name: "no_wildcard", key: "prefix/file.csv", want: false},
		{name: "empty_string", key: "", want: false},
		{name: "asterisk_only", key: "*", want: true},
		{name: "brace_only", key: "{", want: true},
		{name: "question_mark_not_wildcard", key: "prefix/file?.csv", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := containsWildcard(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestExtractPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want string
	}{
		{name: "asterisk_after_slash", key: "prefix/*.csv", want: "prefix/"},
		{name: "asterisk_in_middle", key: "a/b/*/c.csv", want: "a/b/"},
		{name: "brace_after_slash", key: "data/{a,b}.csv", want: "data/"},
		{name: "wildcard_at_start", key: "*.csv", want: ""},
		{name: "no_wildcard", key: "prefix/file.csv", want: "prefix/"},
		{name: "deep_nested", key: "a/b/c/d/*.parquet", want: "a/b/c/d/"},
		{name: "asterisk_in_directory", key: "logs/2024-*/*.log", want: "logs/"},
		{name: "brace_no_slash_before", key: "{a,b}.csv", want: ""},
		{name: "empty_string", key: "", want: ""},
		{name: "multiple_wildcards", key: "a/*/b/*.csv", want: "a/"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := extractPrefix(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWildcardToRegex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		wantRe  string
		match   []string
		noMatch []string
	}{
		{
			name:    "simple_asterisk",
			pattern: "prefix/*.csv",
			wantRe:  `^prefix/[^/]*\.csv$`,
			match:   []string{"prefix/file.csv", "prefix/data.csv", "prefix/.csv"},
			noMatch: []string{"prefix/sub/file.csv", "other/file.csv"},
		},
		{
			name:    "brace_alternatives",
			pattern: "data/{foo,bar}.csv",
			wantRe:  `^data/(foo|bar)\.csv$`,
			match:   []string{"data/foo.csv", "data/bar.csv"},
			noMatch: []string{"data/baz.csv", "data/foobar.csv"},
		},
		{
			name:    "brace_with_asterisk_inside",
			pattern: "logs/{access*,error*}.log",
			wantRe:  `^logs/(access[^/]*|error[^/]*)\.log$`,
			match:   []string{"logs/access_2024.log", "logs/error_critical.log"},
			noMatch: []string{"logs/debug.log"},
		},
		{
			name:    "no_special_chars",
			pattern: "exact/path/file.txt",
			wantRe:  `^exact/path/file\.txt$`,
			match:   []string{"exact/path/file.txt"},
			noMatch: []string{"exact/path/file_txt", "other/path/file.txt"},
		},
		{
			name:    "asterisk_only",
			pattern: "*",
			wantRe:  `^[^/]*$`,
			match:   []string{"file.csv", "anything"},
			noMatch: []string{"path/file.csv"},
		},
		{
			name:    "unclosed_brace_treated_as_literal",
			pattern: "prefix/{abc.csv",
			wantRe:  `^prefix/\{abc\.csv$`,
			match:   []string{"prefix/{abc.csv"},
			noMatch: []string{"prefix/abc.csv"},
		},
		{
			name:    "multiple_braces",
			pattern: "{a,b}/{c,d}.txt",
			wantRe:  `^(a|b)/(c|d)\.txt$`,
			match:   []string{"a/c.txt", "b/d.txt", "a/d.txt", "b/c.txt"},
			noMatch: []string{"a/e.txt", "c/c.txt"},
		},
		{
			name:    "dot_is_escaped",
			pattern: "file.name.csv",
			wantRe:  `^file\.name\.csv$`,
			match:   []string{"file.name.csv"},
			noMatch: []string{"filexnamexcsv"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := wildcardToRegex(tt.pattern)
			assert.Equal(t, tt.wantRe, got)

			for _, s := range tt.match {
				assert.Regexp(t, got, s, "expected %q to match pattern %q", s, tt.pattern)
			}
			for _, s := range tt.noMatch {
				assert.NotRegexp(t, got, s, "expected %q NOT to match pattern %q", s, tt.pattern)
			}
		})
	}
}

func TestNewKeySensor(t *testing.T) {
	t.Parallel()

	conn := &mockConnectionGetter{}
	ks := NewKeySensor(conn, "once")

	assert.NotNil(t, ks)
	assert.Equal(t, "once", ks.sensorMode)
	assert.Equal(t, conn, ks.connection)
}

func TestKeySensor_RunTask_SkipMode(t *testing.T) {
	t.Parallel()

	ks := NewKeySensor(&mockConnectionGetter{}, "skip")
	err := ks.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{})
	require.NoError(t, err)
}

func TestKeySensor_RunTask_MissingBucketName(t *testing.T) {
	t.Parallel()

	ks := NewKeySensor(&mockConnectionGetter{}, "once")
	asset := &pipeline.Asset{
		Parameters: map[string]string{},
	}
	err := ks.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bucket_name")
}

func TestKeySensor_RunTask_MissingBucketKey(t *testing.T) {
	t.Parallel()

	ks := NewKeySensor(&mockConnectionGetter{}, "once")
	asset := &pipeline.Asset{
		Parameters: map[string]string{
			"bucket_name": "my-bucket",
		},
	}
	err := ks.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bucket_key")
}

func TestKeySensor_RunTask_ConnectionNotFound(t *testing.T) {
	t.Parallel()

	conn := &mockConnectionGetter{
		details: nil,
	}
	ks := NewKeySensor(conn, "once")
	asset := &pipeline.Asset{
		Connection: "my-conn",
		Parameters: map[string]string{
			"bucket_name": "my-bucket",
			"bucket_key":  "path/to/file.csv",
		},
	}
	err := ks.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not exist")
}

func TestKeySensor_RunTask_WrongConnectionType(t *testing.T) {
	t.Parallel()

	conn := &mockConnectionGetter{
		details: "not-a-valid-connection-type",
	}
	ks := NewKeySensor(conn, "once")
	asset := &pipeline.Asset{
		Connection: "my-conn",
		Parameters: map[string]string{
			"bucket_name": "my-bucket",
			"bucket_key":  "path/to/file.csv",
		},
	}
	err := ks.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not an AWS/S3 connection")
}

func TestKeySensor_RunTask_AwsConnectionUsesCorrectCredentials(t *testing.T) {
	t.Parallel()

	conn := &mockConnectionGetter{
		details: &config.AwsConnection{
			Name:      "my-conn",
			AccessKey: "test-access-key",
			SecretKey: "test-secret-key",
			Region:    "us-west-2",
		},
	}
	ks := NewKeySensor(conn, "once")
	asset := &pipeline.Asset{
		Connection: "my-conn",
		Parameters: map[string]string{
			"bucket_name": "my-bucket",
			"bucket_key":  "path/to/file.csv",
		},
	}

	err := ks.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "does not exist")
	assert.NotContains(t, err.Error(), "not an AWS/S3 connection")
}

func TestKeySensor_RunTask_S3ConnectionUsesCorrectCredentials(t *testing.T) {
	t.Parallel()

	conn := &mockConnectionGetter{
		details: &config.S3Connection{
			Name:            "my-s3-conn",
			AccessKeyID:     "test-access-key",
			SecretAccessKey: "test-secret-key",
			EndpointURL:     "http://localhost:9000",
		},
	}
	ks := NewKeySensor(conn, "once")
	asset := &pipeline.Asset{
		Connection: "my-s3-conn",
		Parameters: map[string]string{
			"bucket_name": "my-bucket",
			"bucket_key":  "path/to/file.csv",
		},
	}

	err := ks.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "does not exist")
	assert.NotContains(t, err.Error(), "not an AWS/S3 connection")
}

type mockConnectionGetter struct {
	details any
}

func (m *mockConnectionGetter) GetConnection(name string) any {
	return m.details
}

func (m *mockConnectionGetter) GetConnectionDetails(name string) any {
	return m.details
}

func (m *mockConnectionGetter) GetConnectionType(name string) string {
	return ""
}
