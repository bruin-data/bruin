package python

import "testing"

func TestRedactArgsForLog(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "no debug-mask flags leaves args untouched",
			args: []string{"ingest", "--source-uri", "duckdb:///x.db", "--yes"},
			want: "ingest --source-uri duckdb:///x.db --yes",
		},
		{
			name: "single debug-mask redacts everywhere",
			args: []string{
				"ingest",
				"--source-uri", "postgres://u:SuperSecretPass987@h/d",
				"--debug-mask", "SuperSecretPass987",
			},
			want: "ingest --source-uri postgres://u:***@h/d --debug-mask ***",
		},
		{
			name: "multiple debug-masks each redacted",
			args: []string{
				"ingest",
				"--source-uri", "https://api.example.com/v1?api_key=abcdef12345678&token=zzz_yyy_xxx",
				"--debug-mask", "abcdef12345678",
				"--debug-mask", "zzz_yyy_xxx",
			},
			want: "ingest --source-uri https://api.example.com/v1?api_key=***&token=*** --debug-mask *** --debug-mask ***",
		},
		{
			name: "trailing --debug-mask without value ignored",
			args: []string{"ingest", "--debug-mask"},
			want: "ingest --debug-mask",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := redactArgsForLog(tc.args); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
