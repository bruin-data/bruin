package ingestr

import (
	"reflect"
	"sort"
	"testing"
)

func TestCollectURISecrets(t *testing.T) {
	tests := []struct {
		name string
		uri  string
		want []string
	}{
		{
			name: "postgres with password and sensitive query param",
			uri:  "postgres://myuser:supersecretpass@db.example.com:5432/mydb?sslmode=require&api_key=sk_live_abc123",
			want: []string{"supersecretpass", "sk_live_abc123"},
		},
		{
			name: "bigquery credentials_base64",
			uri:  "bigquery://my-project/dataset?credentials_base64=ZmFrZWNyZWRzMTIz",
			want: []string{"ZmFrZWNyZWRzMTIz"},
		},
		{
			name: "presigned s3 url",
			uri:  "https://bucket.s3.amazonaws.com/key?X-Amz-Signature=abc123def456&X-Amz-Expires=3600",
			want: []string{"abc123def456"},
		},
		{
			name: "underscore vs hyphen vs case insensitivity",
			uri:  "https://api.example.com/v1?API_KEY=v1&access-token=v2&ClientSecret=v3",
			want: []string{"v1", "v2", "v3"},
		},
		{
			name: "no secrets",
			uri:  "duckdb:///path/to/db.duckdb",
			want: nil,
		},
		{
			name: "unparseable",
			uri:  "::::not a uri::::",
			want: nil,
		},
		{
			name: "harmless params untouched",
			uri:  "https://api.example.com/v1/users?limit=10&offset=20",
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := collectURISecrets(tc.uri)
			sort.Strings(got)
			want := append([]string(nil), tc.want...)
			sort.Strings(want)
			if !reflect.DeepEqual(got, want) {
				t.Errorf("got %v, want %v", got, want)
			}
		})
	}
}

func TestAppendDebugMaskFlags_DeduplicatesAcrossURIs(t *testing.T) {
	got := appendDebugMaskFlags(
		nil,
		"postgres://u:pw_shared_secret@a.example.com/db",
		"postgres://u:pw_shared_secret@b.example.com/db",
	)
	want := []string{"--debug-mask", "pw_shared_secret"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestAppendDebugMaskFlags_EmptyURIs(t *testing.T) {
	got := appendDebugMaskFlags([]string{"--debug"}, "", "")
	want := []string{"--debug"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected unchanged args, got %v", got)
	}
}

func TestIsCloudRun(t *testing.T) {
	t.Setenv("BRUIN_RUN_ID", "")
	if isCloudRun() {
		t.Error("expected false when BRUIN_RUN_ID unset")
	}
	t.Setenv("BRUIN_RUN_ID", "run_abc")
	if !isCloudRun() {
		t.Error("expected true when BRUIN_RUN_ID set")
	}
}
