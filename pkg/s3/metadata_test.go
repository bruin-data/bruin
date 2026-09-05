package s3

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestKeySensor_Metadata(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>data/missing.csv</Key></Contents><Contents><Key>data/file.csv</Key></Contents></ListBucketResult>`)
			return
		}
		if r.URL.Path == "/test-bucket/data/missing.csv" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", `"provider-opaque-multipart-7"`)
		w.Header().Set("Content-Length", "42")
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
	}))
	t.Cleanup(server.Close)
	for _, key := range []string{"data/file.csv", "data/*.csv"} {
		for _, tc := range []struct {
			name    string
			params  pipeline.ParameterMap
			matches bool
		}{
			{"opaque etag", pipeline.ParameterMap{"etag": "provider-opaque-multipart-7"}, true},
			{"quoted etag", pipeline.ParameterMap{"etag": `"provider-opaque-multipart-7"`}, true},
			{"different etag", pipeline.ParameterMap{"etag": "other"}, false},
			{"minimum size", pipeline.ParameterMap{"min_size": 42}, true},
			{"too small", pipeline.ParameterMap{"min_size": 43}, false},
			{"newer", pipeline.ParameterMap{"last_modified_after": "2006-01-01T00:00:00Z"}, true},
			{"same timestamp", pipeline.ParameterMap{"last_modified_after": "2006-01-02T15:04:05Z"}, false},
			{"all constraints", pipeline.ParameterMap{"etag": "provider-opaque-multipart-7", "min_size": "42", "last_modified_after": "2006-01-01T00:00:00Z"}, true},
		} {
			t.Run(key+"/"+tc.name, func(t *testing.T) {
				t.Parallel()

				conn := endpointConnection(server.URL)
				sensor := NewKeySensor(&mockConnectionGetter{details: &conn}, "once")
				tc.params["bucket_name"] = "test-bucket"
				tc.params["bucket_key"] = key
				err := sensor.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{Connection: "s3-test", Parameters: tc.params})
				if tc.matches {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, "Sensor didn't return the expected result")
				}
			})
		}
	}
}

func TestParseMetadataFilter_Invalid(t *testing.T) {
	t.Parallel()
	for _, params := range []pipeline.ParameterMap{
		{"etag": 12},
		{"etag": `""`},
		{"min_size": -1},
		{"min_size": 1.5},
		{"min_size": nil},
		{"last_modified_after": "yesterday"},
	} {
		_, err := parseMetadataFilter(params)
		require.Error(t, err)
	}
}

func TestParseMetadataFilter_YAMLTimestamps(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		`"2024-01-01T00:00:00Z"`,
		`2024-01-01T00:00:00Z`,
		`2024-01-01T03:00:00+03:00`,
	} {
		t.Run(value, func(t *testing.T) {
			t.Parallel()

			var params pipeline.ParameterMap
			require.NoError(t, yaml.Unmarshal([]byte("last_modified_after: "+value), &params))
			filter, err := parseMetadataFilter(params)
			require.NoError(t, err)
			require.True(t, filter.modifiedAfter.Equal(time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)))
		})
	}
}

func TestKeySensor_WaitForMetadataAndTimeout(t *testing.T) {
	t.Parallel()
	for _, key := range []string{"file.csv", "*.csv"} {
		t.Run(key, func(t *testing.T) {
			t.Parallel()

			var heads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>file.csv</Key></Contents></ListBucketResult>`)
					return
				}
				if heads.Add(1) == 1 {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				w.Header().Set("ETag", `"ready"`)
			}))
			defer server.Close()
			conn := endpointConnection(server.URL)
			sensor := NewKeySensor(&mockConnectionGetter{details: &conn}, "wait")
			asset := &pipeline.Asset{Connection: "s3-test", Parameters: pipeline.ParameterMap{
				"bucket_name": "test-bucket", "bucket_key": key, "etag": "ready", "poke_interval": "0", "timeout": "5s",
			}}
			require.NoError(t, sensor.RunTask(t.Context(), &pipeline.Pipeline{}, asset))
			require.Equal(t, int32(2), heads.Load())
			asset.Parameters["etag"] = "not-ready"
			asset.Parameters["poke_interval"] = "60"
			asset.Parameters["timeout"] = "10ms"
			err := sensor.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		})
	}
}
