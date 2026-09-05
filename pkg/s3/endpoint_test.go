package s3

import (
	"bytes"
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeySensor_CustomEndpointHeadUsesPathRegionAndSessionToken(t *testing.T) {
	var called atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
		assert.Equal(t, http.MethodHead, r.Method)
		assert.Equal(t, "/test-bucket/folder/file.csv", r.URL.EscapedPath())
		assert.Contains(t, r.Header.Get("Authorization"), "Credential=test-access/", "request must use configured access key")
		assert.Contains(t, r.Header.Get("Authorization"), "/eu-north-1/s3/aws4_request", "request must be signed for configured region")
		assert.Equal(t, "test-session-token", r.Header.Get("X-Amz-Security-Token"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	output := &bytes.Buffer{}
	ctx := context.WithValue(t.Context(), executor.KeyPrinter, output)
	err := runEndpointSensor(ctx, config.S3Connection{
		AccessKeyID:     "test-access",
		SecretAccessKey: "test-secret",
		SessionToken:    "test-session-token",
		EndpointURL:     server.URL,
		Region:          "eu-north-1",
	}, "folder/file.csv")

	require.NoError(t, err)
	assert.True(t, called.Load())
	assert.Contains(t, output.String(), "addressing: path")
	assert.Contains(t, output.String(), "Warning: S3 HTTP endpoint disables TLS")
	assert.NotContains(t, output.String(), "test-secret")
	assert.NotContains(t, output.String(), "test-session-token")
}

func TestKeySensor_CustomEndpointWildcardPagination(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/test-bucket", r.URL.Path)
		assert.Equal(t, "reports/", r.URL.Query().Get("prefix"))
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("continuation-token") == "next-page" {
			fmt.Fprint(w, `<ListBucketResult><Name>test-bucket</Name><IsTruncated>false</IsTruncated><Contents><Key>reports/result.csv</Key></Contents></ListBucketResult>`)
			return
		}
		fmt.Fprint(w, `<ListBucketResult><Name>test-bucket</Name><IsTruncated>true</IsTruncated><NextContinuationToken>next-page</NextContinuationToken><Contents><Key>reports/result.txt</Key></Contents></ListBucketResult>`)
	}))
	defer server.Close()

	err := runEndpointSensor(t.Context(), endpointConnection(server.URL), "reports/*.csv")
	require.NoError(t, err)
	assert.Equal(t, int32(2), requests.Load())
}

func TestKeySensor_CustomEndpointStatusHandling(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		errorMatch string
	}{
		{name: "missing", status: http.StatusNotFound, errorMatch: "Sensor didn't return the expected result"},
		{name: "forbidden", status: http.StatusForbidden, errorMatch: "failed to check object existence"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
			}))
			defer server.Close()

			err := runEndpointSensor(t.Context(), endpointConnection(server.URL), "missing.csv")
			require.ErrorContains(t, err, tt.errorMatch)
		})
	}
}

func TestKeySensor_CustomEndpointCABundle(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/test-bucket/file.csv", r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	withoutBundle := endpointConnection(server.URL)
	err := runEndpointSensor(t.Context(), withoutBundle, "file.csv")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate signed by unknown authority")

	certificate, err := x509.ParseCertificate(server.Certificate().Raw)
	require.NoError(t, err)
	bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
	bundlePath := t.TempDir() + "/ca.pem"
	require.NoError(t, os.WriteFile(bundlePath, bundle, 0o600))

	withBundle := endpointConnection(server.URL)
	withBundle.CABundle = bundlePath
	require.NoError(t, runEndpointSensor(t.Context(), withBundle, "file.csv"))
}

func TestKeySensor_CustomEndpointValidationDoesNotLeakCredentials(t *testing.T) {
	useHTTP := false
	useHTTPS := true
	tests := []struct {
		name       string
		connection config.S3Connection
		message    string
	}{
		{name: "invalid endpoint", connection: config.S3Connection{EndpointURL: "://secret-access:secret-key@example.com"}, message: "endpoint_url must be an HTTP(S) URL"},
		{name: "endpoint credentials", connection: config.S3Connection{EndpointURL: "https://secret-access:secret-key@example.com"}, message: "endpoint_url must be an HTTP(S) URL"},
		{name: "invalid style", connection: config.S3Connection{URLStyle: "secret-style"}, message: "url_style must be 'path' or 'vhost'"},
		{name: "https marked non SSL", connection: config.S3Connection{EndpointURL: "https://example.com", UseSSL: &useHTTP}, message: "use_ssl must agree"},
		{name: "http marked SSL", connection: config.S3Connection{EndpointURL: "http://example.com", UseSSL: &useHTTPS}, message: "use_ssl must agree"},
		{name: "non SSL without endpoint", connection: config.S3Connection{UseSSL: &useHTTP}, message: "requires an explicit HTTP endpoint_url"},
		{name: "incomplete static credentials", connection: config.S3Connection{EndpointURL: "http://example.com", AccessKeyID: "secret-access"}, message: "access_key_id and secret_access_key must both be supplied"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := &bytes.Buffer{}
			ctx := context.WithValue(t.Context(), executor.KeyPrinter, output)
			err := runEndpointSensor(ctx, tt.connection, "file.csv")
			require.ErrorContains(t, err, tt.message)
			combined := err.Error() + output.String()
			assert.NotContains(t, combined, "secret-access")
			assert.NotContains(t, combined, "secret-key")
		})
	}
}

func TestKeySensor_CustomEndpointUsesDefaultCredentialChain(t *testing.T) {
	// Environment mutation makes this test intentionally non-parallel.
	t.Setenv("AWS_ACCESS_KEY_ID", "chain-access")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "chain-secret")
	t.Setenv("AWS_SESSION_TOKEN", "chain-token")
	t.Setenv("AWS_REGION", "ap-southeast-2")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Contains(t, r.Header.Get("Authorization"), "Credential=chain-access/")
		assert.Contains(t, r.Header.Get("Authorization"), "/ap-southeast-2/s3/aws4_request")
		assert.Equal(t, "chain-token", r.Header.Get("X-Amz-Security-Token"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	conn := config.S3Connection{EndpointURL: server.URL}
	require.NoError(t, runEndpointSensor(t.Context(), conn, "file.csv"))
}

func TestKeySensor_VirtualHostEndpoint(t *testing.T) {
	// A subprocess isolates Go's cached proxy environment. The proxy captures the
	// actual SDK request without DNS or any connection to an external provider.
	if os.Getenv("BRUIN_S3_VHOST_TEST") == "1" {
		conn := endpointConnection("http://objects.example.test")
		conn.URLStyle = "vhost"
		require.NoError(t, runEndpointSensor(t.Context(), conn, "folder/a b.csv"))
		return
	}
	var called atomic.Bool
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called.Store(true)
		assert.Equal(t, "test-bucket.objects.example.test", r.Host)
		assert.Equal(t, "/folder/a%20b.csv", r.URL.EscapedPath())
		w.WriteHeader(http.StatusOK)
	}))
	defer proxy.Close()
	t.Setenv("HTTP_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	t.Setenv("BRUIN_S3_VHOST_TEST", "1")
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestKeySensor_VirtualHostEndpoint$")
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	assert.True(t, called.Load())
}

func endpointConnection(endpoint string) config.S3Connection {
	return config.S3Connection{
		AccessKeyID:     "test-access",
		SecretAccessKey: "test-secret",
		EndpointURL:     endpoint,
		Region:          "us-west-2",
	}
}

func runEndpointSensor(ctx context.Context, connection config.S3Connection, key string) error {
	connection.Name = "s3-test"
	sensor := NewKeySensor(&mockConnectionGetter{details: &connection}, "once")
	asset := &pipeline.Asset{
		Connection: "s3-test",
		Parameters: pipeline.ParameterMap{
			"bucket_name": "test-bucket",
			"bucket_key":  key,
		},
	}
	return sensor.RunTask(ctx, &pipeline.Pipeline{}, asset)
}
