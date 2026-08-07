package s3

import (
	"context"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		Parameters: pipeline.ParameterMap{},
	}
	err := ks.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "bucket_name")
}

func TestKeySensor_RunTask_MissingBucketKey(t *testing.T) {
	t.Parallel()

	ks := NewKeySensor(&mockConnectionGetter{}, "once")
	asset := &pipeline.Asset{
		Parameters: pipeline.ParameterMap{
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
		Parameters: pipeline.ParameterMap{
			"bucket_name": "my-bucket",
			"bucket_key":  "path/to/file.csv",
		},
	}
	err := ks.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "connection 'my-conn' not found in config file '.bruin.yml' under environment 'default'")
}

func TestKeySensor_RunTask_WrongConnectionType(t *testing.T) {
	t.Parallel()

	conn := &mockConnectionGetter{
		details: "not-a-valid-connection-type",
	}
	ks := NewKeySensor(conn, "once")
	asset := &pipeline.Asset{
		Connection: "my-conn",
		Parameters: pipeline.ParameterMap{
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
			ConnectionMetadata: config.ConnectionMetadata{Name: "my-conn"},
			AccessKey:          "test-access-key",
			SecretKey:          "test-secret-key",
			Region:             "us-west-2",
		},
	}
	ks := NewKeySensor(conn, "once")
	asset := &pipeline.Asset{
		Connection: "my-conn",
		Parameters: pipeline.ParameterMap{
			"bucket_name": "my-bucket",
			"bucket_key":  "path/to/file.csv",
		},
	}

	// Use a short context timeout to avoid waiting for AWS SDK retries against fake credentials.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()

	err := ks.RunTask(ctx, &pipeline.Pipeline{}, asset)
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "does not exist")
	assert.NotContains(t, err.Error(), "not an AWS/S3 connection")
}

func TestKeySensor_RunTask_S3ConnectionUsesCorrectCredentials(t *testing.T) {
	t.Parallel()

	conn := &mockConnectionGetter{
		details: &config.S3Connection{
			ConnectionMetadata: config.ConnectionMetadata{Name: "my-s3-conn"},
			AccessKeyID:        "test-access-key",
			SecretAccessKey:    "test-secret-key",
			EndpointURL:        "http://localhost:9000",
		},
	}
	ks := NewKeySensor(conn, "once")
	asset := &pipeline.Asset{
		Connection: "my-s3-conn",
		Parameters: pipeline.ParameterMap{
			"bucket_name": "my-bucket",
			"bucket_key":  "path/to/file.csv",
		},
	}

	// Use a short context timeout to avoid waiting for AWS SDK retries against a non-existent endpoint.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Millisecond)
	defer cancel()

	err := ks.RunTask(ctx, &pipeline.Pipeline{}, asset)
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
