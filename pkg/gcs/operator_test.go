package gcs

import (
	"context"
	"errors"
	"testing"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectSensorValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		asset     *pipeline.Asset
		wantError string
	}{
		{
			name: "missing bucket",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeGCSObjectSensor,
				Parameters: pipeline.ParameterMap{"object": "folder/file.csv"},
			},
			wantError: "parameter named 'bucket'",
		},
		{
			name: "missing object",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeGCSObjectSensor,
				Parameters: pipeline.ParameterMap{"bucket": "bucket"},
			},
			wantError: "parameter named 'object'",
		},
		{
			name: "missing prefix",
			asset: &pipeline.Asset{
				Type:       pipeline.AssetTypeGCSPrefixSensor,
				Parameters: pipeline.ParameterMap{"bucket": "bucket"},
			},
			wantError: "parameter named 'prefix'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sensor := NewObjectSensor(&mockConnectionGetter{}, "once")
			err := sensor.RunTask(t.Context(), &pipeline.Pipeline{}, tt.asset)
			require.ErrorContains(t, err, tt.wantError)
		})
	}
}

func TestObjectSensorSkipMode(t *testing.T) {
	t.Parallel()

	sensor := NewObjectSensor(&mockConnectionGetter{}, "skip")
	require.NoError(t, sensor.RunTask(t.Context(), &pipeline.Pipeline{}, &pipeline.Asset{}))
}

func TestObjectSensorConnectionNotFound(t *testing.T) {
	t.Parallel()

	sensor := NewObjectSensor(&mockConnectionGetter{}, "once")
	asset := objectSensorAsset(pipeline.AssetTypeGCSObjectSensor, pipeline.ParameterMap{
		"bucket": "bucket",
		"object": "folder/file.csv",
	})
	err := sensor.RunTask(t.Context(), &pipeline.Pipeline{}, asset)
	require.ErrorContains(t, err, "connection 'gcs-test' not found")
}

func TestObjectSensorObjectAliasesAndWildcard(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{patternResults: []bool{true}}
	sensor := testObjectSensor(store, "once")
	asset := objectSensorAsset(pipeline.AssetTypeGCSObjectSensor, pipeline.ParameterMap{
		"bucket_name": "bucket",
		"bucket_key":  "exports/*.csv",
	})

	require.NoError(t, sensor.RunTask(t.Context(), &pipeline.Pipeline{}, asset))
	assert.Equal(t, []objectCall{{bucket: "bucket", target: "exports/*.csv"}}, store.patternCalls)
	assert.True(t, store.closed)
}

func TestObjectSensorExactObject(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{objectResults: []bool{true}}
	sensor := testObjectSensor(store, "once")
	asset := objectSensorAsset(pipeline.AssetTypeGCSObjectSensor, pipeline.ParameterMap{
		"bucket": "bucket",
		"object": "folder/file.csv",
	})

	require.NoError(t, sensor.RunTask(t.Context(), &pipeline.Pipeline{}, asset))
	assert.Equal(t, []objectCall{{bucket: "bucket", target: "folder/file.csv"}}, store.objectCalls)
}

func TestObjectSensorPrefixTypes(t *testing.T) {
	t.Parallel()

	for _, assetType := range []pipeline.AssetType{
		pipeline.AssetTypeGCSPrefixSensor,
		pipeline.AssetTypeGCSPrefixSensorLegacy,
	} {
		t.Run(string(assetType), func(t *testing.T) {
			t.Parallel()
			store := &fakeObjectStore{prefixResults: []bool{true}}
			sensor := testObjectSensor(store, "once")
			asset := objectSensorAsset(assetType, pipeline.ParameterMap{
				"bucket": "bucket",
				"prefix": "incoming/",
			})

			require.NoError(t, sensor.RunTask(t.Context(), &pipeline.Pipeline{}, asset))
			assert.Equal(t, []objectCall{{bucket: "bucket", target: "incoming/"}}, store.prefixCalls)
		})
	}
}

func TestObjectSensorOnceModeNotFound(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{objectResults: []bool{false}}
	sensor := testObjectSensor(store, "once")
	err := sensor.RunTask(t.Context(), &pipeline.Pipeline{}, objectSensorAsset(
		pipeline.AssetTypeGCSObjectSensor,
		pipeline.ParameterMap{"bucket": "bucket", "object": "missing.csv"},
	))
	require.ErrorContains(t, err, "expected result")
}

func TestObjectSensorWaitMode(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{objectResults: []bool{false, true}}
	sensor := testObjectSensor(store, "wait")
	err := sensor.RunTask(t.Context(), &pipeline.Pipeline{}, objectSensorAsset(
		pipeline.AssetTypeGCSObjectSensor,
		pipeline.ParameterMap{
			"bucket":        "bucket",
			"object":        "event.json",
			"poke_interval": "0",
		},
	))
	require.NoError(t, err)
	assert.Len(t, store.objectCalls, 2)
}

func TestObjectSensorTimeout(t *testing.T) {
	t.Parallel()

	store := &fakeObjectStore{objectResults: []bool{false}}
	sensor := testObjectSensor(store, "wait")
	err := sensor.RunTask(t.Context(), &pipeline.Pipeline{}, objectSensorAsset(
		pipeline.AssetTypeGCSObjectSensor,
		pipeline.ParameterMap{
			"bucket":        "bucket",
			"object":        "event.json",
			"poke_interval": "30",
			"timeout":       "1ms",
		},
	))
	require.ErrorContains(t, err, "timed out after 1ms")
}

func TestObjectSensorReturnsStoreError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("storage unavailable")
	store := &fakeObjectStore{err: wantErr}
	sensor := testObjectSensor(store, "once")
	err := sensor.RunTask(t.Context(), &pipeline.Pipeline{}, objectSensorAsset(
		pipeline.AssetTypeGCSObjectSensor,
		pipeline.ParameterMap{"bucket": "bucket", "object": "event.json"},
	))
	require.ErrorIs(t, err, wantErr)
}

func TestStorageClientOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connection any
		wantCount  int
		wantError  string
	}{
		{
			name: "GCS ADC",
			connection: &config.GCSConnection{
				UseApplicationDefaultCredentials: true,
			},
		},
		{
			name: "GCP ADC",
			connection: &config.GoogleCloudPlatformConnection{
				UseApplicationDefaultCredentials: true,
			},
		},
		{
			name: "service account JSON",
			connection: &config.GCSConnection{
				ServiceAccountJSON: `{}`,
			},
			wantCount: 1,
		},
		{
			name:       "wrong connection type",
			connection: "wrong",
			wantError:  "not a GCS or Google Cloud Platform connection",
		},
		{
			name:       "missing credentials",
			connection: &config.GCSConnection{},
			wantError:  "GCS credentials are required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := storageClientOptions(tt.connection)
			if tt.wantError != "" {
				require.ErrorContains(t, err, tt.wantError)
				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func objectSensorAsset(assetType pipeline.AssetType, parameters pipeline.ParameterMap) *pipeline.Asset {
	return &pipeline.Asset{
		Type:       assetType,
		Connection: "gcs-test",
		Parameters: parameters,
	}
}

func testObjectSensor(store objectStore, sensorMode string) *ObjectSensor {
	sensor := NewObjectSensor(&mockConnectionGetter{
		details: &config.GCSConnection{UseApplicationDefaultCredentials: true},
	}, sensorMode)
	sensor.newStore = func(context.Context, any) (objectStore, error) {
		return store, nil
	}
	return sensor
}

type objectCall struct {
	bucket string
	target string
}

type fakeObjectStore struct {
	objectResults  []bool
	patternResults []bool
	prefixResults  []bool
	err            error
	objectCalls    []objectCall
	patternCalls   []objectCall
	prefixCalls    []objectCall
	closed         bool
}

func (f *fakeObjectStore) ObjectExists(_ context.Context, bucket, object string) (bool, error) {
	f.objectCalls = append(f.objectCalls, objectCall{bucket: bucket, target: object})
	return nextResult(&f.objectResults), f.err
}

func (f *fakeObjectStore) ObjectMatchingPatternExists(
	_ context.Context,
	bucket,
	pattern string,
) (bool, error) {
	f.patternCalls = append(f.patternCalls, objectCall{bucket: bucket, target: pattern})
	return nextResult(&f.patternResults), f.err
}

func (f *fakeObjectStore) ObjectWithPrefixExists(
	_ context.Context,
	bucket,
	prefix string,
) (bool, error) {
	f.prefixCalls = append(f.prefixCalls, objectCall{bucket: bucket, target: prefix})
	return nextResult(&f.prefixResults), f.err
}

func (f *fakeObjectStore) Close() error {
	f.closed = true
	return nil
}

func nextResult(results *[]bool) bool {
	if len(*results) == 0 {
		return false
	}
	result := (*results)[0]
	if len(*results) > 1 {
		*results = (*results)[1:]
	}
	return result
}

type mockConnectionGetter struct {
	details any
}

func (m *mockConnectionGetter) GetConnection(string) any {
	return m.details
}

func (m *mockConnectionGetter) GetConnectionDetails(string) any {
	return m.details
}

func (m *mockConnectionGetter) GetConnectionType(string) string {
	return ""
}
