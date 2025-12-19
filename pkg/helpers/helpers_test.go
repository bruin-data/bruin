package helpers

import (
	"encoding/json"
	"math"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetIngestrDestinationType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		asset   *pipeline.Asset
		want    pipeline.AssetType
		wantErr bool
	}{
		{
			name: "postgres",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"destination": "postgres",
				},
			},
			want: pipeline.AssetTypePostgresQuery,
		},
		{
			name: "gcp",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"destination": "bigquery",
				},
			},
			want: pipeline.AssetTypeBigqueryQuery,
		},
		{
			name: "not found",
			asset: &pipeline.Asset{
				Parameters: map[string]string{
					"destination": "sqlite",
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assetType, err := GetIngestrDestinationType(tc.asset)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				assert.Equal(t, tc.want, assetType)
			}
		})
	}
}

func TestWriteJSONToFile(t *testing.T) {
	t.Parallel()

	type testData struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	data := testData{
		Name:  "Test",
		Value: 123,
	}

	filename := "test_output.json"

	fs := afero.NewMemMapFs()

	err := WriteJSONToFile(fs, data, filename)
	require.NoError(t, err, "WriteJSONToFileWithFs should not return an error")

	_, err = fs.Stat(filename)
	require.NoError(t, err, "File should exist after writing")

	fileContent, err := afero.ReadFile(fs, filename)
	require.NoError(t, err, "Should be able to read the file")

	expectedContent, err := json.MarshalIndent(data, "", "  ")
	require.NoError(t, err, "Should be able to marshal expected data")

	assert.Equal(t, string(expectedContent), string(fileContent), "File content should match expected JSON")
}

func TestReadJSONToFile(t *testing.T) {
	t.Parallel()

	type testData struct {
		Name  string `json:"name"`
		Value int    `json:"value"`
	}

	expectedData := testData{
		Name:  "Test",
		Value: 123,
	}

	filename := "test_input.json"

	fs := afero.NewMemMapFs()

	fileContent, err := json.MarshalIndent(expectedData, "", "  ")
	require.NoError(t, err, "Should be able to marshal expected data")

	err = afero.WriteFile(fs, filename, fileContent, 0o644)
	require.NoError(t, err, "Should be able to write file to in-memory filesystem")

	var actualData testData
	err = ReadJSONToFile(fs, filename, &actualData)
	require.NoError(t, err, "ReadJSONToFile should not return an error")

	assert.Equal(t, expectedData, actualData, "Data read from file should match expected data")
}

// trackingFile wraps an afero.File and records when Close is called.
type trackingFile struct {
	afero.File
	closed *bool
}

func (f *trackingFile) Close() error {
	err := f.File.Close()
	if err == nil {
		*f.closed = true
	}
	return err
}

// trackingFs is an afero filesystem that tracks file closes.
type trackingFs struct {
	afero.Fs
	closed *bool
}

func (fsys *trackingFs) Open(name string) (afero.File, error) { //nolint:ireturn
	f, err := fsys.Fs.Open(name)
	if err != nil {
		return nil, err
	}
	return &trackingFile{File: f, closed: fsys.closed}, nil
}

func TestReadJSONToFileClosesFile(t *testing.T) {
	t.Parallel()

	data := map[string]int{"foo": 1}
	buf, err := json.MarshalIndent(data, "", "  ")
	require.NoError(t, err)

	baseFs := afero.NewMemMapFs()
	const filename = "input.json"
	err = afero.WriteFile(baseFs, filename, buf, 0o644)
	require.NoError(t, err)

	closed := false
	fs := &trackingFs{Fs: baseFs, closed: &closed}

	var out map[string]int
	err = ReadJSONToFile(fs, filename, &out)
	require.NoError(t, err)
	assert.Equal(t, data, out)
	assert.True(t, closed, "file should be closed after ReadJSONToFile")
}

func TestGetLatestFileInDir(t *testing.T) {
	t.Parallel()

	fs := afero.NewMemMapFs()
	dir := "/testdir"

	files := []struct {
		name    string
		modTime time.Time
	}{
		{"file1.txt", time.Now().Add(-3 * time.Hour)},
		{"file2.txt", time.Now().Add(-2 * time.Hour)},
		{"file3.txt", time.Now().Add(-1 * time.Hour)},
	}

	for _, file := range files {
		f, err := fs.Create(filepath.Join(dir, file.name))
		require.NoError(t, err, "Should be able to create file")
		defer f.Close()

		err = fs.Chtimes(filepath.Join(dir, file.name), file.modTime, file.modTime)
		require.NoError(t, err, "Should be able to change file times") // Check the error
	}

	latestFile, err := GetLatestFileInDir(fs, dir)
	require.NoError(t, err, "GetLatestFileInDir should not return an error")

	expectedLatestFile := filepath.Join(dir, "file3.txt")
	assert.Equal(t, expectedLatestFile, latestFile, "Latest file should be the one with the most recent modification time")
}

func TestGetExitCode(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		err      error
		expected int
	}{
		{"No error", nil, 0},
		{"Exit error", &exec.ExitError{}, -1},
		{"Non-exit error", errors.New("some error"), -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := GetExitCode(tt.err); got != tt.expected {
				t.Errorf("getExitCode() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestParseJSONOutputs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		actual, expected string
		wantErr          bool
	}{
		{"Valid JSON", `{"key": "value"}`, `{"key": "value"}`, false},
		{"Invalid actual JSON", `{"key": "value"`, `{"key": "value"}`, true},
		{"Invalid expected JSON", `{"key": "value"}`, `{"key": "value"`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := ParseJSONOutputs(tt.actual, tt.expected)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseJSONOutputs() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestCastResultToInteger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		res        [][]interface{}
		tolerant   bool
		expected   int64
		expectErr  bool
		errMessage string
	}{
		{
			name:      "empty result tolerant",
			res:       [][]interface{}{},
			tolerant:  true,
			expected:  0,
			expectErr: false,
		},
		{
			name:       "empty result not tolerant",
			res:        [][]interface{}{},
			tolerant:   false,
			expectErr:  true,
			errMessage: "multiple results are returned from query",
		},
		{
			name:       "multiple rows",
			res:        [][]interface{}{{1}, {2}},
			tolerant:   false,
			expectErr:  true,
			errMessage: "multiple results are returned from query",
		},
		{
			name:       "multiple columns",
			res:        [][]interface{}{{1, 2}},
			tolerant:   false,
			expectErr:  true,
			errMessage: "multiple results are returned from query",
		},
		{
			name:      "nil value tolerant",
			res:       [][]interface{}{{nil}},
			tolerant:  true,
			expected:  0,
			expectErr: false,
		},
		{
			name:       "nil value not tolerant",
			res:        [][]interface{}{{nil}},
			tolerant:   false,
			expectErr:  true,
			errMessage: "unexpected result from query, result is nil",
		},
		{
			name:      "float64 value",
			res:       [][]interface{}{{float64(42.7)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "float32 value",
			res:       [][]interface{}{{float32(42.7)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "int8 value",
			res:       [][]interface{}{{int8(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "int16 value",
			res:       [][]interface{}{{int16(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "int32 value",
			res:       [][]interface{}{{int32(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "int64 value",
			res:       [][]interface{}{{int64(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "int value",
			res:       [][]interface{}{{int(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "uint8 value",
			res:       [][]interface{}{{uint8(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "uint16 value",
			res:       [][]interface{}{{uint16(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "uint32 value",
			res:       [][]interface{}{{uint32(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "uint64 value within range",
			res:       [][]interface{}{{uint64(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:       "uint64 value overflow",
			res:        [][]interface{}{{uint64(math.MaxInt64) + 1}},
			tolerant:   false,
			expectErr:  true,
			errMessage: "uint64 value",
		},
		{
			name:      "uint value within range",
			res:       [][]interface{}{{uint(42)}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:       "uint value overflow",
			res:        [][]interface{}{{uint(math.MaxInt64) + 1}},
			tolerant:   false,
			expectErr:  true,
			errMessage: "uint value",
		},
		{
			name:      "bool true",
			res:       [][]interface{}{{true}},
			tolerant:  false,
			expected:  1,
			expectErr: false,
		},
		{
			name:      "bool false",
			res:       [][]interface{}{{false}},
			tolerant:  false,
			expected:  0,
			expectErr: false,
		},
		{
			name:      "string integer",
			res:       [][]interface{}{{"42"}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "string negative integer",
			res:       [][]interface{}{{"-42"}},
			tolerant:  false,
			expected:  -42,
			expectErr: false,
		},
		{
			name:      "string float",
			res:       [][]interface{}{{"42.7"}},
			tolerant:  false,
			expected:  42,
			expectErr: false,
		},
		{
			name:      "string bool true",
			res:       [][]interface{}{{"true"}},
			tolerant:  false,
			expected:  1,
			expectErr: false,
		},
		{
			name:      "string bool false",
			res:       [][]interface{}{{"false"}},
			tolerant:  false,
			expected:  0,
			expectErr: false,
		},
		{
			name:       "string invalid",
			res:        [][]interface{}{{"invalid"}},
			tolerant:   false,
			expectErr:  true,
			errMessage: "cannot cast result string to integer",
		},
		{
			name:       "unsupported type",
			res:        [][]interface{}{{[]int{1, 2, 3}}},
			tolerant:   false,
			expectErr:  true,
			errMessage: "cannot cast result to integer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result, err := CastResultToInteger(tt.res, tt.tolerant)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errMessage != "" {
					assert.Contains(t, err.Error(), tt.errMessage)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}
