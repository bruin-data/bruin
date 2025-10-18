package helpers

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/afero"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

func GetIngestrDestinationType(asset *pipeline.Asset) (pipeline.AssetType, error) {
	ingestrDestination, ok := asset.Parameters["destination"]
	if !ok {
		return "", errors.New("`destination` parameter not found")
	}

	value, ok := pipeline.IngestrTypeConnectionMapping[ingestrDestination]
	if !ok {
		return "", fmt.Errorf("unknown destination %s", ingestrDestination)
	}

	return value, nil
}

func PrefixGenerator() string {
	// Always return same when testing
	if flag.Lookup("test.v") != nil {
		return "abcefghi"
	}

	letters := []rune("abcdefghijklmnopqrstuvwxyz")
	b := make([]rune, 8)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))] //nolint:all
	}
	return string(b)
}

func CastResultToInteger(res [][]interface{}) (int64, error) {
	if len(res) != 1 || len(res[0]) != 1 {
		return 0, errors.Errorf("multiple results are returned from query, please make sure your query just expects one value - value: %v", res)
	}

	switch v := res[0][0].(type) {
	case nil:
		return 0, errors.Errorf("unexpected result from query, result is nil")
	case float64:
		return int64(v), nil
	case float32:
		return int64(v), nil
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > math.MaxInt64 {
			return 0, errors.Errorf("uint64 value %d overflows int64", v)
		}
		return int64(v), nil
	case uint:
		if uint64(v) > math.MaxInt64 {
			return 0, errors.Errorf("uint value %d overflows int64", v)
		}
		// #nosec G115: overflow is checked above
		return int64(v), nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil

	case string:
		atoi, err := strconv.Atoi(v)
		if err == nil {
			return int64(atoi), nil
		}

		floatValue, err := strconv.ParseFloat(v, 64)
		if err == nil {
			return int64(floatValue), nil
		}

		boolValue, err := strconv.ParseBool(v)
		if err == nil {
			if boolValue {
				return 1, nil
			}

			return 0, nil
		}

		return 0, errors.Errorf("unexpected result from query, cannot cast result string to integer: %v", res)
	}

	return 0, errors.Errorf("unexpected result from query during, cannot cast result to integer: %v", res)
}

func WriteJSONToFile(fs afero.Fs, data interface{}, filename string) error {
	file, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	if _, err := fs.Stat(filepath.Dir(filename)); os.IsNotExist(err) {
		err := fs.MkdirAll(filepath.Dir(filename), 0o755)
		if err != nil {
			return err
		}
	}
	err = afero.WriteFile(fs, filename, file, 0o600)
	if err != nil {
		return err
	}
	return nil
}

func ReadJSONToFile(fs afero.Fs, filename string, v interface{}) error {
	file, err := fs.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	err = decoder.Decode(v)
	if err != nil {
		return err
	}

	return nil
}

func GetAllFilesInDir(fs afero.Fs, dir string) ([]string, error) {
	filesInfo, err := afero.ReadDir(fs, dir)
	if err != nil {
		return nil, err
	}

	files := make([]string, 0, len(filesInfo)) // Pre-allocate the slice
	for _, fileInfo := range filesInfo {
		files = append(files, filepath.Join(dir, fileInfo.Name()))
	}
	return files, nil
}

func GetLatestFileInDir(fs afero.Fs, dir string) (string, error) {
	files, err := GetAllFilesInDir(fs, dir)
	if err != nil {
		return "", err
	}

	var latestFile string
	var latestModTime time.Time

	for _, file := range files {
		info, err := fs.Stat(file)
		if err != nil {
			return "", err
		}

		if info.ModTime().After(latestModTime) {
			latestModTime = info.ModTime()
			latestFile = file
		}
	}

	if latestFile == "" {
		return "", errors.New("no files found in directory")
	}
	return latestFile, nil
}

func ReadFile(path string) string {
	content, err := os.ReadFile(path)
	if err != nil {
		panic(err)
	}
	return string(content)
}

func GetExitCode(err error) int {
	if err == nil {
		return 0
	}
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return exitError.ExitCode()
	}
	return -1
}

func ParseJSONOutputs(actual, expected string) (interface{}, interface{}, error) {
	var actualData, expectedData interface{}

	if err := json.Unmarshal([]byte(actual), &actualData); err != nil {
		return nil, nil, fmt.Errorf("failed to parse actual output as JSON: %w", err)
	}

	if err := json.Unmarshal([]byte(expected), &expectedData); err != nil {
		return nil, nil, fmt.Errorf("failed to parse expected output as JSON: %w", err)
	}

	return actualData, expectedData, nil
}

func TrimToLength(s string, maxLength int) string {
	runes := []rune(s)
	if len(runes) > maxLength {
		return string(runes[:maxLength]) + "..."
	}
	return s
}

func GetPokeInterval(ctx context.Context, t *pipeline.Asset) int64 {
	pokeIntervalStr, ok := t.Parameters["poke_interval"]
	var pokeInterval int64
	if ok {
		var err error
		pokeInterval, err = strconv.ParseInt(pokeIntervalStr, 10, 64)
		if err != nil {
			pokeInterval = 30
		}
	} else {
		pokeInterval = 30
	}
	return pokeInterval
}
