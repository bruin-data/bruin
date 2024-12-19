package helpers

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/pkg/errors"
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
	case int64:
		return v, nil
	case int:
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

func WriteJSONToFile(data interface{}, filename string) error {
	file, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Dir(filename)); os.IsNotExist(err) {
		err := os.MkdirAll(filepath.Dir(filename), 0o755)
		if err != nil {
			return err
		}
	}
	err = os.WriteFile(filename, file, 0o600)
	if err != nil {
		return err
	}
	return nil
}

func ReadJSONToFile(filename string, v interface{}) error {
	file, err := os.ReadFile(filename)
	if err != nil {
		return err
	}

	err = json.Unmarshal(file, v)
	if err != nil {
		return err
	}

	return nil
}
