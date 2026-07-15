package pipeline

import (
	"encoding/json"
	"fmt"
	"time"

	"gopkg.in/yaml.v3"
)

// DurationSeconds stores a duration while serializing to JSON as whole seconds.
type DurationSeconds time.Duration

func (d DurationSeconds) Duration() time.Duration {
	return time.Duration(d)
}

func (d DurationSeconds) MarshalJSON() ([]byte, error) {
	seconds := int64(time.Duration(d).Seconds())
	return json.Marshal(seconds)
}

func (d *DurationSeconds) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*d = 0
		return nil
	}

	var seconds json.Number
	if err := json.Unmarshal(data, &seconds); err != nil {
		return err
	}

	intSeconds, err := seconds.Int64()
	if err != nil {
		floatSeconds, floatErr := seconds.Float64()
		if floatErr != nil {
			return err
		}
		*d = DurationSeconds(time.Duration(floatSeconds * float64(time.Second)))
		return nil
	}

	*d = DurationSeconds(time.Duration(intSeconds) * time.Second)
	return nil
}

func (d *DurationSeconds) UnmarshalYAML(value *yaml.Node) error {
	var v time.Duration
	if err := value.Decode(&v); err != nil {
		return err
	}
	*d = DurationSeconds(v)
	return nil
}

func (d DurationSeconds) MarshalYAML() (interface{}, error) {
	if d == 0 {
		return nil, nil
	}

	return time.Duration(d).String(), nil
}

func (d *DurationSeconds) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("failed to parse duration %q: %w", string(text), err)
	}
	*d = DurationSeconds(parsed)
	return nil
}

func (d DurationSeconds) MarshalText() ([]byte, error) {
	if d == 0 {
		return nil, nil
	}

	return []byte(time.Duration(d).String()), nil
}
