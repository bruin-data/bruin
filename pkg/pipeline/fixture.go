package pipeline

import (
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// FixtureRows preserves whether YAML timestamps were written with a timezone.
type FixtureRows []map[string]interface{}

func (r *FixtureRows) UnmarshalYAML(value *yaml.Node) error {
	var rows []map[string]fixtureCell
	if err := value.Decode(&rows); err != nil {
		return err
	}
	*r = make(FixtureRows, len(rows))
	for i, row := range rows {
		if row == nil {
			continue
		}
		(*r)[i] = make(map[string]interface{}, len(row))
		for key, cell := range row {
			(*r)[i][key] = cell.value
		}
	}
	return nil
}

// FixtureTimestamp retains the original scalar so YAML formatting cannot add a
// timezone to a date or a timezone-free timestamp.
type FixtureTimestamp struct {
	time.Time
	literal string
}

func (t FixtureTimestamp) HasTimezone() bool {
	// yaml.v3's zoned timestamp formats use T/t; its timezone-free format uses a space.
	return strings.ContainsAny(t.literal, "Tt")
}

func (t FixtureTimestamp) MarshalYAML() (interface{}, error) {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!timestamp", Value: t.literal}, nil
}

type fixtureCell struct {
	value interface{}
}

func (c *fixtureCell) UnmarshalYAML(value *yaml.Node) error {
	if err := value.Decode(&c.value); err != nil {
		return err
	}
	if t, ok := c.value.(time.Time); ok {
		c.value = FixtureTimestamp{Time: t, literal: value.Value}
	}
	return nil
}
