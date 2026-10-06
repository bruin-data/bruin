package semantic

import (
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// UnmarshalYAML accepts either a mapping ({name: order_date, granularity: month})
// or the CLI shorthand string ("order_date:month").
func (d *DimensionRef) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		var raw string
		if err := node.Decode(&raw); err != nil {
			return err
		}
		ref, err := ParseDimensionRef(raw)
		if err != nil {
			return err
		}
		*d = ref
		return nil
	}

	type plain DimensionRef
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*d = DimensionRef(decoded)
	return nil
}

// UnmarshalYAML accepts either a mapping ({name: revenue, direction: desc}) or
// the CLI shorthand string ("revenue:desc").
func (s *SortSpec) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		var raw string
		if err := node.Decode(&raw); err != nil {
			return err
		}
		spec, err := ParseSortSpec(raw)
		if err != nil {
			return err
		}
		*s = spec
		return nil
	}

	type plain SortSpec
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*s = SortSpec(decoded)
	return nil
}

// UnmarshalYAML records whether `value` was written as an explicit null, so
// that it can expect NULL instead of the default zero.
func (c *ModelCheck) UnmarshalYAML(node *yaml.Node) error {
	type plain ModelCheck
	var decoded plain
	if err := node.Decode(&decoded); err != nil {
		return err
	}
	*c = ModelCheck(decoded)
	if node.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			if node.Content[i].Value == "value" && node.Content[i+1].ShortTag() == "!!null" {
				c.valueNull = true
			}
		}
	}
	return nil
}

// ParseDimensionRef parses "name" or "name:granularity".
func ParseDimensionRef(raw string) (DimensionRef, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DimensionRef{}, errors.New("dimension reference is required")
	}
	name, granularity, found := strings.Cut(raw, ":")
	name = strings.TrimSpace(name)
	granularity = strings.TrimSpace(granularity)
	if name == "" || (found && granularity == "") {
		return DimensionRef{}, fmt.Errorf("invalid dimension reference %q; expected name or name:granularity", raw)
	}
	return DimensionRef{Name: name, Granularity: granularity}, nil
}

// ParseSortSpec parses "name", "name:asc", or "name:desc".
func ParseSortSpec(raw string) (SortSpec, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return SortSpec{}, errors.New("sort reference is required")
	}
	name, direction, found := strings.Cut(raw, ":")
	name = strings.TrimSpace(name)
	direction = strings.ToLower(strings.TrimSpace(direction))
	if name == "" {
		return SortSpec{}, fmt.Errorf("invalid sort reference %q; expected name, name:asc, or name:desc", raw)
	}
	if found {
		switch direction {
		case "asc", "desc":
		default:
			return SortSpec{}, fmt.Errorf("invalid sort direction %q for %q; expected asc or desc", direction, name)
		}
	}
	return SortSpec{Name: name, Direction: direction}, nil
}
