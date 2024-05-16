package entity

import (
	path2 "github.com/bruin-data/bruin/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type Attribute struct {
	Name        string `json:"name" yaml:"name"`
	Description string `json:"description" yaml:"description"`
	Type        string `json:"type" yaml:"type"`
}

type Entity struct {
	Name        string                `json:"name" yaml:"name"`
	Description string                `json:"description" yaml:"description"`
	Attributes  map[string]*Attribute `json:"attributes" yaml:"attributes"`
}

func LoadEntitiesFromFile(path string) ([]*Entity, error) {
	entities := make(map[string]Entity, 0)
	err := path2.ReadYaml(afero.NewOsFs(), path, &entities)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read entities")
	}

	result := make([]*Entity, len(entities))
	idx := 0
	for name, entity := range entities {
		for attrName, attr := range entity.Attributes {
			if attr.Name == "" {
				attr.Name = attrName
			}
		}

		if entity.Name == "" {
			entity.Name = name
		}

		result[idx] = &entity
		idx++
	}

	return result, nil
}
