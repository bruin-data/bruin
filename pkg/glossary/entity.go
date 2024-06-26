package glossary

import (
	path2 "github.com/bruin-data/bruin/pkg/path"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"os"
	"path"
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

type GlossaryReader struct {
	RootPath  string
	FileNames []string

	glossary *Glossary
}

type Glossary struct {
	Entities []*Entity `yaml:"entities"`
}

type glossaryYaml struct {
	Entities map[string]*Entity `yaml:"entities"`
}

func (g *Glossary) Merge(anotherGlossary *Glossary) {
	if g.Entities == nil {
		g.Entities = make([]*Entity, 0)
	}

	g.Entities = append(g.Entities, anotherGlossary.Entities...)
}

func (r *GlossaryReader) GetGlossary() (*Glossary, error) {
	if r.glossary != nil {
		return r.glossary, nil
	}

	var glossary Glossary

	for _, fileName := range r.FileNames {
		pathToLook := path.Join(r.RootPath, fileName)

		entitiesFromFile, err := LoadGlossaryFromFile(pathToLook)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}

			return nil, errors.Wrap(err, "failed to load entities from file")
		}

		glossary.Merge(entitiesFromFile)
	}

	r.glossary = &glossary

	return r.glossary, nil
}

func (r *GlossaryReader) GetEntities() ([]*Entity, error) {
	if r.glossary == nil {
		_, err := r.GetGlossary()
		if err != nil {
			return nil, err
		}
	}

	return r.glossary.Entities, nil
}

func LoadGlossaryFromFile(path string) (*Glossary, error) {
	var glossary glossaryYaml
	err := path2.ReadYaml(afero.NewCacheOnReadFs(afero.NewOsFs(), afero.NewMemMapFs(), 30), path, &glossary)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read entities")
	}

	result := make([]*Entity, len(glossary.Entities))
	idx := 0
	for name, entity := range glossary.Entities {
		for attrName, attr := range entity.Attributes {
			if attr.Name == "" {
				attr.Name = attrName
			}
		}

		if entity.Name == "" {
			entity.Name = name
		}

		result[idx] = entity
		idx++
	}

	return &Glossary{
		Entities: result,
	}, nil
}
