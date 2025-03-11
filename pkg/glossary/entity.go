package glossary

import (
	"os"
	"path"
	"sync"

	"github.com/bruin-data/bruin/pkg/git"
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

func (e Entity) GetAttribute(name string) *Attribute {
	return e.Attributes[name]
}

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

type GlossaryReader struct {
	FileNames  []string
	RepoFinder repoFinder

	glossary *Glossary
	mutex    sync.Mutex
}

type Glossary struct {
	Entities []*Entity `yaml:"entities" json:"entities"`
}

func (g *Glossary) GetEntity(name string) *Entity {
	for _, entity := range g.Entities {
		if entity.Name == name {
			return entity
		}
	}

	return nil
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

func (r *GlossaryReader) GetGlossary(pipelinePath string) (*Glossary, error) {
	var glossary Glossary

	repo, err := r.RepoFinder.Repo(pipelinePath)
	if err != nil {
		return nil, errors.Wrap(err, "failed to find repo")
	}

	for _, fileName := range r.FileNames {
		pathToLook := path.Join(repo.Path, fileName)

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

func (r *GlossaryReader) GetEntities(pathToPipeline string) ([]*Entity, error) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.glossary == nil {
		_, err := r.GetGlossary(pathToPipeline)
		if err != nil {
			return nil, err
		}
	}

	return r.glossary.Entities, nil
}

func LoadGlossaryFromFile(path string) (*Glossary, error) {
	var glossary glossaryYaml
	err := path2.ReadYaml(afero.NewOsFs(), path, &glossary)
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
