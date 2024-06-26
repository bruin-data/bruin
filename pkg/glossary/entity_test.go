package glossary

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGlossary_Merge(t *testing.T) {
	mainGlossary := &Glossary{}

	anotherGlossary := &Glossary{
		Entities: []*Entity{
			{
				Name:        "entity1",
				Description: "entity1 description",
				Attributes: map[string]*Attribute{
					"attr1": {
						Name:        "attr1",
						Description: "attr1 description",
					},
				},
			},
		},
	}

	mainGlossary.Merge(anotherGlossary)

	assert.Equal(t, 1, len(mainGlossary.Entities))
	assert.Equal(t, anotherGlossary, mainGlossary)
}
