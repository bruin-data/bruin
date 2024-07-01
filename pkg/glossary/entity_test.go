package glossary

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGlossary_Merge(t *testing.T) {
	t.Parallel()

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

	assert.Len(t, mainGlossary.Entities, 1)
	assert.Equal(t, anotherGlossary, mainGlossary)
}
