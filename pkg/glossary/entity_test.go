package glossary

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		Domains: []*Domain{
			{
				Name:        "domain1",
				Description: "domain1 description",
				Owners:      []string{"owner1"},
				Tags:        []string{"tag1"},
			},
		},
	}

	mainGlossary.Merge(anotherGlossary)

	assert.Len(t, mainGlossary.Entities, 1)
	assert.Len(t, mainGlossary.Domains, 1)
	assert.Equal(t, anotherGlossary, mainGlossary)
}

func TestGlossary_GetDomain(t *testing.T) {
	t.Parallel()

	glossary := &Glossary{
		Domains: []*Domain{
			{
				Name:        "analytics",
				Description: "Analytics domain",
				Owners:      []string{"data-team"},
				Tags:        []string{"analytics"},
			},
			{
				Name:        "finance",
				Description: "Finance domain",
				Owners:      []string{"finance-team"},
				Tags:        []string{"finance"},
			},
		},
	}

	// Test finding existing domain
	domain := glossary.GetDomain("analytics")
	assert.NotNil(t, domain)
	assert.Equal(t, "analytics", domain.Name)
	assert.Equal(t, "Analytics domain", domain.Description)

	// Test finding non-existing domain
	domain = glossary.GetDomain("non-existing")
	assert.Nil(t, domain)
}

func TestLoadGlossaryFromFile_WithDomains(t *testing.T) {
	t.Parallel()

	// Test loading the test glossary file to ensure domains are properly converted from map to array
	glossary, err := LoadGlossaryFromFile("../../test-glossary.yml")
	require.NoError(t, err)
	assert.NotNil(t, glossary)

	// Verify domains were loaded and converted to array
	assert.NotEmpty(t, glossary.Domains)

	// Test that we can find a domain by name
	analyticsDomain := glossary.GetDomain("analytics")
	assert.NotNil(t, analyticsDomain)
	assert.Equal(t, "analytics", analyticsDomain.Name)
	assert.Contains(t, analyticsDomain.Description, "Analytics domain")
}
