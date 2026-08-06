package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFilledFieldsForConnection(t *testing.T) {
	t.Parallel()

	c := &Connections{
		Linear: []LinearConnection{{
			ConnectionMetadata: ConnectionMetadata{Name: "my-linear"},
			APIKey:             "secret",
		}},
		Notion: []NotionConnection{{
			ConnectionMetadata: ConnectionMetadata{Name: "empty-notion"},
		}},
	}

	assert.Equal(t, []string{"api_key"}, c.FilledFieldsForConnection("my-linear"))
	assert.Empty(t, c.FilledFieldsForConnection("empty-notion"))
	assert.Nil(t, c.FilledFieldsForConnection("nope"))
}
