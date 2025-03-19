package devenv

import (
	"github.com/bruin-data/bruin/pkg/query"
)

type queryExtractor interface {
	ExtractQueriesFromString(content string) ([]*query.Query, error)
}

type sqlParser interface {
}

type DevEnvAwareExtractor struct {
	extractor    queryExtractor
	schemaPrefix string
}

func (d *DevEnvAwareExtractor) ExtractQueriesFromString(filepath string) ([]*query.Query, error) {
	queries, err := d.extractor.ExtractQueriesFromString(filepath)
	if err != nil {
		return nil, err
	}
	for _, q := range queries {
		q.Query = d.schemaPrefix + q.Query
	}
	return queries, nil
}
