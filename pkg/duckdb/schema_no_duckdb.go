//go:build bruin_no_duckdb

package duck

import (
	"context"
	"sync"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

type DuckDBSchemaCreator struct {
	schemaNameCache *sync.Map
}

func NewDuckDBSchemaCreator() *DuckDBSchemaCreator {
	return &DuckDBSchemaCreator{
		schemaNameCache: &sync.Map{},
	}
}

func (sc *DuckDBSchemaCreator) CreateSchemaIfNotExist(ctx context.Context, qr queryRunner, asset *pipeline.Asset) error {
	return errDuckDBNotSupported
}
