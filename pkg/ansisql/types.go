package ansisql

import (
	"context"
	"fmt"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

type MetadataHandler interface {
	ExecuteMetadataOperations(ctx context.Context, asset *pipeline.Asset) error
}

type DBDatabase struct {
	Name    string      `json:"name"`
	Schemas []*DBSchema `json:"schemas"`
}

func (d *DBDatabase) TableExists(schema, table string) bool {
	for _, schemaInstance := range d.Schemas {
		if schemaInstance.Name == schema {
			for _, tableInstance := range schemaInstance.Tables {
				if tableInstance.Name == table {
					return true
				}
			}
		}
	}
	return false
}

type DBSchema struct {
	Name   string     `json:"name"`
	Tables []*DBTable `json:"tables"`
}

type DBTable struct {
	Name    string      `json:"name"`
	Columns []*DBColumn `json:"columns"`
}

type DBColumn struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primary_key"`
	Unique     bool   `json:"unique"`
}

type DBColumnType struct {
	Name      string `json:"name"`
	Size      int    `json:"size"`
	Precision int    `json:"precision"`
	Scale     int    `json:"scale"`
}

type TableSummaryResult struct {
	RowCount int64    `json:"row_count"`
	Table    *DBTable `json:"table"`
}

func (tsr *TableSummaryResult) String() string {
	if tsr == nil {
		return "<nil summary>"
	}
	tableName := "<nil>"
	if tsr.Table != nil {
		tableName = tsr.Table.Name
	}
	return fmt.Sprintf("Table: %s, RowCount: %d", tableName, tsr.RowCount)
}
