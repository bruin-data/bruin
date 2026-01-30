package ansisql

import (
	"fmt"
	"time"
)

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

// DBTableType represents the type of a database table.
type DBTableType string

const (
	// DBTableTypeTable represents a regular table.
	DBTableTypeTable DBTableType = "table"
	// DBTableTypeView represents a view.
	DBTableTypeView DBTableType = "view"
)

type DBTable struct {
	Name           string      `json:"name"`
	Type           DBTableType `json:"type,omitempty"`            // "table" or "view"
	ViewDefinition string      `json:"view_definition,omitempty"` // SQL definition for views
	Columns        []*DBColumn `json:"columns"`

	// Metadata fields for import description enrichment
	CreatedAt    *time.Time `json:"created_at,omitempty"`    // When the table was created
	LastModified *time.Time `json:"last_modified,omitempty"` // When the table was last modified
	RowCount     *int64     `json:"row_count,omitempty"`     // Number of rows in the table
	SizeBytes    *int64     `json:"size_bytes,omitempty"`    // Size of the table in bytes
	Description  string     `json:"description,omitempty"`   // Description from the database
	Owner        string     `json:"owner,omitempty"`         // Owner of the table
	TableComment string     `json:"table_comment,omitempty"` // Comment/description on the table
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
