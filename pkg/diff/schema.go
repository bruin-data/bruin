package diff

import (
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
)

// TypeDifference represents a difference in column types between two tables.
type TypeDifference struct {
	Table1Type           string
	Table2Type           string
	Table1NormalizedType CommonDataType
	Table2NormalizedType CommonDataType
	IsComparable         bool // True if normalized types are the same (e.g., both numeric)
}

// NullabilityDifference represents a difference in nullability between two tables.
type NullabilityDifference struct {
	Table1Nullable bool
	Table2Nullable bool
}

// UniquenessDifference represents a difference in uniqueness constraints between two tables.
type UniquenessDifference struct {
	Table1Unique bool
	Table2Unique bool
}

// ColumnDifference represents differences for a column that exists in both tables.
type ColumnDifference struct {
	ColumnName            string
	TypeDifference        *TypeDifference
	NullabilityDifference *NullabilityDifference
	UniquenessDifference  *UniquenessDifference
}

// MissingColumn represents a column that exists in only one table.
type MissingColumn struct {
	ColumnName  string
	Type        string
	Nullable    bool
	PrimaryKey  bool
	Unique      bool
	TableName   string // The table where this column exists
	MissingFrom string // The table where this column is missing
}

// SchemaComparisonResult holds the detailed results of a table schema comparison.
type SchemaComparisonResult struct {
	Table1 *TableSummaryResult
	Table2 *TableSummaryResult

	SamePropertiesCount      int                // Columns with identical properties in both tables
	DifferentPropertiesCount int                // Columns present in both tables but with different properties
	InTable1OnlyCount        int                // Columns present in table1 but not in table2
	InTable2OnlyCount        int                // Columns present in table2 but not in table1
	ColumnDifferences        []ColumnDifference // Structured differences for columns present in both tables
	MissingColumns           []MissingColumn    // Columns missing from one of the tables
	HasSchemaDifferences     bool               // True if DifferentPropertiesCount, InTable1OnlyCount, or InTable2OnlyCount > 0
	RowCountDiff             int64              // Difference in row count between the two tables
	HasRowCountDifference    bool               // True if RowCountDiff != 0
}

func (c *SchemaComparisonResult) GetSummaryTable() string {
	t := table.NewWriter()
	t.SetStyle(table.StyleRounded)
	t.SetRowPainter(func(row table.Row) text.Colors {
		if len(row) == 0 {
			return text.Colors{}
		}
		if row[0] == "Row Count" {
			if c.RowCountDiff != 0 {
				return text.Colors{text.FgRed}
			} else {
				return text.Colors{text.FgGreen}
			}
		}
		if row[0] == "Column Count" {
			if row[3] != 0 {
				return text.Colors{text.FgRed}
			} else {
				return text.Colors{text.FgGreen}
			}
		}
		return text.Colors{}
	})

	t.AppendHeader(table.Row{"", c.Table1.Table.Name, c.Table2.Table.Name, "Diff"})
	if c.RowCountDiff != 0 {
		t.AppendRow(table.Row{"Row Count", c.Table1.RowCount, c.Table2.RowCount, c.RowCountDiff})
	} else {
		t.AppendRow(table.Row{"Row Count", c.Table1.RowCount, c.Table2.RowCount, 0})
	}
	t.AppendRow(table.Row{"Column Count", len(c.Table1.Table.Columns), len(c.Table2.Table.Columns), len(c.Table1.Table.Columns) - len(c.Table2.Table.Columns)})
	return t.Render()
}

func CompareTableSchemas(summary1, summary2 *TableSummaryResult, t1Name, t2Name string) SchemaComparisonResult {
	res := SchemaComparisonResult{
		Table1: summary1,
		Table2: summary2,
	}

	if summary1 == nil || summary2 == nil {
		return res
	}

	if summary1.Table == nil || summary2.Table == nil {
		return res
	}

	res.RowCountDiff = summary1.RowCount - summary2.RowCount
	res.HasRowCountDifference = res.RowCountDiff != 0

	t1 := summary1.Table
	t2 := summary2.Table

	table1Columns := make(map[string]*Column)
	for _, col := range t1.Columns {
		table1Columns[col.Name] = col
	}

	table2Columns := make(map[string]*Column)
	for _, col := range t2.Columns {
		table2Columns[col.Name] = col
	}

	processedColsT2 := make(map[string]bool) // Tracks columns from t2 that are matched

	// Iterate through columns of table 1
	for _, col1 := range t1.Columns {
		col2, existsInT2 := table2Columns[col1.Name]
		if existsInT2 {
			processedColsT2[col1.Name] = true
			columnIsDifferent := false
			colDiff := ColumnDifference{
				ColumnName: col1.Name,
			}

			// Check if types are different (considering both original and normalized types)
			typesAreDifferent := col1.Type != col2.Type
			normalizedTypesMatch := col1.NormalizedType == col2.NormalizedType

			if typesAreDifferent {
				colDiff.TypeDifference = &TypeDifference{
					Table1Type:           col1.Type,
					Table2Type:           col2.Type,
					Table1NormalizedType: col1.NormalizedType,
					Table2NormalizedType: col2.NormalizedType,
					IsComparable:         normalizedTypesMatch,
				}

				columnIsDifferent = true
			}
			if col1.Nullable != col2.Nullable {
				colDiff.NullabilityDifference = &NullabilityDifference{
					Table1Nullable: col1.Nullable,
					Table2Nullable: col2.Nullable,
				}
				columnIsDifferent = true
			}
			if col1.Unique != col2.Unique {
				colDiff.UniquenessDifference = &UniquenessDifference{
					Table1Unique: col1.Unique,
					Table2Unique: col2.Unique,
				}
				columnIsDifferent = true
			}

			if columnIsDifferent {
				res.DifferentPropertiesCount++
				res.ColumnDifferences = append(res.ColumnDifferences, colDiff)
			} else {
				res.SamePropertiesCount++
			}
		} else {
			// Column from t1 does not exist in t2
			res.InTable1OnlyCount++
			res.MissingColumns = append(res.MissingColumns, MissingColumn{
				ColumnName:  col1.Name,
				Type:        col1.Type,
				Nullable:    col1.Nullable,
				PrimaryKey:  col1.PrimaryKey,
				Unique:      col1.Unique,
				TableName:   t1Name,
				MissingFrom: t2Name,
			})
		}
	}

	// Identify columns that are only in table 2 (extra in t2)
	for _, col2 := range t2.Columns {
		if _, foundAndProcessed := processedColsT2[col2.Name]; !foundAndProcessed {
			res.InTable2OnlyCount++
			res.MissingColumns = append(res.MissingColumns, MissingColumn{
				ColumnName:  col2.Name,
				Type:        col2.Type,
				Nullable:    col2.Nullable,
				PrimaryKey:  col2.PrimaryKey,
				Unique:      col2.Unique,
				TableName:   t2Name,
				MissingFrom: t1Name,
			})
		}
	}

	res.HasSchemaDifferences = res.DifferentPropertiesCount > 0 || res.InTable1OnlyCount > 0 || res.InTable2OnlyCount > 0
	return res
}
