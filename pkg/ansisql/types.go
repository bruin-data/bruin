package ansisql

type DBDatabase struct {
	Name string
	Schemas []*DBSchema
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
	Name string
	Tables []*DBTable
}
 
type DBTable struct {
	Name string
	Columns []*DBColumn
}

type DBColumn struct {
	Name string
	Type string
	Nullable bool
	PrimaryKey bool
	Unique bool	
}

type DBColumnType struct {
	Name string
	Size int
	Precision int
	Scale int
}