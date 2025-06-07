package diff

type Table struct {
	Name    string
	Columns []*Column
}

type Column struct {
	Name       string
	Type       string
	Nullable   bool
	PrimaryKey bool
	Unique     bool

	Stats ColumnStatistics
}

type Type struct {
	Name      string
	Size      int
	Precision int
	Scale     int
}

type TableSummaryResult struct {
	RowCount int64
	Table    *Table
}

// ColumnStatistics is an interface for different types of column statistics
type ColumnStatistics interface {
	Type() string
}

// NumericalStatistics holds statistics for numerical columns
type NumericalStatistics struct {
	Min       *float64 // pointer to handle NULL values
	Max       *float64
	Avg       *float64
	Sum       *float64
	Count     int64
	NullCount int64
	StdDev    *float64
}

func (ns *NumericalStatistics) Type() string {
	return "numerical"
}

// StringStatistics holds statistics for string/text columns
type StringStatistics struct {
	DistinctCount int64
	MaxLength     int
	MinLength     int
	AvgLength     float64
	Count         int64
	NullCount     int64
	EmptyCount    int64
	MostCommon    map[string]int64 // value -> frequency
	TopNDistinct  []string         // top N most common values
}

func (ss *StringStatistics) Type() string {
	return "string"
}

// BooleanStatistics holds statistics for boolean columns
type BooleanStatistics struct {
	TrueCount  int64
	FalseCount int64
	NullCount  int64
	Count      int64
}

func (bs *BooleanStatistics) Type() string {
	return "boolean"
}

// DateTimeStatistics holds statistics for date/time columns
type DateTimeStatistics struct {
	EarliestDate *string // ISO format or nil
	LatestDate   *string
	Count        int64
	NullCount    int64
	UniqueCount  int64
}

func (dts *DateTimeStatistics) Type() string {
	return "datetime"
}

type UnknownStatistics struct {
}

func (us *UnknownStatistics) Type() string {
	return "unknown"
}
