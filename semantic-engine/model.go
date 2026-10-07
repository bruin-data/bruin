package semantic

// Model describes a single semantic model with dimensions, metrics, and segments.
type Model struct {
	Schema      string       `yaml:"schema,omitempty" json:"schema,omitempty"`
	Name        string       `yaml:"name" json:"name"`
	Label       string       `yaml:"label,omitempty" json:"label,omitempty"`
	Description string       `yaml:"description,omitempty" json:"description,omitempty"`
	Source      Source       `yaml:"source" json:"source"`
	PrimaryKey  string       `yaml:"primary_key,omitempty" json:"primary_key,omitempty"`
	Joins       []Join       `yaml:"joins,omitempty" json:"joins,omitempty"`
	Dimensions  []Dimension  `yaml:"dimensions,omitempty" json:"dimensions,omitempty"`
	Metrics     []Metric     `yaml:"metrics,omitempty" json:"metrics,omitempty"`
	Segments    []Segment    `yaml:"segments,omitempty" json:"segments,omitempty"`
	Notes       []Note       `yaml:"notes,omitempty" json:"notes,omitempty"`
	Checks      []ModelCheck `yaml:"checks,omitempty" json:"checks,omitempty"`
}

// Note is a reusable annotation matched to rows/points by its dimensions.
type Note struct {
	ID         string          `yaml:"id" json:"id"`
	Dimensions []NoteDimension `yaml:"dimensions" json:"dimensions"`
}

type NoteDimension struct {
	Name        string `yaml:"name" json:"name"`
	Type        string `yaml:"type,omitempty" json:"type,omitempty"`
	Required    bool   `yaml:"required,omitempty" json:"required,omitempty"`
	Multiselect bool   `yaml:"multiselect,omitempty" json:"multiselect,omitempty"`
}

type Source struct {
	Table string `yaml:"table" json:"table"`
	// Connection is the optional name of the Bruin connection the source table
	// lives in. Commands that need a warehouse (semantic validate, semantic
	// check, query --pipeline) use it when no --connection flag is given.
	Connection string `yaml:"connection,omitempty" json:"connection,omitempty"`
}

type Join struct {
	Name         string `yaml:"name" json:"name"`
	Model        string `yaml:"model,omitempty" json:"model,omitempty"`
	Relationship string `yaml:"relationship" json:"relationship"` // one_to_one, many_to_one, one_to_many, many_to_many
	ForeignKey   string `yaml:"foreign_key,omitempty" json:"foreign_key,omitempty"`
	TargetKey    string `yaml:"target_key,omitempty" json:"target_key,omitempty"`
	SQL          string `yaml:"sql,omitempty" json:"sql,omitempty"`
}

type Dimension struct {
	Name          string            `yaml:"name" json:"name"`
	Label         string            `yaml:"label,omitempty" json:"label,omitempty"`
	Description   string            `yaml:"description,omitempty" json:"description,omitempty"`
	Type          string            `yaml:"type,omitempty" json:"type,omitempty"` // string, number, boolean, time
	Expression    string            `yaml:"expression,omitempty" json:"expression,omitempty"`
	Granularities map[string]string `yaml:"granularities,omitempty" json:"granularities,omitempty"`
	Hidden        bool              `yaml:"hidden,omitempty" json:"hidden,omitempty"`
	Group         string            `yaml:"group,omitempty" json:"group,omitempty"`
	Checks        []Check           `yaml:"checks,omitempty" json:"checks,omitempty"`
}

type Metric struct {
	Name        string  `yaml:"name" json:"name"`
	Label       string  `yaml:"label,omitempty" json:"label,omitempty"`
	Description string  `yaml:"description,omitempty" json:"description,omitempty"`
	Expression  string  `yaml:"expression" json:"expression"`
	Filter      string  `yaml:"filter,omitempty" json:"filter,omitempty"`
	Hidden      bool    `yaml:"hidden,omitempty" json:"hidden,omitempty"`
	Group       string  `yaml:"group,omitempty" json:"group,omitempty"`
	Format      *Format `yaml:"format,omitempty" json:"format,omitempty"`
	Window      *Window `yaml:"window,omitempty" json:"window,omitempty"`
	Checks      []Check `yaml:"checks,omitempty" json:"checks,omitempty"`
}

type Format struct {
	Type     string `yaml:"type,omitempty" json:"type,omitempty"` // number, currency, percentage, decimal
	Currency string `yaml:"currency,omitempty" json:"currency,omitempty"`
	Decimals int    `yaml:"decimals,omitempty" json:"decimals,omitempty"`
}

type Window struct {
	Type        string   `yaml:"type,omitempty" json:"type,omitempty"` // running_total, lag, lead, rank, percent_of_total
	OrderBy     string   `yaml:"order_by,omitempty" json:"order_by,omitempty"`
	PartitionBy []string `yaml:"partition_by,omitempty" json:"partition_by,omitempty"`
	Offset      int      `yaml:"offset,omitempty" json:"offset,omitempty"`
}

type Segment struct {
	Name        string `yaml:"name" json:"name"`
	Label       string `yaml:"label,omitempty" json:"label,omitempty"`
	Description string `yaml:"description,omitempty" json:"description,omitempty"`
	Filter      string `yaml:"filter" json:"filter"`
}

// Query specifies what to retrieve from a model.
type Query struct {
	Dimensions []DimensionRef `yaml:"dimensions,omitempty" json:"dimensions,omitempty"`
	Metrics    []string       `yaml:"metrics,omitempty" json:"metrics,omitempty"`
	Filters    []Filter       `yaml:"filters,omitempty" json:"filters,omitempty"`
	Segments   []string       `yaml:"segments,omitempty" json:"segments,omitempty"`
	Sort       []SortSpec     `yaml:"sort,omitempty" json:"sort,omitempty"`
	Limit      int            `yaml:"limit,omitempty" json:"limit,omitempty"`
}

// DimensionRef selects a dimension, optionally at a time granularity. In YAML
// it accepts either a mapping or the "name:granularity" shorthand used by the
// CLI.
type DimensionRef struct {
	Name        string `yaml:"name" json:"name"`
	Granularity string `yaml:"granularity,omitempty" json:"granularity,omitempty"`
}

type Filter struct {
	Dimension  string      `yaml:"dimension,omitempty" json:"dimension,omitempty"`
	Operator   string      `yaml:"operator,omitempty" json:"operator,omitempty"` // equals, not_equals, gt, gte, lt, lte, in, not_in, between, is_null, is_not_null
	Value      interface{} `yaml:"value,omitempty" json:"value,omitempty"`
	Expression string      `yaml:"expression,omitempty" json:"expression,omitempty"`
}

// SortSpec orders the result by a dimension or metric. In YAML it accepts
// either a mapping or the "name:direction" shorthand used by the CLI.
type SortSpec struct {
	Name      string `yaml:"name" json:"name"`
	Direction string `yaml:"direction,omitempty" json:"direction,omitempty"` // asc, desc
}

// QueryColumn describes a column in a compiled query's result set.
//
// Name is the column name as it appears in the generated SQL (and therefore in
// the executed result). Qualified dimensions on joined models are sanitized
// (e.g. "customers.country" becomes "customers_country"), so Name and Field can
// differ. Field is the name the query referenced — a dimension ref name or a
// metric name — which callers typically use to map results back to a request.
type QueryColumn struct {
	Name  string `json:"name"`
	Field string `json:"field"`
}
