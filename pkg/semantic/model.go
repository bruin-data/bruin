package semantic

// Model describes a single semantic model with dimensions, metrics, and segments.
type Model struct {
	Schema      string      `yaml:"schema,omitempty" json:"schema,omitempty"`
	Name        string      `yaml:"name" json:"name"`
	Label       string      `yaml:"label,omitempty" json:"label,omitempty"`
	Description string      `yaml:"description,omitempty" json:"description,omitempty"`
	Source      Source      `yaml:"source" json:"source"`
	PrimaryKey  string      `yaml:"primary_key,omitempty" json:"primary_key,omitempty"`
	Joins       []Join      `yaml:"joins,omitempty" json:"joins,omitempty"`
	Dimensions  []Dimension `yaml:"dimensions,omitempty" json:"dimensions,omitempty"`
	Metrics     []Metric    `yaml:"metrics,omitempty" json:"metrics,omitempty"`
	Segments    []Segment   `yaml:"segments,omitempty" json:"segments,omitempty"`
}

type Source struct {
	Table string `yaml:"table" json:"table"`
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
	Dimensions []DimensionRef `json:"dimensions,omitempty"`
	Metrics    []string       `json:"metrics,omitempty"`
	Filters    []Filter       `json:"filters,omitempty"`
	Segments   []string       `json:"segments,omitempty"`
	Sort       []SortSpec     `json:"sort,omitempty"`
	Limit      int            `json:"limit,omitempty"`
}

type DimensionRef struct {
	Name        string `json:"name"`
	Granularity string `json:"granularity,omitempty"`
}

type Filter struct {
	Dimension  string      `json:"dimension,omitempty"`
	Operator   string      `json:"operator,omitempty"` // equals, not_equals, gt, gte, lt, lte, in, not_in, between, is_null, is_not_null
	Value      interface{} `json:"value,omitempty"`
	Expression string      `json:"expression,omitempty"`
}

type SortSpec struct {
	Name      string `json:"name"`
	Direction string `json:"direction,omitempty"` // asc, desc
}
