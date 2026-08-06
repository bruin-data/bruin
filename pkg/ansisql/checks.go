package ansisql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/bruin-data/bruin/pkg/jinja"
	"github.com/bruin-data/bruin/pkg/query"
	"github.com/bruin-data/bruin/pkg/scheduler"
	"github.com/pkg/errors"
)

type selector interface {
	Select(ctx context.Context, query *query.Query) ([][]interface{}, error)
}

// CheckError is a custom error type that carries query and result information
// for better error reporting when running single checks.
type CheckError struct {
	Query    string
	Result   int64
	Expected int64
	Message  string
}

func (e *CheckError) Error() string {
	return e.Message
}

type CountableQueryCheck struct {
	conn                config.ConnectionGetter
	expectedQueryResult int64
	queryInstance       *query.Query
	checkName           string
	customError         func(count int64) error
}

func NewCountableQueryCheck(conn config.ConnectionGetter, expectedQueryResult int64, queryInstance *query.Query, checkName string, customError func(count int64) error) *CountableQueryCheck {
	return &CountableQueryCheck{
		conn:                conn,
		expectedQueryResult: expectedQueryResult,
		queryInstance:       queryInstance,
		checkName:           checkName,
		customError:         customError,
	}
}

func (c *CountableQueryCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	conn, err := ti.Pipeline.GetConnectionNameForAsset(ti.GetAsset())
	if err != nil {
		return err
	}

	annotatedQuery, err := AddColumnCheckAnnotationComment(ctx, c.queryInstance, ti.GetAsset().Name, ti.Column.Name, c.checkName, ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}
	c.queryInstance = annotatedQuery
	ti.ExecutedQuery = c.queryInstance.Query

	return c.check(ctx, conn)
}

func (c *CountableQueryCheck) CustomCheck(ctx context.Context, ti *scheduler.CustomCheckInstance) error {
	conn, err := ti.Pipeline.GetConnectionNameForAsset(ti.GetAsset())
	if err != nil {
		return err
	}

	annotatedQuery, err := AddCustomCheckAnnotationComment(ctx, c.queryInstance, ti.GetAsset().Name, c.checkName, ti.Pipeline.Name)
	if err != nil {
		return errors.Wrap(err, "failed to add annotation comment")
	}
	c.queryInstance = annotatedQuery
	ti.ExecutedQuery = c.queryInstance.Query

	return c.check(ctx, conn)
}

func (c *CountableQueryCheck) check(ctx context.Context, connectionName string) error {
	q := c.conn.GetConnection(connectionName)
	if q == nil {
		return config.NewConnectionNotFoundError(ctx, "", connectionName)
	}

	s, ok := q.(selector)
	if !ok {
		return errors.Errorf("connection '%s' cannot be used for the check '%s'", connectionName, c.checkName)
	}

	res, err := s.Select(ctx, c.queryInstance)
	if err != nil {
		return errors.Wrapf(err, "failed '%s' check", c.checkName)
	}

	count, err := helpers.CastResultToInteger(res, false)
	if err != nil {
		return errors.Wrapf(err, "failed to parse '%s' check result", c.checkName)
	}

	if count != c.expectedQueryResult {
		return &CheckError{
			Query:    c.queryInstance.Query,
			Result:   count,
			Expected: c.expectedQueryResult,
			Message:  c.customError(count).Error(),
		}
	}

	return nil
}

type NotNullCheck struct {
	conn config.ConnectionGetter
}

func NewNotNullCheck(conn config.ConnectionGetter) *NotNullCheck {
	return &NotNullCheck{conn: conn}
}

func (c *NotNullCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s IS NULL", ti.GetAsset().Name, ti.Column.Name)

	return (&CountableQueryCheck{
		conn:                c.conn,
		expectedQueryResult: 0,
		queryInstance:       &query.Query{Query: qq},
		checkName:           "not_null",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d null values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type UniqueCheck struct {
	conn config.ConnectionGetter
}

func NewUniqueCheck(conn config.ConnectionGetter) *UniqueCheck {
	return &UniqueCheck{conn: conn}
}

func (c *UniqueCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT COUNT(%s) - COUNT(DISTINCT %s) FROM %s", ti.Column.Name, ti.Column.Name, ti.GetAsset().Name)

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "unique",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non-unique values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

// RelationshipsCheck verifies that every non-null value in a child column
// exists in the column referenced by its foreign_key metadata. A non-correlated
// NOT IN subquery works across all supported SQL dialects, including ClickHouse
// versions from before correlated subqueries were supported. Nulls are removed
// from both sides to make NOT IN null-safe. The query deliberately counts child
// rows, matching dbt's relationships test semantics.
type RelationshipsCheck struct {
	conn            config.ConnectionGetter
	quoteIdentifier func(string) string
}

func NewRelationshipsCheck(conn config.ConnectionGetter, quoteIdentifier func(string) string) *RelationshipsCheck {
	return &RelationshipsCheck{conn: conn, quoteIdentifier: quoteIdentifier}
}

func (c *RelationshipsCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	foreignKey := ti.Column.ForeignKey
	if foreignKey == nil || strings.TrimSpace(foreignKey.Table) == "" || strings.TrimSpace(foreignKey.Column) == "" {
		return errors.Errorf("relationships check on column '%s' requires foreign_key.table and foreign_key.column", ti.Column.Name)
	}

	qq := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s bruin_relationship_child WHERE bruin_relationship_child.%s IS NOT NULL AND bruin_relationship_child.%s NOT IN (SELECT bruin_relationship_parent.%s FROM %s bruin_relationship_parent WHERE bruin_relationship_parent.%s IS NOT NULL)",
		c.quoteIdentifier(ti.GetAsset().Name),
		c.quoteIdentifier(ti.Column.Name),
		c.quoteIdentifier(ti.Column.Name),
		c.quoteIdentifier(foreignKey.Column),
		c.quoteIdentifier(foreignKey.Table),
		c.quoteIdentifier(foreignKey.Column),
	)

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "relationships",
		customError: func(count int64) error {
			return errors.Errorf(
				"column '%s' has %d rows with values missing from '%s.%s'",
				ti.Column.Name,
				count,
				foreignKey.Table,
				foreignKey.Column,
			)
		},
	}).Check(ctx, ti)
}

var reservedSQLIdentifiers = map[string]bool{
	"ALL": true, "ALTER": true, "AND": true, "ANY": true, "AS": true, "ASC": true,
	"BETWEEN": true, "BY": true, "CASE": true, "CAST": true, "CHECK": true, "COLUMN": true,
	"CREATE": true, "CROSS": true, "CURRENT": true, "DATABASE": true, "DEFAULT": true,
	"DELETE": true, "DESC": true, "DISTINCT": true, "DROP": true, "ELSE": true, "END": true,
	"EXISTS": true, "FALSE": true, "FETCH": true, "FOR": true, "FOREIGN": true, "FROM": true,
	"FULL": true, "GROUP": true, "HAVING": true, "IN": true, "INNER": true, "INSERT": true,
	"INTERSECT": true, "INTO": true, "IS": true, "JOIN": true, "KEY": true, "LEFT": true,
	"LIKE": true, "LIMIT": true, "MERGE": true, "NATURAL": true, "NOT": true, "NULL": true,
	"OFFSET": true, "ON": true, "OR": true, "ORDER": true, "OUTER": true, "PRIMARY": true,
	"REFERENCES": true, "RIGHT": true, "ROW": true, "SELECT": true, "SET": true, "TABLE": true,
	"THEN": true, "TRUE": true, "UNION": true, "UNIQUE": true, "UPDATE": true, "USING": true,
	"VALUES": true, "VIEW": true, "WHEN": true, "WHERE": true, "WITH": true,
}

// QuoteIdentifierWithDoubleQuotes quotes every dotted identifier component using ANSI double quotes.
func QuoteIdentifierWithDoubleQuotes(identifier string) string {
	return quoteIdentifier(identifier, `"`, `"`, true)
}

// QuoteIdentifierWithDoubleQuotesWhenNeeded preserves ordinary identifiers and quotes reserved or non-standard components.
func QuoteIdentifierWithDoubleQuotesWhenNeeded(identifier string) string {
	return quoteIdentifier(identifier, `"`, `"`, false)
}

// QuoteIdentifierWithBackticks quotes every dotted identifier component using backticks.
func QuoteIdentifierWithBackticks(identifier string) string {
	return quoteIdentifier(identifier, "`", "`", true)
}

// QuoteIdentifierWithBrackets quotes every dotted identifier component using SQL Server brackets.
func QuoteIdentifierWithBrackets(identifier string) string {
	return quoteIdentifier(identifier, "[", "]", true)
}

func quoteIdentifier(identifier, openingQuote, closingQuote string, always bool) string {
	identifier = strings.TrimSpace(identifier)
	if isQuotedIdentifier(identifier) {
		return identifier
	}

	parts := strings.Split(identifier, ".")
	for index, part := range parts {
		part = strings.TrimSpace(part)
		if isQuotedIdentifier(part) || (!always && !identifierNeedsQuoting(part)) {
			parts[index] = part
			continue
		}
		parts[index] = openingQuote + strings.ReplaceAll(part, closingQuote, closingQuote+closingQuote) + closingQuote
	}
	return strings.Join(parts, ".")
}

func isQuotedIdentifier(identifier string) bool {
	if len(identifier) < 2 {
		return false
	}
	return (identifier[0] == '"' && identifier[len(identifier)-1] == '"') ||
		(identifier[0] == '`' && identifier[len(identifier)-1] == '`') ||
		(identifier[0] == '[' && identifier[len(identifier)-1] == ']')
}

func identifierNeedsQuoting(identifier string) bool {
	if identifier == "" || reservedSQLIdentifiers[strings.ToUpper(identifier)] {
		return true
	}
	for index := range len(identifier) {
		char := identifier[index]
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || char == '_' || (index > 0 && char >= '0' && char <= '9') {
			continue
		}
		return true
	}
	return false
}

type PositiveCheck struct {
	conn config.ConnectionGetter
}

func NewPositiveCheck(conn config.ConnectionGetter) *PositiveCheck {
	return &PositiveCheck{conn: conn}
}

func (c *PositiveCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s <= 0", ti.GetAsset().Name, ti.Column.Name)

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "positive",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non-positive values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type NonNegativeCheck struct {
	conn config.ConnectionGetter
}

func NewNonNegativeCheck(conn config.ConnectionGetter) *NonNegativeCheck {
	return &NonNegativeCheck{conn: conn}
}

func (c *NonNegativeCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s < 0", ti.GetAsset().Name, ti.Column.Name)

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "non_negative",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d negative values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

type NegativeCheck struct {
	conn config.ConnectionGetter
}

func NewNegativeCheck(conn config.ConnectionGetter) *NegativeCheck {
	return &NegativeCheck{conn: conn}
}

func (c *NegativeCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s >= 0", ti.GetAsset().Name, ti.Column.Name)

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "negative",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d non negative values", ti.Column.Name, count)
		},
	}).Check(ctx, ti)
}

func thresholdSQLValue(intPtr *int, floatPtr *float64, stringPtr *string, checkName string) (string, error) {
	switch {
	case intPtr != nil:
		return strconv.Itoa(*intPtr), nil
	case floatPtr != nil:
		return strconv.FormatFloat(*floatPtr, 'f', 6, 64), nil
	case stringPtr != nil:
		return fmt.Sprintf("'%s'", *stringPtr), nil
	default:
		return "", errors.Errorf("unexpected value for %s check, the value must be an int, float or string", checkName)
	}
}

type MinCheck struct {
	conn config.ConnectionGetter
}

func NewMinCheck(conn config.ConnectionGetter) *MinCheck { return &MinCheck{conn: conn} }

func (c *MinCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	threshold, err := thresholdSQLValue(ti.Check.Value.Int, ti.Check.Value.Float, ti.Check.Value.String, "min")
	if err != nil {
		return err
	}

	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s < %s", ti.GetAsset().Name, ti.Column.Name, threshold)

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "min",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d values below minimum %s", ti.Column.Name, count, ti.Check.Value.ToString())
		},
	}).Check(ctx, ti)
}

type MaxCheck struct {
	conn config.ConnectionGetter
}

func NewMaxCheck(conn config.ConnectionGetter) *MaxCheck { return &MaxCheck{conn: conn} }

func (c *MaxCheck) Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error {
	threshold, err := thresholdSQLValue(ti.Check.Value.Int, ti.Check.Value.Float, ti.Check.Value.String, "max")
	if err != nil {
		return err
	}

	qq := fmt.Sprintf("SELECT count(*) FROM %s WHERE %s > %s", ti.GetAsset().Name, ti.Column.Name, threshold)

	return (&CountableQueryCheck{
		conn:          c.conn,
		queryInstance: &query.Query{Query: qq},
		checkName:     "max",
		customError: func(count int64) error {
			return errors.Errorf("column '%s' has %d values above maximum %s", ti.Column.Name, count, ti.Check.Value.ToString())
		},
	}).Check(ctx, ti)
}

type CustomCheck struct {
	conn     config.ConnectionGetter
	renderer jinja.RendererInterface
}

func NewCustomCheck(conn config.ConnectionGetter, renderer jinja.RendererInterface) *CustomCheck {
	return &CustomCheck{conn: conn, renderer: renderer}
}

func (c *CustomCheck) Check(ctx context.Context, ti *scheduler.CustomCheckInstance) error {
	qq := ti.Check.Query
	if c.renderer != nil {
		r, err := c.renderer.CloneForAsset(ctx, ti.GetPipeline(), ti.GetAsset())
		if err != nil {
			return errors.Wrap(err, "failed to create renderer for asset")
		}
		qry, err := r.Render(qq)
		if err != nil {
			return errors.Wrap(err, "failed to render custom check query")
		}

		qq = qry
	}
	expected := ti.Check.Value
	if ti.Check.Count != nil {
		expected = *ti.Check.Count
		qq = fmt.Sprintf("SELECT count(*) FROM (%s) AS t", qq)
	}

	return NewCountableQueryCheck(c.conn, expected, &query.Query{Query: qq}, ti.Check.Name, func(count int64) error {
		return errors.Errorf("custom check '%s' has returned %d instead of the expected %d", ti.Check.Name, count, expected)
	}).CustomCheck(ctx, ti)
}

type CheckRunner interface {
	Check(ctx context.Context, ti *scheduler.ColumnCheckInstance) error
}

type ColumnCheckOperator struct {
	checkRunners map[string]CheckRunner
}

func NewColumnCheckOperator(checks map[string]CheckRunner) *ColumnCheckOperator {
	return &ColumnCheckOperator{
		checkRunners: checks,
	}
}

func (o ColumnCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	ctx = query.WithQueryType(ctx, query.QueryTypeColumn)
	test, ok := ti.(*scheduler.ColumnCheckInstance)
	if !ok {
		return errors.New("cannot run a non-column check instance")
	}

	executor, ok := o.checkRunners[test.Check.Name]
	if !ok {
		return errors.New("there is no executor configured for the check type, check cannot be run: " + test.Check.Name)
	}

	return executor.Check(ctx, test)
}

type CustomCheckRunner interface {
	Check(ctx context.Context, ti *scheduler.CustomCheckInstance) error
}

type CustomCheckOperator struct {
	checkRunner CustomCheckRunner
}

func NewCustomCheckOperator(manager config.ConnectionGetter, r jinja.RendererInterface) *CustomCheckOperator {
	return &CustomCheckOperator{
		checkRunner: &CustomCheck{conn: manager, renderer: r},
	}
}

func (o *CustomCheckOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	ctx = query.WithQueryType(ctx, query.QueryTypeCustom)
	instance, ok := ti.(*scheduler.CustomCheckInstance)
	if !ok {
		return errors.New("cannot run a non-custom check instance")
	}

	return o.checkRunner.Check(ctx, instance)
}
