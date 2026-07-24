package jinja

import (
	"errors"
	"fmt"
	"maps"
	"math/bits"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/nikolalohinski/gonja/v2/exec"
)

// Platform represents a target database platform for SQL generation.
type Platform string

const (
	PlatformDefault    Platform = ""
	PlatformBigQuery   Platform = "bigquery"
	PlatformSnowflake  Platform = "snowflake"
	PlatformPostgres   Platform = "postgres"
	PlatformRedshift   Platform = "redshift"
	PlatformMySQL      Platform = "mysql"
	PlatformDoris      Platform = "doris"
	PlatformDuckDB     Platform = "duckdb"
	PlatformDatabricks Platform = "databricks"
	PlatformSpark      Platform = "spark"
	PlatformMSSQL      Platform = "mssql"
	PlatformClickhouse Platform = "clickhouse"
	PlatformAthena     Platform = "athena"
	PlatformTrino      Platform = "trino"
	PlatformSynapse    Platform = "synapse"
	PlatformOracle     Platform = "oracle"
	PlatformFabric     Platform = "fabric"
	PlatformVertica    Platform = "vertica"
	PlatformStarRocks  Platform = "starrocks"
)

// ---------------------------------------------------------------------------
// Platform override registry
// ---------------------------------------------------------------------------

var (
	platformOverridesMu sync.RWMutex
	platformOverrides   = map[Platform]map[string]any{}
)

// RegisterPlatformOverrides registers platform-specific function overrides.
// Platform packages call this in their init() to replace default built-in
// functions with platform-appropriate SQL generation. The platform package
// must be imported (directly or transitively) for its init() to execute.
func RegisterPlatformOverrides(platform Platform, overrides map[string]any) {
	platformOverridesMu.Lock()
	defer platformOverridesMu.Unlock()
	platformOverrides[platform] = overrides
}

// BuiltinFunctions returns a map of built-in SQL helper functions for use in Jinja templates.
// When a platform is specified, any registered overrides for that platform are merged on top of defaults.
func BuiltinFunctions(platform ...Platform) map[string]any {
	funcs := defaultBuiltinFunctions()
	if len(platform) > 0 && platform[0] != PlatformDefault {
		platformOverridesMu.RLock()
		overrides := platformOverrides[platform[0]]
		platformOverridesMu.RUnlock()
		maps.Copy(funcs, overrides)
	}
	return funcs
}

// MergeBuiltinOverrides combines override maps left-to-right.
func MergeBuiltinOverrides(overrides ...map[string]any) map[string]any {
	result := map[string]any{}
	for _, override := range overrides {
		maps.Copy(result, override)
	}
	return result
}

func defaultBuiltinFunctions() map[string]any {
	return map[string]any{
		"group_by":               bruinGroupBy,
		"safe_divide":            bruinSafeDivide,
		"safe_add":               bruinSafeAdd,
		"safe_subtract":          bruinSafeSubtract,
		"generate_surrogate_key": defaultSurrogateKey,
		"pivot":                  bruinPivot,
		"haversine_distance":     defaultHaversineDistance,
		"degrees_to_radians":     bruinDegreesToRadians,
		"width_bucket":           bruinWidthBucket,
		"deduplicate":            bruinDeduplicate,
		"generate_series":        bruinGenerateSeries,
		"date_spine":             defaultDateSpine,
		"slugify":                bruinSlugify,
		"get_url_host":           bruinGetURLHost,
		"get_url_parameter":      bruinGetURLParameter,
		"get_url_path":           bruinGetURLPath,
	}
}

// ---------------------------------------------------------------------------
// Builder functions for platform packages
//
// These let platform packages construct override functions without importing
// gonja/exec directly. The caller provides only the SQL-level differences
// (cast type, hash wrapper, radians expression, dateadd syntax) and the
// builder handles VarArgs/Value plumbing.
// ---------------------------------------------------------------------------

// SurrogateKeyWith builds a generate_surrogate_key function using the given cast type and hash wrapper.
func SurrogateKeyWith(castType string, hashFn func(concatExpr string) string) func(*exec.VarArgs) *exec.Value {
	return SurrogateKeyWithConcat(castType, concatFunction, hashFn)
}

// SurrogateKeyWithConcat builds a generate_surrogate_key function using platform-specific concat and hash syntax.
func SurrogateKeyWithConcat(castType string, concatFn func(parts []string) string, hashFn func(concatExpr string) string) func(*exec.VarArgs) *exec.Value {
	return SurrogateKeyWithCoalesceExpr(func(field string) string {
		return fmt.Sprintf("coalesce(cast(%s as %s), '%s')", field, castType, surrogateKeyNullValue)
	}, concatFn, hashFn)
}

const surrogateKeyNullValue = "_bruin_surrogate_key_null_"

// SurrogateKeyNullValue returns the sentinel used to preserve null positions while hashing.
func SurrogateKeyNullValue() string {
	return surrogateKeyNullValue
}

// SurrogateKeyWithCoalesceExpr builds a generate_surrogate_key function using a caller-provided per-field expression.
func SurrogateKeyWithCoalesceExpr(valueFn func(field string) string, concatFn func(parts []string) string, hashFn func(concatExpr string) string) func(*exec.VarArgs) *exec.Value {
	return func(va *exec.VarArgs) *exec.Value {
		fields := extractStringListFromVarArgs(va)
		if len(fields) == 0 {
			return exec.AsValue("")
		}

		concatParts := make([]string, 0, len(fields)*2-1)
		for i, f := range fields {
			concatParts = append(concatParts, valueFn(f))
			if i < len(fields)-1 {
				concatParts = append(concatParts, "'-'")
			}
		}
		concatExpr := concatFn(concatParts)
		return exec.AsValue(hashFn(concatExpr))
	}
}

func concatFunction(parts []string) string {
	if len(parts) == 1 {
		return parts[0]
	}
	return fmt.Sprintf("concat(%s)", strings.Join(parts, ", "))
}

// ConcatFunction joins expressions with a variadic concat function, returning the expression unchanged for one part.
func ConcatFunction(parts []string) string {
	return concatFunction(parts)
}

// ConcatOperator joins expressions with the ANSI string concatenation operator.
func ConcatOperator(parts []string) string {
	return strings.Join(parts, " || ")
}

// PivotWithIdentifierQuote builds a pivot function using platform-specific quoted identifier syntax.
func PivotWithIdentifierQuote(quoteIdentifier func(string) string) func(*exec.VarArgs) *exec.Value {
	return func(va *exec.VarArgs) *exec.Value {
		return bruinPivotWithIdentifierQuote(va, quoteIdentifier)
	}
}

// DoubleQuoteIdentifier quotes an identifier with ANSI double quotes.
func DoubleQuoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// BacktickQuoteIdentifier quotes an identifier with backticks.
func BacktickQuoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "``") + "`"
}

// BigQueryQuoteIdentifier quotes a GoogleSQL identifier with backticks and backslash-escaped backticks.
func BigQueryQuoteIdentifier(identifier string) string {
	return "`" + strings.ReplaceAll(identifier, "`", "\\`") + "`"
}

// BracketQuoteIdentifier quotes an identifier with SQL Server square brackets.
func BracketQuoteIdentifier(identifier string) string {
	return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
}

// HaversineDistanceWithRadians builds a haversine_distance function using the given radians conversion.
func HaversineDistanceWithRadians(radiansFn func(expr string) string) func(*exec.VarArgs) (*exec.Value, error) {
	return func(va *exec.VarArgs) (*exec.Value, error) {
		if len(va.Args) < 4 {
			return nil, errors.New("haversine_distance requires 4 arguments: lat1, lon1, lat2, lon2")
		}

		lat1 := va.Args[0].String()
		lon1 := va.Args[1].String()
		lat2 := va.Args[2].String()
		lon2 := va.Args[3].String()

		unit := "mi"
		if v, ok := va.KwArgs["unit"]; ok {
			unit = v.String()
		} else if len(va.Args) > 4 {
			unit = va.Args[4].String()
		}

		conversionRate := "1"
		switch unit {
		case "mi":
		case "km":
			conversionRate = "1.60934"
		default:
			return nil, fmt.Errorf("haversine_distance unit must be 'mi' or 'km', got %q", unit)
		}

		return exec.AsValue(fmt.Sprintf(
			"2 * 3961 * asin(sqrt(power((sin(%s)), 2) +\n"+
				"    cos(%s) * cos(%s) *\n"+
				"    power((sin(%s)), 2))) * %s",
			radiansFn(fmt.Sprintf("(%s - %s) / 2", lat2, lat1)),
			radiansFn(lat1),
			radiansFn(lat2),
			radiansFn(fmt.Sprintf("(%s - %s) / 2", lon2, lon1)),
			conversionRate,
		)), nil
	}
}

// DateSpineWithDateAdd builds a date_spine function using the given date-addition expression builder.
func DateSpineWithDateAdd(dateAddFn func(datepart, n, start string) string) func(string, string, string) string {
	return DateSpineWithRecursiveDateAdd(true, "", dateAddFn)
}

// DateSpineWithDateAddAndDateDiff builds a recursive date_spine function using an interval count.
func DateSpineWithDateAddAndDateDiff(withRecursive bool, suffix string, dateAddFn, dateDiffFn func(datepart, start, end string) string) func(string, string, string) string {
	return func(datepart, startDate, endDate string) string {
		columnName := "date_" + datepart
		diffExpr := dateDiffFn(datepart, startDate, endDate)
		nextExpr := dateAddFn(datepart, "(n + 1)", startDate)
		withKeyword := "with"
		if withRecursive {
			withKeyword = "with recursive"
		}

		return fmt.Sprintf(`%s date_spine(n, %s) as (

    select 0 as n, %s as %s
    where %s > 0

    union all

    select n + 1, %s
    from date_spine
    where n + 1 < %s

)

select %s
from date_spine%s`, withKeyword, columnName, startDate, columnName, diffExpr, nextExpr, diffExpr, columnName, suffix)
	}
}

// DateSpineWithRecursiveDateAdd builds a recursive CTE date_spine function.
func DateSpineWithRecursiveDateAdd(withRecursive bool, suffix string, dateAddFn func(datepart, n, start string) string) func(string, string, string) string {
	return func(datepart, startDate, endDate string) string {
		columnName := "date_" + datepart
		nextExpr := dateAddFn(datepart, "(n + 1)", startDate)
		withKeyword := "with"
		if withRecursive {
			withKeyword = "with recursive"
		}

		return fmt.Sprintf(`%s date_spine(n, %s) as (

    select 0 as n, %s as %s
    where %s < %s

    union all

    select n + 1, %s
    from date_spine
    where %s < %s

)

select %s
from date_spine%s`, withKeyword, columnName, startDate, columnName, startDate, endDate, nextExpr, nextExpr, endDate, columnName, suffix)
	}
}

// BigQueryDateSpine builds date_spine using BigQuery's array generators.
func BigQueryDateSpine(datepart, startDate, endDate string) string {
	columnName := "date_" + datepart
	part := strings.ToUpper(datepart)
	if IsTimestampDatepart(datepart) {
		spineExpr := fmt.Sprintf("timestamp_add(timestamp(%s), interval n %s)", startDate, part)
		return fmt.Sprintf(`select %s as %s
from unnest(generate_array(0, greatest(timestamp_diff(timestamp(%s), timestamp(%s), %s), 0))) as n
where %s < timestamp(%s)`, spineExpr, columnName, endDate, startDate, part, spineExpr, endDate)
	}
	spineExpr := fmt.Sprintf("date_add(date(%s), interval n %s)", startDate, part)
	return fmt.Sprintf(`select %s as %s
from unnest(generate_array(0, greatest(date_diff(date(%s), date(%s), %s), 0))) as n
where %s < date(%s)`, spineExpr, columnName, endDate, startDate, part, spineExpr, endDate)
}

// DuckDBDateSpine builds date_spine using DuckDB's generate_series table function.
func DuckDBDateSpine(datepart, startDate, endDate string) string {
	columnName := "date_" + datepart
	castType := "date"
	selectExpr := columnName
	if IsTimestampDatepart(datepart) {
		castType = "timestamp"
	} else {
		selectExpr = fmt.Sprintf("cast(%s as date)", columnName)
	}
	step := IntervalStepLiteral(datepart)
	return fmt.Sprintf(`select %s as %s
from generate_series(cast(%s as %s), cast(%s as %s), interval '%s') as t(%s)
where %s < cast(%s as %s)`, selectExpr, columnName, startDate, castType, endDate, castType, step, columnName, columnName, endDate, castType)
}

// PostgresDateSpine builds date_spine using Postgres generate_series.
func PostgresDateSpine(datepart, startDate, endDate string) string {
	columnName := "date_" + datepart
	selectExpr := columnName
	if !IsTimestampDatepart(datepart) {
		selectExpr = fmt.Sprintf("cast(%s as date)", columnName)
	}
	step := IntervalStepLiteral(datepart)
	return fmt.Sprintf(`select %s as %s
from generate_series(cast(%s as timestamp), cast(%s as timestamp), interval '%s') as t(%s)
where %s < cast(%s as timestamp)`, selectExpr, columnName, startDate, endDate, step, columnName, columnName, endDate)
}

// SparkDateSpine builds date_spine using Spark/Databricks sequence + explode.
func SparkDateSpine(datepart, startDate, endDate string) string {
	columnName := "date_" + datepart
	castFn := "to_date"
	arrayType := "array<date>"
	if IsTimestampDatepart(datepart) {
		castFn = "to_timestamp"
		arrayType = "array<timestamp>"
	}
	step := IntervalStepLiteral(datepart)
	return fmt.Sprintf(`select explode(
    case
        when %s(%s) + interval %s <= %s(%s)
            then filter(sequence(%s(%s), %s(%s), interval %s), x -> x < %s(%s))
        else cast(array() as %s)
    end
) as %s`, castFn, startDate, step, castFn, endDate, castFn, startDate, castFn, endDate, step, castFn, endDate, arrayType, columnName)
}

// PrestoDateSpine builds date_spine for Trino/Athena using sequence + unnest.
func PrestoDateSpine(datepart, startDate, endDate string) string {
	columnName := "date_" + datepart
	castType := "date"
	if IsTimestampDatepart(datepart) {
		castType = "timestamp"
	}
	diffExpr := fmt.Sprintf("date_diff('%s', cast(%s as %s), cast(%s as %s))", datepart, startDate, castType, endDate, castType)
	spineExpr := fmt.Sprintf("date_add('%s', n, cast(%s as %s))", datepart, startDate, castType)
	return fmt.Sprintf(`select date_add('%s', n, cast(%s as %s)) as %s
from unnest(sequence(cast(0 as bigint), greatest(%s, cast(0 as bigint)))) as t(n)
where %s < cast(%s as %s)`, datepart, startDate, castType, columnName, diffExpr, spineExpr, endDate, castType)
}

// ClickHouseDateSpine builds date_spine using numbers and dateDiff/dateAdd.
func ClickHouseDateSpine(datepart, startDate, endDate string) string {
	columnName := "date_" + datepart
	castFn := "toDate"
	switch datepart {
	case "millisecond":
		castFn = "toDateTime64"
		startDate += ", 3"
		endDate += ", 3"
	case "microsecond":
		castFn = "toDateTime64"
		startDate += ", 6"
		endDate += ", 6"
	case "hour", "minute", "second":
		castFn = "toDateTime"
	}
	return fmt.Sprintf(`select date_add(%s, number, %s(%s)) as %s
from numbers(greatest(dateDiff('%s', %s(%s), %s(%s)), 0) + 1)
where date_add(%s, number, %s(%s)) < %s(%s)`, datepart, castFn, startDate, columnName, datepart, castFn, startDate, castFn, endDate, datepart, castFn, startDate, castFn, endDate)
}

// OracleDateSpine builds date_spine using CONNECT BY LEVEL.
func OracleDateSpine(datepart, startDate, endDate string) string {
	columnName := "date_" + datepart
	startExpr := fmt.Sprintf("cast(%s as date)", startDate)
	endExpr := fmt.Sprintf("cast(%s as date)", endDate)
	switch datepart {
	case "quarter":
		return fmt.Sprintf(`select %s
from (
    select ADD_MONTHS(base_date, (level - 1) * 3) as %s, end_date
    from (select %s as base_date, %s as end_date from dual where %s < %s)
    connect by level <= (ceil(months_between(end_date, base_date) / 3) + 1)
)
where %s < end_date`, columnName, columnName, startExpr, endExpr, startExpr, endExpr, columnName)
	case "month":
		return fmt.Sprintf(`select %s
from (
    select ADD_MONTHS(base_date, level - 1) as %s, end_date
    from (select %s as base_date, %s as end_date from dual where %s < %s)
    connect by level <= (ceil(months_between(end_date, base_date)) + 1)
)
where %s < end_date`, columnName, columnName, startExpr, endExpr, startExpr, endExpr, columnName)
	case "year":
		return fmt.Sprintf(`select %s
from (
    select ADD_MONTHS(base_date, (level - 1) * 12) as %s, end_date
    from (select %s as base_date, %s as end_date from dual where %s < %s)
    connect by level <= (ceil(months_between(end_date, base_date) / 12) + 1)
)
where %s < end_date`, columnName, columnName, startExpr, endExpr, startExpr, endExpr, columnName)
	case "hour", "minute", "second":
		multiplier := map[string]string{"hour": "24", "minute": "1440", "second": "86400"}[datepart]
		return fmt.Sprintf(`select %s
from (
    select (base_date + NUMTODSINTERVAL(level - 1, '%s')) as %s, end_date
    from (select %s as base_date, %s as end_date from dual where %s < %s)
    connect by level <= (ceil((end_date - base_date) * %s) + 1)
)
where %s < end_date`, columnName, datepart, columnName, startExpr, endExpr, startExpr, endExpr, multiplier, columnName)
	case "week":
		return fmt.Sprintf(`select %s
from (
    select (base_date + ((level - 1) * 7)) as %s, end_date
    from (select %s as base_date, %s as end_date from dual where %s < %s)
    connect by level <= (ceil((end_date - base_date) / 7) + 1)
)
where %s < end_date`, columnName, columnName, startExpr, endExpr, startExpr, endExpr, columnName)
	default:
		return fmt.Sprintf(`select %s
from (
    select (base_date + level - 1) as %s, end_date
    from (select %s as base_date, %s as end_date from dual where %s < %s)
    connect by level <= (ceil(end_date - base_date) + 1)
)
where %s < end_date`, columnName, columnName, startExpr, endExpr, startExpr, endExpr, columnName)
	}
}

// MySQLDateSpine builds a recursive date_spine with MySQL casts and a per-query recursion-depth hint.
func MySQLDateSpine(datepart, startDate, endDate string) string {
	columnName := "date_" + datepart
	castType := dateSpineCastType(datepart, "date", "datetime")
	startExpr := fmt.Sprintf("cast(%s as %s)", startDate, castType)
	endExpr := fmt.Sprintf("cast(%s as %s)", endDate, castType)
	nextExpr := fmt.Sprintf("DATE_ADD(%s, INTERVAL (n + 1) %s)", startExpr, datepart)

	return fmt.Sprintf(`with recursive date_spine(n, %s) as (

    select 0 as n, %s as %s
    where %s < %s

    union all

    select n + 1, %s
    from date_spine
    where %s < %s

)

select /*+ SET_VAR(cte_max_recursion_depth=1000000) */ %s
from date_spine`, columnName, startExpr, columnName, startExpr, endExpr, nextExpr, nextExpr, endExpr, columnName)
}

// TSQLDateSpine builds date_spine for SQL Server-family dialects.
func TSQLDateSpine(datepart, startDate, endDate string) string {
	castType := dateSpineCastType(datepart, "date", "datetime2")
	startExpr := fmt.Sprintf("cast(%s as %s)", startDate, castType)
	endExpr := fmt.Sprintf("cast(%s as %s)", endDate, castType)
	return DateSpineWithRecursiveDateAdd(false, "\noption (maxrecursion 0)", func(datepart, n, start string) string {
		return fmt.Sprintf("dateadd(%s, %s, %s)", datepart, n, start)
	})(datepart, startExpr, endExpr)
}

// TSQLDateSpineWithTally builds date_spine without recursive CTEs for Synapse/Fabric.
func TSQLDateSpineWithTally(datepart, startDate, endDate string) string {
	columnName := "date_" + datepart
	castType := dateSpineCastType(datepart, "date", "datetime2")
	startExpr := fmt.Sprintf("cast(%s as %s)", startDate, castType)
	endExpr := fmt.Sprintf("cast(%s as %s)", endDate, castType)
	diffExpr := fmt.Sprintf("datediff(%s, %s, %s)", datepart, startExpr, endExpr)
	topExpr := fmt.Sprintf("case when %s < 0 then 0 else %s + 2 end", diffExpr, diffExpr)
	nextExpr := fmt.Sprintf("dateadd(%s, cast(n as int), %s)", datepart, startExpr)

	return fmt.Sprintf(`with digits(n) as (
    select 0 union all select 1 union all select 2 union all select 3 union all select 4
    union all select 5 union all select 6 union all select 7 union all select 8 union all select 9
), numbers(n) as (
    select top (%s)
        row_number() over (order by d0.n, d1.n, d2.n, d3.n, d4.n, d5.n, d6.n) - 1 as n
    from digits d0
    cross join digits d1
    cross join digits d2
    cross join digits d3
    cross join digits d4
    cross join digits d5
    cross join digits d6
)
select %s as %s
from numbers
where %s < %s`, topExpr, nextExpr, columnName, nextExpr, endExpr)
}

func dateSpineCastType(datepart, dateType, timestampType string) string {
	if IsTimestampDatepart(datepart) {
		return timestampType
	}
	return dateType
}

// IntervalStepLiteral returns a SQL interval literal body for dateparts that are not universally accepted as interval units.
func IntervalStepLiteral(datepart string) string {
	switch datepart {
	case "quarter":
		return "3 month"
	case "week":
		return "7 day"
	default:
		return "1 " + datepart
	}
}

// IsTimestampDatepart reports whether a date_spine datepart needs timestamp rather than date values.
func IsTimestampDatepart(datepart string) bool {
	switch strings.ToLower(datepart) {
	case "hour", "minute", "second", "millisecond", "microsecond":
		return true
	default:
		return false
	}
}

// ---------------------------------------------------------------------------
// Default implementations (used when no platform override is registered)
// ---------------------------------------------------------------------------

var defaultSurrogateKey = SurrogateKeyWith("varchar", func(concatExpr string) string {
	return fmt.Sprintf("md5(%s)", concatExpr)
})

var defaultHaversineDistance = HaversineDistanceWithRadians(func(expr string) string {
	return fmt.Sprintf("radians(%s)", expr)
})

var defaultDateSpine = DateSpineWithDateAdd(func(datepart, n, start string) string {
	return fmt.Sprintf("dateadd(%s, %s, %s)", datepart, n, start)
})

// ---------------------------------------------------------------------------
// Platform-independent functions
// ---------------------------------------------------------------------------

func bruinGroupBy(n int) string {
	parts := make([]string, n)
	for i := range n {
		parts[i] = strconv.Itoa(i + 1)
	}
	return "group by " + strings.Join(parts, ", ")
}

func bruinSafeDivide(numerator, denominator string) string {
	return fmt.Sprintf("(%s) / nullif((%s), 0)", numerator, denominator)
}

func bruinSafeAdd(va *exec.VarArgs) *exec.Value {
	return safeArithmetic(va, " +\n    ")
}

func bruinSafeSubtract(va *exec.VarArgs) *exec.Value {
	return safeArithmetic(va, " -\n    ")
}

func safeArithmetic(va *exec.VarArgs, operator string) *exec.Value {
	fields := extractStringListFromVarArgs(va)
	if len(fields) == 0 {
		return exec.AsValue("")
	}
	parts := make([]string, len(fields))
	for i, f := range fields {
		parts[i] = fmt.Sprintf("coalesce(%s, 0)", f)
	}
	return exec.AsValue(strings.Join(parts, operator))
}

func bruinPivot(va *exec.VarArgs) *exec.Value {
	return bruinPivotWithIdentifierQuote(va, DoubleQuoteIdentifier)
}

func bruinPivotWithIdentifierQuote(va *exec.VarArgs, quoteIdentifier func(string) string) *exec.Value {
	if len(va.Args) < 2 {
		return exec.AsValue("/* pivot requires at least 2 arguments: column, values */")
	}

	column := va.Args[0].String()
	values := extractStringListFromValue(va.Args[1])

	alias := getKwArgBool(va, "alias", true)
	agg := getKwArgString(va, "agg", "sum")
	cmp := getKwArgString(va, "cmp", "=")
	prefix := getKwArgString(va, "prefix", "")
	suffix := getKwArgString(va, "suffix", "")
	thenValue := getKwArgString(va, "then_value", "1")
	elseValue := getKwArgString(va, "else_value", "0")
	quoteIdentifiers := getKwArgBool(va, "quote_identifiers", true)
	distinct := getKwArgBool(va, "distinct", false)

	parts := make([]string, 0, len(values))
	for _, value := range values {
		escapedValue := strings.ReplaceAll(value, "'", "''")

		distinctStr := ""
		if distinct {
			distinctStr = "distinct "
		}

		expr := fmt.Sprintf("%s(\n        %scase\n        when %s %s '%s'\n            then %s\n        else %s\n        end\n    )",
			agg, distinctStr, column, cmp, escapedValue, thenValue, elseValue)

		if alias {
			aliasName := prefix + value + suffix
			if quoteIdentifiers {
				expr += "\n        as " + quoteIdentifier(aliasName)
			} else {
				expr += "\n        as " + bruinSlugify(aliasName)
			}
		}

		parts = append(parts, expr)
	}

	return exec.AsValue(strings.Join(parts, ",\n    "))
}

func bruinDegreesToRadians(degrees string) string {
	return fmt.Sprintf("acos(-1) * %s / 180", degrees)
}

func bruinWidthBucket(expr, minValue, maxValue, numBuckets string) string {
	binSize := fmt.Sprintf("((cast(%s as numeric) - cast(%s as numeric)) / cast(%s as numeric))", maxValue, minValue, numBuckets)

	return fmt.Sprintf(`case
        when cast(%s as numeric) < cast(%s as numeric) then 0
        when cast(%s as numeric) >= cast(%s as numeric) then cast(%s as numeric) + 1
        when mod(cast(%s as numeric) - cast(%s as numeric), %s) = 0
            then ceil((cast(%s as numeric) - cast(%s as numeric)) / %s) + 1
        else ceil((cast(%s as numeric) - cast(%s as numeric)) / %s)
    end`, expr, minValue, expr, maxValue, numBuckets, expr, minValue, binSize, expr, minValue, binSize, expr, minValue, binSize)
}

// NativeWidthBucket returns a platform-native width_bucket implementation.
func NativeWidthBucket(expr, minValue, maxValue, numBuckets string) string {
	return fmt.Sprintf("width_bucket(%s, %s, %s, %s)", expr, minValue, maxValue, numBuckets)
}

// BigQueryWidthBucket builds standard width_bucket behavior for BigQuery, which has no native function.
func BigQueryWidthBucket(expr, minValue, maxValue, numBuckets string) string {
	return fmt.Sprintf(`case
        when cast(%s as numeric) < cast(%s as numeric) then 0
        when cast(%s as numeric) >= cast(%s as numeric) then cast(%s as int64) + 1
        else cast(floor(
            (cast(%s as numeric) - cast(%s as numeric))
            / ((cast(%s as numeric) - cast(%s as numeric)) / cast(%s as numeric))
        ) as int64) + 1
    end`, expr, minValue, expr, maxValue, numBuckets, expr, minValue, maxValue, minValue, numBuckets)
}

// ClickHouseWidthBucket builds standard width_bucket behavior for ClickHouse.
func ClickHouseWidthBucket(expr, minValue, maxValue, numBuckets string) string {
	return fmt.Sprintf(`multiIf(
        %s < %s, 0,
        %s >= %s, %s + 1,
        toInt64(floor((%s - %s) / ((%s - %s) / %s))) + 1
    )`, expr, minValue, expr, maxValue, numBuckets, expr, minValue, maxValue, minValue, numBuckets)
}

// TSQLWidthBucket builds standard width_bucket behavior using T-SQL math functions.
func TSQLWidthBucket(expr, minValue, maxValue, numBuckets string) string {
	return fmt.Sprintf(`case
        when cast(%s as float) < cast(%s as float) then 0
        when cast(%s as float) >= cast(%s as float) then cast(%s as int) + 1
        else cast(floor(
            (cast(%s as float) - cast(%s as float))
            / ((cast(%s as float) - cast(%s as float)) / cast(%s as float))
        ) as int) + 1
    end`, expr, minValue, expr, maxValue, numBuckets, expr, minValue, maxValue, minValue, numBuckets)
}

func bruinDeduplicate(relation, partitionBy, orderBy string) string {
	return fmt.Sprintf(`with row_numbered as (
        select
            _inner.*,
            row_number() over (
                partition by %s
                order by %s
            ) as __bruin_row_number
        from %s as _inner
    )

    select
        distinct data.*
    from %s as data
    natural join row_numbered
    where row_numbered.__bruin_row_number = 1`, partitionBy, orderBy, relation, relation)
}

// generateSeriesCTEs returns only the CTE definitions (WITH p AS ..., unioned AS ...)
// without a terminal SELECT, for use in composite queries like date_spine.
func generateSeriesCTEs(upperBound int) string {
	n := getPowersOfTwo(upperBound)

	var sb strings.Builder
	sb.WriteString("with p as (\n        select 0 as generated_number union all select 1\n    ), unioned as (\n\n        select\n\n        ")

	for i := range n {
		sb.WriteString("p")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(".generated_number * power(2, ")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString(")")
		if i < n-1 {
			sb.WriteString("\n        + ")
		}
	}

	sb.WriteString("\n        + 1\n        as generated_number\n\n        from\n\n        ")

	for i := range n {
		sb.WriteString("p as p")
		sb.WriteString(strconv.Itoa(i))
		if i < n-1 {
			sb.WriteString("\n        cross join ")
		}
	}

	sb.WriteString("\n\n    )")

	return sb.String()
}

// bruinGenerateSeries produces a cross-join CTE generating numbers 1..upperBound.
// Works on all SQL databases without platform-specific syntax.
func bruinGenerateSeries(upperBound int) string {
	return fmt.Sprintf("%s\n\n    select *\n    from unioned\n    where generated_number <= %d\n    order by generated_number", generateSeriesCTEs(upperBound), upperBound)
}

// DuckDBGenerateSeries uses DuckDB's native integer series generator.
func DuckDBGenerateSeries(upperBound int) string {
	return fmt.Sprintf(`select generated_number
from generate_series(1, %d) as t(generated_number)
order by generated_number`, upperBound)
}

// ---------------------------------------------------------------------------
// Jinja helpers
// ---------------------------------------------------------------------------

var (
	slugifySpaceDash = regexp.MustCompile(`[ -]+`)
	slugifyNonAlnum  = regexp.MustCompile(`[^a-z0-9_]+`)
	slugifyLeadDigit = regexp.MustCompile(`^([0-9])`)
)

func bruinSlugify(s string) string {
	if s == "" {
		return ""
	}
	s = strings.ToLower(s)
	s = slugifySpaceDash.ReplaceAllString(s, "_")
	s = slugifyNonAlnum.ReplaceAllString(s, "")
	if slugifyLeadDigit.MatchString(s) {
		s = "_" + s
	}
	return s
}

// ---------------------------------------------------------------------------
// Web helpers
// ---------------------------------------------------------------------------

func bruinGetURLHost(field string) string {
	return fmt.Sprintf(
		"split_part(split_part(replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', ''), '/', 1), '?', 1)",
		field,
	)
}

func bruinGetURLParameter(field, urlParameter string) string {
	queryPart := fmt.Sprintf("case when position('?' in %s) > 0 then split_part(%s, '?', 2) else %s end", field, field, field)
	return fmt.Sprintf(
		"nullif(split_part(split_part(concat('&', %s), %s, 2), '&', 1), '')",
		queryPart,
		sqlStringLiteral("&"+urlParameter+"="),
	)
}

func bruinGetURLPath(field string) string {
	strippedURL := fmt.Sprintf("replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', '')", field)
	parsedPath := fmt.Sprintf(
		"case when position('/' in %s) > 0 then split_part(right(%s, length(%s) - position('/' in %s)), '?', 1) else '' end",
		strippedURL, strippedURL, strippedURL, strippedURL,
	)
	return fmt.Sprintf("cast(%s as varchar)", parsedPath)
}

// SplitPartURLHelpers returns URL helpers for dialects that support split_part, position, right, and SQL-standard casts.
func SplitPartURLHelpers(castType string) map[string]any {
	return map[string]any{
		"get_url_host": func(field string) string {
			parsed := fmt.Sprintf(
				"split_part(split_part(replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', ''), '/', 1), '?', 1)",
				field,
			)
			return fmt.Sprintf("cast(%s as %s)", parsed, castType)
		},
		"get_url_parameter": func(field, urlParameter string) string {
			wrappedField := fmt.Sprintf("case when position('?' in %s) > 0 then split_part(%s, '?', 2) else %s end", field, field, field)
			return fmt.Sprintf(
				"nullif(split_part(split_part(concat('&', %s), %s, 2), '&', 1), '')",
				wrappedField,
				sqlStringLiteral("&"+urlParameter+"="),
			)
		},
		"get_url_path": func(field string) string {
			strippedURL := fmt.Sprintf("replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', '')", field)
			parsedPath := fmt.Sprintf(
				"case when position('/' in %s) > 0 then split_part(right(%s, length(%s) - position('/' in %s)), '?', 1) else '' end",
				strippedURL, strippedURL, strippedURL, strippedURL,
			)
			return fmt.Sprintf("cast(%s as %s)", parsedPath, castType)
		},
	}
}

// BigQueryURLHelpers returns URL helpers using BigQuery regex functions.
func BigQueryURLHelpers() map[string]any {
	return regexURLHelpers("string", "regexp_extract", "regexp_replace", sqlRawStringLiteral)
}

// SparkURLHelpers returns URL helpers using Spark/Databricks regex functions.
func SparkURLHelpers() map[string]any {
	return map[string]any{
		"get_url_host": func(field string) string {
			stripped := fmt.Sprintf("regexp_replace(%s, '^(android-app://|https?://)', '')", field)
			return fmt.Sprintf("cast(regexp_extract(%s, '^([^/?]+)', 1) as string)", stripped)
		},
		"get_url_parameter": func(field, urlParameter string) string {
			return fmt.Sprintf("nullif(regexp_extract(%s, %s, 1), '')", field, sqlRawStringLiteral(urlParameterRegex(urlParameter)))
		},
		"get_url_path": func(field string) string {
			stripped := fmt.Sprintf("regexp_replace(%s, '^(android-app://|https?://)', '')", field)
			return fmt.Sprintf("cast(regexp_extract(%s, '^[^/?]+/([^?]*)', 1) as string)", stripped)
		},
	}
}

// ClickHouseURLHelpers returns URL helpers using ClickHouse regex functions.
func ClickHouseURLHelpers() map[string]any {
	return map[string]any{
		"get_url_host": func(field string) string {
			stripped := fmt.Sprintf("replaceRegexpOne(%s, '^(android-app://|https?://)', '')", field)
			return fmt.Sprintf("extract(%s, '^[^/?]+')", stripped)
		},
		"get_url_parameter": func(field, urlParameter string) string {
			return fmt.Sprintf("nullIf(extract(%s, %s), '')", field, sqlBackslashStringLiteral(urlParameterRegex(urlParameter)))
		},
		"get_url_path": func(field string) string {
			stripped := fmt.Sprintf("replaceRegexpOne(%s, '^(android-app://|https?://)', '')", field)
			return fmt.Sprintf("extract(%s, '^[^/?]+/([^?]*)')", stripped)
		},
	}
}

// MySQLURLHelpers returns URL helpers using MySQL string functions.
func MySQLURLHelpers() map[string]any {
	return map[string]any{
		"get_url_host": func(field string) string {
			parsed := fmt.Sprintf(
				"substring_index(substring_index(replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', ''), '/', 1), '?', 1)",
				field,
			)
			return fmt.Sprintf("cast(%s as char)", parsed)
		},
		"get_url_parameter": func(field, urlParameter string) string {
			needle := sqlStringLiteral("&" + urlParameter + "=")
			queryPart := fmt.Sprintf("case when locate('?', %s) > 0 then substring(%s, locate('?', %s) + 1) else %s end", field, field, field, field)
			wrappedQueryPart := fmt.Sprintf("concat('&', %s)", queryPart)
			return fmt.Sprintf(
				"case when instr(%s, %s) > 0 then nullif(substring_index(substring_index(%s, %s, -1), '&', 1), '') else null end",
				wrappedQueryPart, needle, wrappedQueryPart, needle,
			)
		},
		"get_url_path": func(field string) string {
			stripped := fmt.Sprintf("replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', '')", field)
			tail := fmt.Sprintf(`case
        when locate('/', %s) > 0 then substring(%s, locate('/', %s) + 1)
        else ''
    end`, stripped, stripped, stripped)
			return fmt.Sprintf("cast(substring_index(%s, '?', 1) as char)", tail)
		},
	}
}

// TSQLURLHelpers returns URL helpers for SQL Server-family dialects.
func TSQLURLHelpers() map[string]any {
	return map[string]any{
		"get_url_host": func(field string) string {
			stripped := fmt.Sprintf("replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', '')", field)
			beforePath := fmt.Sprintf("left(%s, charindex('/', %s + '/') - 1)", stripped, stripped)
			beforeQuery := fmt.Sprintf("left(%s, charindex('?', %s + '?') - 1)", beforePath, beforePath)
			return fmt.Sprintf("cast(%s as varchar(max))", beforeQuery)
		},
		"get_url_parameter": func(field, urlParameter string) string {
			needle := sqlStringLiteral("&" + urlParameter + "=")
			needleLen := len(urlParameter) + 2
			queryPart := fmt.Sprintf("case when charindex('?', %s) > 0 then substring(%s, charindex('?', %s) + 1, len(%s)) else %s end", field, field, field, field, field)
			wrappedQueryPart := fmt.Sprintf("concat('&', %s)", queryPart)
			valueTail := fmt.Sprintf("substring(%s, charindex(%s, %s) + %d, len(%s))", wrappedQueryPart, needle, wrappedQueryPart, needleLen, wrappedQueryPart)
			value := fmt.Sprintf("left(%s, charindex('&', %s + '&') - 1)", valueTail, valueTail)
			return fmt.Sprintf("case when charindex(%s, %s) > 0 then nullif(%s, '') else null end", needle, wrappedQueryPart, value)
		},
		"get_url_path": func(field string) string {
			stripped := fmt.Sprintf("replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', '')", field)
			startPos := fmt.Sprintf(`case
        when charindex('/', %s) > 0 then charindex('/', %s) + 1
        else len(%s) + 1
    end`, stripped, stripped, stripped)
			tail := fmt.Sprintf("substring(%s, %s, len(%s))", stripped, startPos, stripped)
			parsed := fmt.Sprintf("left(%s, charindex('?', %s + '?') - 1)", tail, tail)
			return fmt.Sprintf("cast(%s as varchar(max))", parsed)
		},
	}
}

// OracleURLHelpers returns URL helpers using Oracle regex functions.
func OracleURLHelpers() map[string]any {
	return map[string]any{
		"get_url_host": func(field string) string {
			stripped := fmt.Sprintf("regexp_replace(regexp_replace(regexp_replace(%s, '^android-app://', ''), '^http://', ''), '^https://', '')", field)
			return fmt.Sprintf("cast(regexp_substr(%s, '^[^/?]+') as varchar2(4000))", stripped)
		},
		"get_url_parameter": func(field, urlParameter string) string {
			return fmt.Sprintf("nullif(regexp_substr(%s, %s, 1, 1, null, 2), '')", field, sqlStringLiteral("(^|[?&])"+regexp.QuoteMeta(urlParameter)+"=([^&]*)"))
		},
		"get_url_path": func(field string) string {
			stripped := fmt.Sprintf("regexp_replace(regexp_replace(regexp_replace(%s, '^android-app://', ''), '^http://', ''), '^https://', '')", field)
			return fmt.Sprintf("cast(regexp_substr(%s, '^[^/?]+/([^?]*)', 1, 1, null, 1) as varchar2(4000))", stripped)
		},
	}
}

// PrestoURLHelpers returns URL helpers for Trino/Athena.
func PrestoURLHelpers() map[string]any {
	return map[string]any{
		"get_url_host": func(field string) string {
			parsed := fmt.Sprintf(
				"split_part(split_part(replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', ''), '/', 1), '?', 1)",
				field,
			)
			return fmt.Sprintf("cast(%s as varchar)", parsed)
		},
		"get_url_parameter": func(field, urlParameter string) string {
			queryPart := fmt.Sprintf("case when strpos(%s, '?') > 0 then split_part(%s, '?', 2) else %s end", field, field, field)
			return fmt.Sprintf(
				"nullif(split_part(split_part(concat('&', %s), %s, 2), '&', 1), '')",
				queryPart,
				sqlStringLiteral("&"+urlParameter+"="),
			)
		},
		"get_url_path": func(field string) string {
			strippedURL := fmt.Sprintf("replace(replace(replace(%s, 'android-app://', ''), 'http://', ''), 'https://', '')", field)
			parsedPath := fmt.Sprintf(
				"case when strpos(%s, '/') > 0 then split_part(substr(%s, strpos(%s, '/') + 1), '?', 1) else '' end",
				strippedURL, strippedURL, strippedURL,
			)
			return fmt.Sprintf("cast(%s as varchar)", parsedPath)
		},
	}
}

func regexURLHelpers(castType, extractFn, replaceFn string, literalFn func(string) string) map[string]any {
	return map[string]any{
		"get_url_host": func(field string) string {
			stripped := fmt.Sprintf("%s(%s, '^(android-app://|https?://)', '')", replaceFn, field)
			return fmt.Sprintf("cast(%s(%s, '^([^/?]+)') as %s)", extractFn, stripped, castType)
		},
		"get_url_parameter": func(field, urlParameter string) string {
			return fmt.Sprintf("nullif(%s(%s, %s), '')", extractFn, field, literalFn(urlParameterRegex(urlParameter)))
		},
		"get_url_path": func(field string) string {
			stripped := fmt.Sprintf("%s(%s, '^(android-app://|https?://)', '')", replaceFn, field)
			return fmt.Sprintf("cast(%s(%s, '^[^/?]+/([^?]*)') as %s)", extractFn, stripped, castType)
		},
	}
}

func sqlStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func sqlRawStringLiteral(s string) string {
	return "r" + sqlStringLiteral(s)
}

func sqlBackslashStringLiteral(s string) string {
	return sqlStringLiteral(strings.ReplaceAll(s, `\`, `\\`))
}

func urlParameterRegex(urlParameter string) string {
	return "(?:^|[?&])" + regexp.QuoteMeta(urlParameter) + "=([^&]*)"
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

func getPowersOfTwo(upperBound int) int {
	if upperBound <= 1 {
		return 1
	}
	n := bits.Len(uint(upperBound - 1))
	if n == 0 {
		return 1
	}
	return n
}

func extractStringListFromVarArgs(va *exec.VarArgs) []string {
	if len(va.Args) == 0 {
		return nil
	}
	if va.Args[0].IsList() {
		return extractStringListFromValue(va.Args[0])
	}
	result := make([]string, len(va.Args))
	for i, arg := range va.Args {
		result[i] = arg.String()
	}
	return result
}

func extractStringListFromValue(val *exec.Value) []string {
	if !val.IsList() {
		return []string{val.String()}
	}
	n := val.Len()
	result := make([]string, n)
	for i := range n {
		result[i] = val.Index(i).String()
	}
	return result
}

func getKwArgString(va *exec.VarArgs, key, defaultValue string) string {
	if v, ok := va.KwArgs[key]; ok {
		return v.String()
	}
	return defaultValue
}

func getKwArgBool(va *exec.VarArgs, key string, defaultValue bool) bool {
	if v, ok := va.KwArgs[key]; ok {
		return v.IsTrue()
	}
	return defaultValue
}
