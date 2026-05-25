package ansisql

import "fmt"

// DeduplicateQualify generates a deduplication query using the QUALIFY clause.
// Supported by Snowflake, BigQuery, Databricks, Redshift, and DuckDB.
func DeduplicateQualify(relation, partitionBy, orderBy string) string {
	return fmt.Sprintf(`select *
    from %s as _bruin_source
    qualify
        row_number() over (
            partition by %s
            order by %s
        ) = 1`, relation, partitionBy, orderBy)
}

// DeduplicateDistinctOn generates a deduplication query using DISTINCT ON.
// Supported by Postgres.
func DeduplicateDistinctOn(relation, partitionBy, orderBy string) string {
	return fmt.Sprintf(`select
        distinct on (%s) *
    from %s
    order by %s, %s`, partitionBy, relation, partitionBy, orderBy)
}

// DeduplicateSubquery generates a deduplication query using TOP WITH TIES.
// For platforms without QUALIFY or DISTINCT ON support (MSSQL, Synapse, Fabric).
// The output contains only the original relation columns (no internal helper columns).
func DeduplicateSubquery(relation, partitionBy, orderBy string) string {
	return fmt.Sprintf(`select top (1) with ties *
    from %s
    order by row_number() over (
        partition by %s
        order by %s
    )`, relation, partitionBy, orderBy)
}

// DeduplicateArrayAgg generates a deduplication query using ordered array aggregation.
// Supported by Trino and Athena, where QUALIFY/NATURAL JOIN are unavailable.
func DeduplicateArrayAgg(relation, partitionBy, orderBy string) string {
	return fmt.Sprintf(`select (array_agg(_bruin_source order by %s)[1]).*
    from %s as _bruin_source
    group by %s`, orderBy, relation, partitionBy)
}

// DeduplicateNaturalJoinNoAs generates the fallback natural-join deduplication shape without AS table aliases.
// This is useful for dialects like Oracle that do not support AS for table aliases.
func DeduplicateNaturalJoinNoAs(relation, partitionBy, orderBy string) string {
	return fmt.Sprintf(`with row_numbered as (
        select
            bruin_inner.*,
            row_number() over (
                partition by %s
                order by %s
            ) as __bruin_row_number
        from %s bruin_inner
    )

    select
        distinct data.*
    from %s data
    natural join row_numbered
    where row_numbered.__bruin_row_number = 1`, partitionBy, orderBy, relation, relation)
}

// DateAddInterval generates DATE_ADD(start, INTERVAL n datepart) syntax used by BigQuery and MySQL.
func DateAddInterval(datepart, n, start string) string {
	return fmt.Sprintf("DATE_ADD(%s, INTERVAL %s %s)", start, n, datepart)
}

// DateAddQuoted generates date_add('datepart', n, start) syntax used by Athena and Trino.
func DateAddQuoted(datepart, n, start string) string {
	return fmt.Sprintf("date_add('%s', %s, %s)", datepart, n, start)
}

// HashBytesHashFn returns a MSSQL-family HASHBYTES hash expression for use with SurrogateKeyWith.
func HashBytesHashFn(expr string) string {
	return fmt.Sprintf("convert(varchar(32), hashbytes('md5', %s), 2)", expr)
}
