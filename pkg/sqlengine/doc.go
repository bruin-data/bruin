// Package sqlengine parses, generates and analyzes SQL across Bruin's dialects: tokenizer,
// expression tree, per-dialect parser and generator, transforms, scope analysis, schema,
// optimizer rules (qualify, type annotation, simplification, subquery unnesting/merging, ...) and
// column lineage.
//
// It started as a Go fork of the Python library sqlglot (v30.13.0, MIT licensed; see
// LICENSE.sqlglot) and is maintained here as regular Go code; it does not track sqlglot's source.
// Much of it still follows sqlglot's structure and naming (`_parse_table_parts` became
// parseTableParts, dialect overrides live in d_<dialect>*.go), which is not a constraint on
// changes.
//
// sqlglot remains the reference for behavior. The conformance tests (conformance_parse_test.go,
// annotate_conformance_test.go) replay golden files recorded from sqlglot by the scripts in
// codegen/ (see codegen/README.md): the engine must fail to parse the same statements, generate the
// same SQL back and infer the same column types. Internals (token streams, tree shapes, tables)
// are free to differ.
//
// Supported dialects: the base dialect (""), athena, bigquery, clickhouse, databricks, doris,
// duckdb, fabric, hive, mysql, oracle, postgres, presto, redshift, snowflake, spark, spark2,
// sqlite, starrocks, trino and tsql.
//
// Errors: entry points return ParseError, TokenError, ValueError, OptimizeError, SchemaError or
// UnsupportedError values.
//
// The package is safe for concurrent use; dialect prototypes are built once and shared read-only.
package sqlengine
