// Package sqlengine parses, generates and analyzes SQL across Bruin's dialects: tokenizer,
// expression tree, per-dialect parser and generator, transforms, scope analysis, schema,
// optimizer rules (qualify, type annotation, simplification, subquery unnesting/merging, ...) and
// column lineage.
//
// It is a Go port of the Python library sqlglot (v30.13.0, MIT licensed; see LICENSE.sqlglot),
// which Bruin previously embedded. The port keeps sqlglot's structure and naming so it can be
// compared against the Python source and upgraded mechanically: `_parse_table_parts` is
// parseTableParts, `table_sql` is tableSQL, dialect overrides live in d_<dialect>*.go. Pure-data
// tables (token types, expression classes, dialect settings, type mappings) are generated from the
// Python objects by codegen/gen.py into zz_*.go.
//
// Supported dialects: the base dialect (""), athena, bigquery, clickhouse, databricks, doris,
// duckdb, fabric, hive, mysql, oracle, postgres, presto, redshift, snowflake, spark, spark2,
// sqlite, starrocks, trino and tsql.
//
// Conformance is verified against sqlglot's own test-suite corpus (see conformance_*_test.go and
// codegen/README.md): tokens, full parse trees and regenerated SQL match Python exactly.
//
// Errors: entry points return ParseError, TokenError, ValueError, OptimizeError, SchemaError or
// UnsupportedError values whose messages match sqlglot's. Where sqlglot itself fails with a plain
// Python exception (AttributeError, KeyError, ...), a ValueError carrying the same message is used.
//
// The package is safe for concurrent use; dialect prototypes are built once and shared read-only.
package sqlengine
