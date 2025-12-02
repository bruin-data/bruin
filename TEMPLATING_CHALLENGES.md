# Challenges in Templating Cloud Integration Tests

This document outlines the challenges and difficulties in creating a unified templating system for cloud integration tests across multiple platforms (BigQuery, Snowflake, Postgres, Redshift, Athena, MySQL, etc.).

SOLVABLE ## 1. Platform Capability Differences

### Problem
Not all platforms support the same materialization strategies and features.

**Examples:**
- **MSSQL**: No SCD2 support (missing from materialization map)
- **MySQL**: Supports SCD2 for tables, but views have restrictions
- **Some platforms**: May not support certain materialization strategies

**Impact:**
- Cannot run all tests on all platforms
- Need conditional test execution based on platform capabilities

**Solution:**
- Platform capability matrix/config that defines which tests to skip per platform
- Feature flags for platform-specific capabilities

---

DIFFICULT ## 2. SQL Dialect Differences

### Problem
Beyond schema naming, SQL dialects vary significantly across platforms.

**Examples:**
- **DDL Syntax**: 
  - Postgres/Snowflake: `CREATE OR REPLACE VIEW`
  - MSSQL: `CREATE OR ALTER VIEW`
- **Identifier Quoting**: 
  - Postgres needs `"schema"."table"` for case-sensitive names
  - Others have different quoting rules
- **Date/Time Functions**: Platform-specific syntax
- **Data Types**: Different type systems (e.g., BigQuery STRUCT, Postgres ARRAY)
- **NULL Handling**: Different behaviors in aggregations and comparisons

**Impact:**
- Cannot use identical SQL across all platforms
- Need platform-specific SQL generation or templates

**Solution:**
- Template variables or platform-specific SQL generators
- Platform-specific SQL fragments in separate files
- Use ANSI SQL where possible, with platform-specific overrides

---

SOLVABLE ## 3. Exit Code and Error Message Differences

### Problem
Same operations return different exit codes and error messages across platforms.

**Examples:**
- **DROP TABLE Exit Codes**:
  - Snowflake/Athena: `ExitCode: 0` (success)
  - BigQuery/Postgres/Redshift: `ExitCode: 1` (expected failure)
- **Error Messages**:
  - Postgres/Redshift: `"relation does not exist"` or `"field descriptions are not available"`
  - Snowflake: `"Object does not exist or not authorized"`
  - Athena: `"Table not found"` or `"Table does not exist"`
- **DDL Statements**: Postgres/Redshift return errors for field descriptions on DDL

**Impact:**
- Cannot use identical assertions across platforms
- Need platform-specific expected values

**Solution:**
- Platform-specific assertion configs
- Map expected exit codes and error patterns per platform
- Conditional assertion logic based on platform


---

SOLVABLE? ## 5. Schema/Table Naming Conventions - 

### Problem
Different platforms use different schema/table naming patterns.

**Examples:**
- **BigQuery**: `dataset.table` (no schema prefix in queries)
- **Postgres/Redshift**: `schema.table` or `public.table`
- **Snowflake**: `schema.table` or just `table` (schema optional)
- **Athena**: Just `table` (no schema prefix at all)

**Impact:**
- Cannot use identical table references in queries
- Need platform-specific naming patterns

**Solution:**
- Template variables for schema/table naming patterns
- Platform config defining naming conventions
- Schema prefix resolution logic

---

SOLVABLE ## 6. Test Coverage Differences

### Problem
Not all platforms have the same test suites.

**Examples:**
- **Redshift**: 6+ materialization tests (truncate-insert, append, merge, delete-insert, time-interval, ddl)
- **BigQuery**: Unique tests (drop-on-mismatch, dry-run, nullable merge)
- **Postgres**: Metadata push test
- **Snowflake/Athena**: Simpler test suites focused on basic operations

**Impact:**
- Cannot template all tests - some are platform-specific
- Need to handle optional vs. required tests

**Solution:**
- Test tagging/categorization system
- Mark which tests apply to which platforms
- Separate common tests from platform-specific tests


SOLVABLE ## 8. Column Naming and Case Sensitivity

### Problem
Different platforms handle column naming and case sensitivity differently.

**Examples:**
- **Uppercase**: `PRODUCT_ID, PRODUCT_NAME, PRICE, STOCK`
- **Lowercase**: `product_id, product_name, stock`
- **Mixed Case**: `ID, Name, Price, _is_current`
- **Case Sensitivity**: Some platforms are case-sensitive, others are not

**Impact:**
- Cannot use identical column references
- Need platform-specific column naming conventions

**Solution:**
- Platform-specific column naming conventions in config
- Template variables for column names
- Case normalization logic

---

SOLVABLE ## 9. Platform-Specific Features

### Problem
Some platforms have unique features that don't exist elsewhere.

**Examples:**
- **BigQuery**: 
  - Complex UNION queries for drop-on-mismatch
  - Asset metadata queries (`internal asset-metadata`)
  - Unique DDL behavior
- **Postgres**: 
  - Metadata push functionality
  - Specific error handling for DDL
- **Platform-specific CLI features**: Not all platforms support all Bruin features

**Impact:**
- Cannot template platform-specific features
- Need separate test suites for unique features

**Solution:**
- Platform-specific test suites alongside common tests
- Feature detection and conditional test execution
- Extension points for platform-specific tests

---


SOLVABLE ## 11. Query Syntax Differences

### Problem
Even for the same logical query, syntax varies across platforms.

**Examples:**
- **Column Spacing**: 
  - Some: `SELECT product_id, product_name, price` (spaced)
  - Others: `SELECT product_id,product_name,stock` (no spaces)
- **ORDER BY**: 
  - Some queries include `ORDER BY`, others don't
  - Multiple `ORDER BY` columns in different formats
- **Complex Queries**: 
  - BigQuery has unique UNION ALL patterns
  - Platform-specific aggregation functions

**Impact:**
- Cannot use identical query strings
- Need platform-specific query templates

**Solution:**
- Query template system with platform-specific rendering
- Standardized query builders where possible
- Platform-specific query fragments

---

SOLVABLE ## 12. Test Structure and Organization - copy bigquery

### Problem
Current test structure is platform-specific, making templating non-trivial.

**Examples:**
- **BigQuery**: Uses temp directories and file copying
- **Postgres/Redshift**: Uses temp directories for SCD2 tests
- **Snowflake**: Uses current folder directly
- **Test Organization**: Different platforms organize tests differently

**Impact:**
- Cannot directly template existing test structure
- Need refactoring to common structure

**Solution:**
- Standardize test structure across platforms
- Common test runner with platform-specific configs
- Gradual migration strategy

---

## 13. Debugging and Maintenance

### Problem
Templated tests are harder to debug and maintain.

**Examples:**
- **Error Messages**: Less clear when tests fail (which platform? which template?)
- **Debugging**: Harder to trace failures through template layers
- **Maintenance**: Changes to templates affect all platforms

**Impact:**
- Increased debugging time
- Risk of breaking multiple platforms with single change
- Harder to understand test failures

**Solution:**
- Comprehensive logging with platform context
- Clear error messages indicating platform and template
- Test isolation to prevent cascading failures
- Strong typing for platform configs

---


## Recommended Mitigation Strategies

### 1. Start with Common Tests
Template the most common tests first (products-create-and-validate, SCD2) before tackling platform-specific ones.

### 2. Keep Platform-Specific Tests Separate
Don't force-fit unique tests into templates. Maintain separate test suites for platform-specific features.

### 3. Use Feature Flags
Mark which features/tests apply to which platforms using a capability matrix.

### 4. Gradual Migration
Convert one test suite at a time rather than attempting a big-bang migration.

### 5. Strong Typing
Use Go structs for platform configs to catch errors at compile time.

### 6. Comprehensive Documentation
Document platform differences and how the templating system handles them.

### 7. Test the Template System
Create tests for the templating system itself to ensure it works correctly.

---

## Conclusion

While templating cloud integration tests offers significant benefits (maintainability, consistency, easier platform addition), it requires careful design to handle platform differences. The key is finding the right balance between abstraction and platform-specific handling.

A hybrid approach that templates common tests while maintaining platform-specific test suites for unique features is likely the most practical solution.

