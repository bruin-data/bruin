package cmd

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	duck "github.com/bruin-data/bruin/pkg/duckdb"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAlterStatementsExecutability tests that generated ALTER statements can be executed successfully.
func TestAlterStatementsExecutability(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	t.Parallel()

	t.Run("add missing column", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		// Create temporary DuckDB database
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test.db")

		db, err := sql.Open("duckdb", dbPath)
		require.NoError(t, err)
		defer db.Close()

		// Create two tables with different schemas
		_, err = db.ExecContext(ctx, `
			CREATE TABLE users_source (
				id INTEGER PRIMARY KEY,
				name VARCHAR(100) NOT NULL,
				email VARCHAR(255) NOT NULL
			)
		`)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, `
			CREATE TABLE users_target (
				id INTEGER PRIMARY KEY,
				name VARCHAR(100) NOT NULL
			)
		`)
		require.NoError(t, err)

		// Insert some test data
		_, err = db.ExecContext(ctx, `INSERT INTO users_source VALUES (1, 'Alice', 'alice@example.com')`)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO users_target VALUES (1, 'Alice')`)
		require.NoError(t, err)

		// Create DuckDB clients
		config1 := &duck.Config{Path: dbPath}
		client1, err := duck.NewClient(config1)
		require.NoError(t, err)

		// Compare tables
		result, err := compareTables(ctx, client1, client1, "users_source", "users_target", true)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Generate ALTER statements
		statements := generateAlterStatements(*result, "duckdb", "duckdb", "", false)
		require.NotEmpty(t, statements, "Expected ALTER statements to be generated")

		// Execute the generated statements
		// Note: DuckDB doesn't support adding columns with NOT NULL constraints directly,
		// so we need to strip out the constraint comments and add the column without constraints
		for _, stmt := range statements {
			t.Logf("Executing: %s", stmt)
			// For DuckDB, remove comment parts from ADD COLUMN statements
			cleanStmt := strings.Split(stmt, "/*")[0]
			cleanStmt = strings.TrimSpace(cleanStmt)
			if !strings.HasSuffix(cleanStmt, ";") {
				cleanStmt += ";"
			}
			t.Logf("Cleaned statement: %s", cleanStmt)
			_, err = db.ExecContext(ctx, cleanStmt)
			require.NoError(t, err, "Generated ALTER statement should execute successfully")
		}

		// Verify the schema was updated
		var columnCount int
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_name = 'users_target' AND column_name = 'email'
		`).Scan(&columnCount)
		require.NoError(t, err)
		assert.Equal(t, 1, columnCount, "email column should exist in users_target after ALTER")
	})

	t.Run("change column type", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test_type_change.db")

		db, err := sql.Open("duckdb", dbPath)
		require.NoError(t, err)
		defer db.Close()

		// Create tables with different column types
		_, err = db.ExecContext(ctx, `
			CREATE TABLE products_source (
				id INTEGER PRIMARY KEY,
				price DECIMAL(10,2)
			)
		`)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, `
			CREATE TABLE products_target (
				id INTEGER PRIMARY KEY,
				price INTEGER
			)
		`)
		require.NoError(t, err)

		config := &duck.Config{Path: dbPath}
		client, err := duck.NewClient(config)
		require.NoError(t, err)

		result, err := compareTables(ctx, client, client, "products_source", "products_target", true)
		require.NoError(t, err)
		require.NotNil(t, result)

		statements := generateAlterStatements(*result, "duckdb", "duckdb", "", false)
		require.NotEmpty(t, statements)

		// Execute ALTER statements
		for _, stmt := range statements {
			t.Logf("Executing: %s", stmt)
			_, err = db.ExecContext(ctx, stmt)
			require.NoError(t, err, "Type change ALTER statement should execute successfully")
		}

		// Verify the type was changed
		var dataType string
		err = db.QueryRowContext(ctx, `
			SELECT data_type
			FROM information_schema.columns
			WHERE table_name = 'products_target' AND column_name = 'price'
		`).Scan(&dataType)
		require.NoError(t, err)
		assert.Contains(t, dataType, "DECIMAL", "price column should be DECIMAL type after ALTER")
	})

	t.Run("change nullability", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test_nullability.db")

		db, err := sql.Open("duckdb", dbPath)
		require.NoError(t, err)
		defer db.Close()

		// Create tables with different nullability
		_, err = db.ExecContext(ctx, `
			CREATE TABLE items_source (
				id INTEGER PRIMARY KEY,
				description TEXT
			)
		`)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, `
			CREATE TABLE items_target (
				id INTEGER PRIMARY KEY,
				description TEXT NOT NULL
			)
		`)
		require.NoError(t, err)

		config := &duck.Config{Path: dbPath}
		client, err := duck.NewClient(config)
		require.NoError(t, err)

		result, err := compareTables(ctx, client, client, "items_source", "items_target", true)
		require.NoError(t, err)
		require.NotNil(t, result)

		statements := generateAlterStatements(*result, "duckdb", "duckdb", "", false)
		require.NotEmpty(t, statements)

		// Execute ALTER statements
		for _, stmt := range statements {
			t.Logf("Executing: %s", stmt)
			_, err = db.ExecContext(ctx, stmt)
			require.NoError(t, err, "Nullability change ALTER statement should execute successfully")
		}

		// Verify nullability was changed
		var isNullable string
		err = db.QueryRowContext(ctx, `
			SELECT is_nullable
			FROM information_schema.columns
			WHERE table_name = 'items_target' AND column_name = 'description'
		`).Scan(&isNullable)
		require.NoError(t, err)
		assert.Equal(t, "YES", isNullable, "description column should be nullable after ALTER")
	})

	t.Run("multiple changes combined", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test_multiple.db")

		db, err := sql.Open("duckdb", dbPath)
		require.NoError(t, err)
		defer db.Close()

		// Create tables with multiple differences
		_, err = db.ExecContext(ctx, `
			CREATE TABLE employees_source (
				id INTEGER PRIMARY KEY,
				name VARCHAR(100) NOT NULL,
				salary DECIMAL(12,2),
				department VARCHAR(50) NOT NULL
			)
		`)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, `
			CREATE TABLE employees_target (
				id INTEGER PRIMARY KEY,
				name VARCHAR(100) NOT NULL,
				salary INTEGER
			)
		`)
		require.NoError(t, err)

		config := &duck.Config{Path: dbPath}
		client, err := duck.NewClient(config)
		require.NoError(t, err)

		result, err := compareTables(ctx, client, client, "employees_source", "employees_target", true)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.True(t, result.HasSchemaDifferences, "Should detect schema differences")

		statements := generateAlterStatements(*result, "duckdb", "duckdb", "", false)
		require.NotEmpty(t, statements)
		// DuckDB generates separate statements for each change
		require.GreaterOrEqual(t, len(statements), 2, "Should have multiple separate statements for DuckDB")

		// Execute all generated statements
		for _, stmt := range statements {
			t.Logf("Executing: %s", stmt)
			// Clean up DuckDB constraint comments
			cleanStmt := strings.Split(stmt, "/*")[0]
			cleanStmt = strings.TrimSpace(cleanStmt)
			if !strings.HasSuffix(cleanStmt, ";") {
				cleanStmt += ";"
			}
			t.Logf("Cleaned statement: %s", cleanStmt)
			_, err = db.ExecContext(ctx, cleanStmt)
			require.NoError(t, err, "ALTER statement should execute successfully")
		}

		// Verify all changes were applied
		var columnCount int
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_name = 'employees_target'
		`).Scan(&columnCount)
		require.NoError(t, err)
		assert.Equal(t, 4, columnCount, "Should have 4 columns after adding department")

		var dataType string
		err = db.QueryRowContext(ctx, `
			SELECT data_type
			FROM information_schema.columns
			WHERE table_name = 'employees_target' AND column_name = 'salary'
		`).Scan(&dataType)
		require.NoError(t, err)
		assert.Contains(t, dataType, "DECIMAL", "salary should be DECIMAL type after ALTER")
	})

	t.Run("reverse direction", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()

		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "test_reverse.db")

		db, err := sql.Open("duckdb", dbPath)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.ExecContext(ctx, `
			CREATE TABLE orders_source (
				id INTEGER PRIMARY KEY,
				amount DECIMAL(10,2)
			)
		`)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, `
			CREATE TABLE orders_target (
				id INTEGER PRIMARY KEY,
				amount DECIMAL(10,2),
				status VARCHAR(20)
			)
		`)
		require.NoError(t, err)

		config := &duck.Config{Path: dbPath}
		client, err := duck.NewClient(config)
		require.NoError(t, err)

		result, err := compareTables(ctx, client, client, "orders_source", "orders_target", true)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Generate statements with reverse=true (modify source to match target)
		statements := generateAlterStatements(*result, "duckdb", "duckdb", "", true)
		require.NotEmpty(t, statements)
		require.Contains(t, statements[0], "orders_source", "Should modify source table when reverse=true")

		// Execute the statements
		for _, stmt := range statements {
			t.Logf("Executing: %s", stmt)
			_, err = db.ExecContext(ctx, stmt)
			require.NoError(t, err, "Reverse ALTER statement should execute successfully")
		}

		// Verify source table was modified
		var columnCount int
		err = db.QueryRowContext(ctx, `
			SELECT COUNT(*)
			FROM information_schema.columns
			WHERE table_name = 'orders_source' AND column_name = 'status'
		`).Scan(&columnCount)
		require.NoError(t, err)
		assert.Equal(t, 1, columnCount, "status column should exist in orders_source after reverse ALTER")
	})
}

// TestAlterStatementsWithRealConfig tests ALTER statement generation with actual .bruin.yml config.
func TestAlterStatementsWithRealConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// This test requires a real .bruin.yml config file
	configPath := "../.bruin.yml"
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Skip("Skipping test: .bruin.yml not found")
	}

	t.Parallel()

	t.Run("detects dialect from config connections", func(t *testing.T) {
		t.Parallel()
		// This is more of a documentation test showing how the feature works
		// with real configuration

		ctx := context.Background()
		tmpDir := t.TempDir()
		dbPath := filepath.Join(tmpDir, "config_test.db")

		db, err := sql.Open("duckdb", dbPath)
		require.NoError(t, err)
		defer db.Close()

		_, err = db.ExecContext(ctx, `
			CREATE TABLE test_source (id INTEGER, name VARCHAR(50))
		`)
		require.NoError(t, err)

		_, err = db.ExecContext(ctx, `
			CREATE TABLE test_target (id INTEGER)
		`)
		require.NoError(t, err)

		config := &duck.Config{Path: dbPath}
		client, err := duck.NewClient(config)
		require.NoError(t, err)

		result, err := compareTables(ctx, client, client, "test_source", "test_target", true)
		require.NoError(t, err)

		// Test auto-detection
		statements := generateAlterStatements(*result, "duckdb", "duckdb", "", false)
		require.NotEmpty(t, statements)
		assert.Contains(t, statements[0], `"test_target"`, "Should use DuckDB double-quote syntax")

		// Test explicit dialect override
		statementsOverride := generateAlterStatements(*result, "duckdb", "duckdb", "bigquery", false)
		require.NotEmpty(t, statementsOverride)
		assert.Contains(t, statementsOverride[0], "`test_target`", "Should use BigQuery backtick syntax")
	})
}
