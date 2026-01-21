#!/usr/bin/env node
/**
 * SQL Formatter Script
 * 
 * This script reads SQL from stdin and outputs formatted SQL to stdout.
 * Used by the Bruin CLI to format SQL queries in query logs.
 * 
 * Usage:
 *   echo "SELECT * FROM users WHERE id = 1" | node scripts/format-sql.js
 *   echo "SELECT * FROM users WHERE id = 1" | node scripts/format-sql.js --dialect=bigquery
 * 
 * Options:
 *   --dialect=<dialect>  SQL dialect (bigquery, snowflake, postgres, mysql, etc.)
 */

const { format } = require('sql-formatter');

// Read SQL from stdin
let sql = '';
process.stdin.setEncoding('utf8');

process.stdin.on('data', (chunk) => {
    sql += chunk;
});

process.stdin.on('end', () => {
    try {
        // Parse command line arguments for dialect
        const args = process.argv.slice(2);
        let dialect = 'sql';  // default dialect
        
        for (const arg of args) {
            if (arg.startsWith('--dialect=')) {
                dialect = arg.split('=')[1];
            }
        }
        
        // Map common dialect names to sql-formatter supported dialects
        const dialectMap = {
            'bigquery': 'bigquery',
            'bq': 'bigquery',
            'snowflake': 'snowflake',
            'sf': 'snowflake',
            'postgres': 'postgresql',
            'postgresql': 'postgresql',
            'pg': 'postgresql',
            'mysql': 'mysql',
            'redshift': 'redshift',
            'spark': 'spark',
            'trino': 'trino',
            'sql': 'sql',
            'duckdb': 'sql',
            'clickhouse': 'sql',
            'athena': 'trino',
            'mssql': 'transactsql',
            'tsql': 'transactsql',
            'transactsql': 'transactsql',
            'synapse': 'transactsql',
            'databricks': 'spark',
        };
        
        const mappedDialect = dialectMap[dialect.toLowerCase()] || 'sql';
        
        const formatted = format(sql, {
            language: mappedDialect,
            tabWidth: 2,
            useTabs: false,
            keywordCase: 'upper',
            linesBetweenQueries: 2,
        });
        
        process.stdout.write(formatted);
        process.exit(0);
    } catch (error) {
        // If formatting fails, output the original SQL
        process.stderr.write(`Warning: SQL formatting failed: ${error.message}\n`);
        process.stdout.write(sql);
        process.exit(0);  // Still exit 0 to not break the flow
    }
});

process.stdin.on('error', (error) => {
    process.stderr.write(`Error reading stdin: ${error.message}\n`);
    process.exit(1);
});
