//go:build !cgo || !(darwin || linux)

package sqlparser

import "github.com/pkg/errors"

func ensureRustSQLParserFFI() error {
	return errors.New("rust sql parser ffi requires cgo on darwin or linux")
}

func rustFFIGetTables(query, dialect string) (string, error) {
	return "", ensureRustSQLParserFFI()
}

func rustFFIRenameTables(query, dialect, tableMappingJSON string) (string, error) {
	return "", ensureRustSQLParserFFI()
}

func rustFFIAddLimit(query string, limit int, dialect string) (string, error) {
	return "", ensureRustSQLParserFFI()
}

func rustFFIIsSingleSelect(query, dialect string) (string, error) {
	return "", ensureRustSQLParserFFI()
}

func rustFFIColumnLineage(query, dialect, schemaJSON string) (string, error) {
	return "", ensureRustSQLParserFFI()
}
