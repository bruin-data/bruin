//go:build !cgo || !(darwin || linux)

package sqlparser

import "github.com/pkg/errors"

func ensureRustSQLParserFFI() error {
	return errors.New("rust sql parser ffi requires cgo on darwin or linux")
}

func rustSQLParserFFIExecute(string) (string, error) {
	return "", ensureRustSQLParserFFI()
}
