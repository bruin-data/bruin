//go:build cgo && darwin

package sqlparser

/*
#cgo amd64 LDFLAGS: -L${SRCDIR}/rustffi/target/release -L${SRCDIR}/rustffi/target/x86_64-apple-darwin/release -lbruin_rustsqlparser
#cgo arm64 LDFLAGS: -L${SRCDIR}/rustffi/target/release -L${SRCDIR}/rustffi/target/aarch64-apple-darwin/release -lbruin_rustsqlparser
#include <stdlib.h>
#include <stdint.h>

char* bruin_rustsqlparser_get_tables(const char* query, const char* dialect);
char* bruin_rustsqlparser_rename_tables(const char* query, const char* dialect, const char* table_mapping_json);
char* bruin_rustsqlparser_add_limit(const char* query, int64_t limit, const char* dialect);
char* bruin_rustsqlparser_is_single_select(const char* query, const char* dialect);
char* bruin_rustsqlparser_column_lineage(const char* query, const char* dialect, const char* schema_json);
void bruin_rustsqlparser_free_string(char* value);
*/
import "C"

import (
	"unsafe"

	"github.com/pkg/errors"
)

func ensureRustSQLParserFFI() error {
	return nil
}

func ffiCall(ptr *C.char) (string, error) {
	if ptr == nil {
		return "", errors.New("rust sql parser ffi returned null")
	}
	defer C.bruin_rustsqlparser_free_string(ptr)
	return C.GoString(ptr), nil
}

func rustFFIGetTables(query, dialect string) (string, error) {
	cQuery := C.CString(query)
	cDialect := C.CString(dialect)
	defer C.free(unsafe.Pointer(cQuery))
	defer C.free(unsafe.Pointer(cDialect))
	return ffiCall(C.bruin_rustsqlparser_get_tables(cQuery, cDialect))
}

func rustFFIRenameTables(query, dialect, tableMappingJSON string) (string, error) {
	cQuery := C.CString(query)
	cDialect := C.CString(dialect)
	cMapping := C.CString(tableMappingJSON)
	defer C.free(unsafe.Pointer(cQuery))
	defer C.free(unsafe.Pointer(cDialect))
	defer C.free(unsafe.Pointer(cMapping))
	return ffiCall(C.bruin_rustsqlparser_rename_tables(cQuery, cDialect, cMapping))
}

func rustFFIAddLimit(query string, limit int, dialect string) (string, error) {
	cQuery := C.CString(query)
	cDialect := C.CString(dialect)
	defer C.free(unsafe.Pointer(cQuery))
	defer C.free(unsafe.Pointer(cDialect))
	return ffiCall(C.bruin_rustsqlparser_add_limit(cQuery, C.int64_t(limit), cDialect))
}

func rustFFIIsSingleSelect(query, dialect string) (string, error) {
	cQuery := C.CString(query)
	cDialect := C.CString(dialect)
	defer C.free(unsafe.Pointer(cQuery))
	defer C.free(unsafe.Pointer(cDialect))
	return ffiCall(C.bruin_rustsqlparser_is_single_select(cQuery, cDialect))
}

func rustFFIColumnLineage(query, dialect, schemaJSON string) (string, error) {
	cQuery := C.CString(query)
	cDialect := C.CString(dialect)
	cSchema := C.CString(schemaJSON)
	defer C.free(unsafe.Pointer(cQuery))
	defer C.free(unsafe.Pointer(cDialect))
	defer C.free(unsafe.Pointer(cSchema))
	return ffiCall(C.bruin_rustsqlparser_column_lineage(cQuery, cDialect, cSchema))
}
