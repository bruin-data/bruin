//go:build cgo && linux

package sqlparser

/*
#cgo LDFLAGS: -L${SRCDIR}/rustffi/target/release -lbruin_rustsqlparser -ldl -lpthread -lm -lc
#include <stdlib.h>

char* bruin_rustsqlparser_execute(const char* input);
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

func rustSQLParserFFIExecute(command string) (string, error) {
	cCommand := C.CString(command)
	defer C.free(unsafe.Pointer(cCommand))

	resp := C.bruin_rustsqlparser_execute(cCommand)
	if resp == nil {
		return "", errors.New("rust sql parser ffi returned null")
	}
	defer C.bruin_rustsqlparser_free_string(resp)

	return C.GoString(resp), nil
}
