package sqlparser

// NOTE: There should be NO space between the comments and the `import "C"` line.
// The -ldl is sometimes necessary to fix linker errors about `dlsym`.

/*
#cgo LDFLAGS: -L${SRCDIR}/../../lib/ -lparser -ldl
#include "../../lib/parser.h"
*/
import "C"
import (
	"encoding/json"
)

func ParseUsedTables(query string) ([]string, error) {
	res := C.extract_table_names(C.CString(query))
	usedTables := C.GoString(res)

	var tables []string
	err := json.Unmarshal([]byte(usedTables), &tables)

	return tables, err
}
