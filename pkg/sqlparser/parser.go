package sqlparser

// NOTE: There should be NO space between the comments and the `import "C"` line.
// The -ldl is sometimes necessary to fix linker errors about `dlsym`.

/*
#cgo LDFLAGS: -L${SRCDIR}/../../lib/ -lparser -ldl
#include "../../lib/parser.h"
*/
import "C"
import "fmt"

func ParseUsedTables() {
	contents2 := `
            with t1 as (
            select *
            from table1
        ),
        t2 as (
            select *
            from table2
        )
        select *
        from t1
        join t2
            using(a)`

	res := C.extract_table_names(C.CString(contents2))
	usedTables := C.GoString(res)

	println(usedTables)

	fmt.Println("This is a highly experimental command that doesn't really do anything, it just exists to check the cross-build abilities of CGo.")
}
