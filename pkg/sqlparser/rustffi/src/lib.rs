mod compat;

use serde_json::{json, Value};
use std::collections::HashMap;
use std::ffi::{c_char, CStr, CString};
use std::str::FromStr;

use polyglot_sql::DialectType;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn into_c_string(value: String) -> *mut c_char {
    match CString::new(value) {
        Ok(value) => value.into_raw(),
        Err(_) => CString::new("{\"error\":\"failed to encode response\"}")
            .expect("static JSON is valid")
            .into_raw(),
    }
}

fn error_json(msg: String) -> *mut c_char {
    into_c_string(json!({ "error": msg }).to_string())
}

fn read_cstr<'a>(ptr: *const c_char) -> Result<&'a str, String> {
    if ptr.is_null() {
        return Err("null pointer".into());
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|e| e.to_string())
}

fn read_dialect(ptr: *const c_char) -> Result<DialectType, String> {
    let s = read_cstr(ptr)?;
    if s.is_empty() {
        return Ok(DialectType::Generic);
    }
    DialectType::from_str(s).map_err(|e| e.to_string())
}

/// Wrap a fallible closure with panic-catching so the FFI boundary is safe.
fn ffi_call(f: impl FnOnce() -> Result<String, String> + std::panic::UnwindSafe) -> *mut c_char {
    match std::panic::catch_unwind(f) {
        Ok(Ok(json)) => into_c_string(json),
        Ok(Err(err)) => error_json(err),
        Err(_) => error_json("panic in rust sql parser".into()),
    }
}

// ---------------------------------------------------------------------------
// FFI entry points
// ---------------------------------------------------------------------------

#[no_mangle]
pub extern "C" fn bruin_rustsqlparser_free_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe {
            let _ = CString::from_raw(ptr);
        }
    }
}

#[no_mangle]
pub extern "C" fn bruin_rustsqlparser_get_tables(
    query: *const c_char,
    dialect: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        let query = read_cstr(query)?;
        let dialect = read_dialect(dialect)?;
        let result = compat::get_tables(query, dialect);
        serde_json::to_string(&result).map_err(|e| e.to_string())
    })
}

#[no_mangle]
pub extern "C" fn bruin_rustsqlparser_rename_tables(
    query: *const c_char,
    dialect: *const c_char,
    table_mapping_json: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        let query = read_cstr(query)?;
        let dialect = read_dialect(dialect)?;
        let mapping_str = read_cstr(table_mapping_json)?;
        let mapping: HashMap<String, String> =
            serde_json::from_str(mapping_str).map_err(|e| e.to_string())?;
        let result = compat::replace_table_references(query, dialect, &mapping);
        serde_json::to_string(&result).map_err(|e| e.to_string())
    })
}

#[no_mangle]
pub extern "C" fn bruin_rustsqlparser_add_limit(
    query: *const c_char,
    limit: i64,
    dialect: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        let query = read_cstr(query)?;
        let dialect = read_dialect(dialect)?;
        let result = compat::add_limit(query, limit as usize, dialect);
        serde_json::to_string(&result).map_err(|e| e.to_string())
    })
}

#[no_mangle]
pub extern "C" fn bruin_rustsqlparser_is_single_select(
    query: *const c_char,
    dialect: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        let query = read_cstr(query)?;
        let dialect = read_dialect(dialect)?;
        let result = compat::is_single_select_query(query, dialect);
        serde_json::to_string(&result).map_err(|e| e.to_string())
    })
}

#[no_mangle]
pub extern "C" fn bruin_rustsqlparser_column_lineage(
    query: *const c_char,
    dialect: *const c_char,
    schema_json: *const c_char,
) -> *mut c_char {
    ffi_call(|| {
        let query = read_cstr(query)?;
        let dialect = read_dialect(dialect)?;
        let schema_str = read_cstr(schema_json)?;
        let schema: Value = if schema_str.is_empty() {
            Value::Object(Default::default())
        } else {
            serde_json::from_str(schema_str).map_err(|e| e.to_string())?
        };
        let result = compat::get_column_lineage(query, &schema, dialect);
        serde_json::to_string(&result).map_err(|e| e.to_string())
    })
}
