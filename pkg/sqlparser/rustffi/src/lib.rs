mod compat;

use serde::Deserialize;
use serde_json::{json, Value};
use std::ffi::{c_char, CStr, CString};

#[derive(Debug, Deserialize)]
struct CommandRequest {
    command: String,
    #[serde(default)]
    contents: Value,
}

#[no_mangle]
pub extern "C" fn bruin_rustsqlparser_execute(command_json: *const c_char) -> *mut c_char {
    match std::panic::catch_unwind(|| execute_inner(command_json)) {
        Ok(Ok(result)) => into_c_string(result),
        Ok(Err(err)) => into_c_string(json!({ "error": err }).to_string()),
        Err(_) => into_c_string(json!({ "error": "panic in rust sql parser" }).to_string()),
    }
}

#[no_mangle]
pub extern "C" fn bruin_rustsqlparser_free_string(ptr: *mut c_char) {
    if ptr.is_null() {
        return;
    }

    // SAFETY: pointer must come from CString::into_raw in this library.
    unsafe {
        let _ = CString::from_raw(ptr);
    }
}

fn execute_inner(command_json: *const c_char) -> Result<String, String> {
    if command_json.is_null() {
        return Err("null command pointer".to_string());
    }

    // SAFETY: caller guarantees a valid NUL-terminated string.
    let command_json = unsafe { CStr::from_ptr(command_json) }
        .to_str()
        .map_err(|err| err.to_string())?;
    let request: CommandRequest =
        serde_json::from_str(command_json).map_err(|err| err.to_string())?;

    let result = match request.command.as_str() {
        "init" => json!({}),
        "lineage" => compat::lineage_command(&request.contents)?,
        "get-tables" => compat::get_tables_command(&request.contents)?,
        "replace-table-references" => compat::replace_table_references_command(&request.contents)?,
        "add-limit" => compat::add_limit_command(&request.contents)?,
        "is-single-select" => compat::is_single_select_command(&request.contents)?,
        "exit" => json!({}),
        _ => return Err(format!("invalid cmd: {}", request.command)),
    };

    serde_json::to_string(&result).map_err(|err| err.to_string())
}

fn into_c_string(value: String) -> *mut c_char {
    match CString::new(value) {
        Ok(value) => value.into_raw(),
        Err(_) => CString::new("{\"error\":\"failed to encode response\"}")
            .expect("static JSON is valid")
            .into_raw(),
    }
}
