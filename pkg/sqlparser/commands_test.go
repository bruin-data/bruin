package sqlparser

import (
	"encoding/json"
	"errors"
)

// The parser's commands addressed by name with JSON-shaped arguments, the form the golden files in
// testdata/golden record them in. Only the tests use this; the public methods call the command
// handlers directly.

type parserCommand struct {
	Command  string                 `json:"command"`
	Contents map[string]interface{} `json:"contents"`
}

// dispatch runs a command and returns its JSON response.
func dispatch(pc *parserCommand) string {
	return string(encodeResponse(runCommand(pc.Command, pc.Contents)))
}

// runCommand runs one command and returns the response object: the command's result, or
// {"error": ...} when it fails.
func runCommand(command string, c map[string]any) any {
	return runGuarded(func() (any, error) { return runCommandUnguarded(command, c) })
}

func runCommandUnguarded(command string, c map[string]any) (result any, err error) {
	switch command {
	case "init":
		result = map[string]any{}
	case "lineage":
		result, err = cmdLineage(c)
	case "get-tables":
		result = getTables(str(c["query"]), str(c["dialect"]))
	case "replace-table-references":
		result, err = replaceTableReferences(str(c["query"]), str(c["dialect"]), c["table_mapping"])
	case "add-limit":
		result, err = addLimit(str(c["query"]), toInt(c["limit"]), str(c["dialect"]))
	case "is-read-only":
		result = isReadOnlyQuery(str(c["query"]), str(c["dialect"]))
	case "is-single-select":
		result = isSingleSelectQuery(str(c["query"]), str(c["dialect"]))
	case "add-ctes":
		result = addCTEs(str(c["query"]), str(c["dialect"]), c["ctes"])
	case "extract-select":
		result = extractSelect(str(c["query"]), str(c["dialect"]))
	case "select-cte":
		result = selectCTE(str(c["query"]), str(c["dialect"]), str(c["cte_name"]))
	case "freeze-time":
		result = freezeTime(str(c["query"]), str(c["dialect"]), str(c["execution_time"]))
	case "hoist-declares":
		result = hoistDeclares(str(c["query"]), str(c["dialect"]))
	case "hoist-declares-list":
		result = hoistDeclaresList(toStrings(c["queries"]), str(c["dialect"]))
	default:
		err = errors.New("invalid cmd")
	}
	return result, err
}

// encodeResponse serializes a response object like the Python command loop's json.dumps.
func encodeResponse(resp any) []byte {
	b, err := json.Marshal(resp)
	if err != nil {
		b, _ = json.Marshal(map[string]string{"error": err.Error()}) //nolint:errchkjson // a map of strings always encodes
	}
	return b
}

// cmdLineage mirrors get_column_lineage(query, schema, dialect) including the error a
// missing (null) schema produces in Python.
func cmdLineage(c map[string]any) (any, error) {
	query := str(c["query"])
	dialect := str(c["dialect"])
	rawSchema := c["schema"]
	schema, err := toRawSchema(rawSchema)
	if err != nil {
		return nil, err
	}
	return getColumnLineage(query, schema, dialect)
}

func toInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case float64:
		return int(x)
	case json.Number:
		i, _ := x.Int64()
		return int(i)
	}
	return 0
}

func toStrings(v any) []string {
	switch x := v.(type) {
	case []string:
		return x
	case []any:
		out := make([]string, 0, len(x))
		for _, s := range x {
			out = append(out, str(s))
		}
		return out
	}
	return nil
}
