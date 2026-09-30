package ssis

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"strings"

	"github.com/spf13/afero"
	"golang.org/x/net/html/charset"
)

// Local XML names allow the standard DTS, SQLTask and ExecuteProcess prefixes
// to vary without removing namespaces from the source text.
type node struct {
	XMLName  xml.Name
	Attrs    []xml.Attr `xml:",any,attr"`
	Children []node     `xml:",any"`
	Text     string     `xml:",chardata"`
}

func (n node) value(key string) string {
	for _, a := range n.Attrs {
		if a.Name.Local == key {
			return a.Value
		}
	}
	for _, child := range n.Children {
		if child.XMLName.Local == "Property" && child.value("Name") == key {
			return child.Text
		}
	}
	return ""
}

func (n node) children(name string) []node {
	var result []node
	for _, child := range n.Children {
		if child.XMLName.Local == name {
			result = append(result, child)
		}
	}
	return result
}

func (n node) descendants(name string) []node {
	result := n.children(name)
	for _, child := range n.Children {
		result = append(result, child.descendants(name)...)
	}
	return result
}

func parsePackage(fs afero.Fs, source string, data []byte) (workflow, error) {
	var root node
	decoder := xml.NewDecoder(bytes.NewReader(data))
	decoder.CharsetReader = charset.NewReaderLabel
	if err := decoder.Decode(&root); err != nil {
		return workflow{}, err
	}
	w := workflow{name: root.value("ObjectName")}
	if root.XMLName.Local != "Executable" || w.name == "" {
		return w, fmt.Errorf("expected an unencrypted SSIS package with ObjectName")
	}
	for _, name := range []string{"PropertyExpression", "EventHandler", "Variable", "PackageParameter"} {
		if len(root.descendants(name)) > 0 {
			return w, fmt.Errorf("%s requires manual migration", name)
		}
	}
	for _, n := range append([]node{root}, root.descendants("Executable")...) {
		if v := n.value("Disabled"); v != "" && v != "0" && !strings.EqualFold(v, "false") {
			return w, fmt.Errorf("disabled executables require manual migration")
		}
		if v := n.value("TransactionOption"); v == "2" || strings.EqualFold(v, "Required") {
			return w, fmt.Errorf("required SSIS transactions cannot be preserved")
		}
	}
	containers := root.children("Executables")
	if len(containers) != 1 {
		return w, fmt.Errorf("expected one Executables collection (modern .dtsx format)")
	}
	for _, executable := range containers[0].children("Executable") {
		t := task{id: executable.value("refId"), name: executable.value("ObjectName")}
		if len(executable.descendants("Executable")) > 0 {
			return w, fmt.Errorf("task %q: nested containers and loops require manual migration", t.name)
		}
		var err error
		switch executable.value("ExecutableType") {
		case "Microsoft.ExecuteSQLTask", "STOCK:SQLTask":
			sql := executable.descendants("SqlTaskData")
			if len(sql) != 1 {
				return w, fmt.Errorf("task %q: missing SqlTaskData", t.name)
			}
			mode := sql[0].value("SqlStatementSourceType")
			if mode != "" && mode != "DirectInput" && mode != "0" {
				return w, fmt.Errorf("task %q: only direct-input SQL is supported", t.name)
			}
			if len(sql[0].Children) > 0 || (sql[0].value("ResultType") != "" && sql[0].value("ResultType") != "ResultSetType_None") {
				return w, fmt.Errorf("task %q: SQL parameter/result bindings require manual migration", t.name)
			}
			t.code = sql[0].value("SqlStatementSource")
		case "Microsoft.ExecuteProcess", "STOCK:ExecuteProcessTask":
			process := executable.descendants("ExecuteProcessData")
			if len(process) != 1 {
				return w, fmt.Errorf("task %q: missing ExecuteProcessData", t.name)
			}
			for _, key := range []string{"WorkingDirectory", "StandardInputVariable", "StandardOutputVariable", "StandardErrorVariable"} {
				if process[0].value(key) != "" {
					return w, fmt.Errorf("task %q: process %s requires manual migration", t.name, key)
				}
			}
			t.python = true
			t.code, err = pythonScript(fs, source, process[0].value("Executable"), process[0].value("Arguments"))
		default:
			return w, fmt.Errorf("task %q: unsupported executable type %q", t.name, executable.value("ExecutableType"))
		}
		if err != nil {
			return w, fmt.Errorf("task %q: %w", t.name, err)
		}
		w.tasks = append(w.tasks, t)
	}
	for _, constraint := range root.descendants("PrecedenceConstraint") {
		if v := constraint.value("Value"); v != "" && v != "0" && v != "Success" {
			return w, fmt.Errorf("only success precedence constraints are supported")
		}
		if v := constraint.value("EvalOp"); (v != "" && v != "0" && v != "Constraint") || constraint.value("Expression") != "" {
			return w, fmt.Errorf("expression precedence constraints require manual migration")
		}
		if v := constraint.value("LogicalAnd"); v == "0" || strings.EqualFold(v, "false") {
			return w, fmt.Errorf("OR precedence constraints require manual migration")
		}
		found := false
		for i := range w.tasks {
			if w.tasks[i].id == constraint.value("To") {
				w.tasks[i].upstream = append(w.tasks[i].upstream, constraint.value("From"))
				found = true
			}
		}
		if !found {
			return w, fmt.Errorf("unresolved precedence destination %q", constraint.value("To"))
		}
	}
	return w, nil
}
