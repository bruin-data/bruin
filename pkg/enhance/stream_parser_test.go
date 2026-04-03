package enhance

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseClaudeStreamJSON(t *testing.T) {
	t.Parallel()

	input := strings.Join([]string{
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t1","name":"Read","input":{"file_path":"assets/seed.yml"}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t2","name":"Glob","input":{"pattern":"**/*.yml"}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t3","name":"Grep","input":{"pattern":"materialization","path":"assets/"}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t4","name":"Edit","input":{"file_path":"assets/seed.yml","old_string":"foo","new_string":"bar"}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t5","name":"Bash","input":{"command":"cat pipeline.yml | head -20"}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t6","name":"WebFetch","input":{"url":"https://example.com"}}]}}`,
		`{"type":"assistant","message":{"content":[{"type":"tool_use","id":"t7","name":"SomeUnknownTool","input":{"foo":"bar"}}]}}`,
		`{"type":"result","subtype":"success"}`,
	}, "\n")

	var buf bytes.Buffer
	parseClaudeStreamJSON(strings.NewReader(input), &buf)
	output := buf.String()

	assert.Contains(t, output, "Read: assets/seed.yml")
	assert.Contains(t, output, "Glob: **/*.yml")
	assert.Contains(t, output, `Grep: "materialization" in assets/`)
	assert.Contains(t, output, "Edit: assets/seed.yml")
	assert.Contains(t, output, "Bash: cat pipeline.yml | head -20")
	assert.Contains(t, output, "WebFetch: https://example.com")
	assert.Contains(t, output, "SomeUnknownTool\n") // no detail, just the name
	assert.Contains(t, output, "enhancement complete")
}

func TestParseCodexStreamJSON(t *testing.T) {
	t.Parallel()

	input := strings.Join([]string{
		`{"type":"thread.started","thread_id":"abc123"}`,
		`{"type":"turn.started"}`,
		`{"type":"item.started","item":{"id":"i1","type":"command_execution","command":"/bin/zsh -lc ls","status":"in_progress"}}`,
		`{"type":"item.completed","item":{"id":"i1","type":"command_execution","command":"/bin/zsh -lc ls","aggregated_output":"file1\nfile2\n","exit_code":0,"status":"completed"}}`,
		`{"type":"item.completed","item":{"id":"i2","type":"agent_message","text":"done"}}`,
		`{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":50}}`,
	}, "\n")

	var buf bytes.Buffer
	parseCodexStreamJSON(strings.NewReader(input), &buf)
	output := buf.String()

	assert.Contains(t, output, "exec: /bin/zsh -lc ls")
	assert.Contains(t, output, "command finished (exit 0)")
	assert.Contains(t, output, "agent: done")
	assert.Contains(t, output, "enhancement complete")
}

func TestParseOpenCodeStreamJSON(t *testing.T) {
	t.Parallel()

	input := strings.Join([]string{
		`{"type":"step_start","part":{"type":"step-start"}}`,
		`{"type":"tool_use","part":{"tool":"bash","state":{"status":"completed","input":{"command":"ls","description":"List files"}}}}`,
		`{"type":"tool_use","part":{"tool":"read","state":{"status":"completed","input":{"path":"foo.yml"}}}}`,
		`{"type":"step_finish","part":{"reason":"tool-calls"}}`,
		`{"type":"step_start","part":{"type":"step-start"}}`,
		`{"type":"text","part":{"text":"done"}}`,
		`{"type":"step_finish","part":{"reason":"stop"}}`,
	}, "\n")

	var buf bytes.Buffer
	parseOpenCodeStreamJSON(strings.NewReader(input), &buf)
	output := buf.String()

	assert.Contains(t, output, "tool: bash — List files")
	assert.Contains(t, output, "tool: read\n") // no description for read
	assert.Contains(t, output, "enhancement complete")
	// step_finish with reason "tool-calls" should NOT emit "enhancement complete"
	assert.Equal(t, 1, strings.Count(output, "enhancement complete"))
}

func TestClaudeToolDetail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tool     string
		input    map[string]interface{}
		expected string
	}{
		{"Read with file_path", "Read", map[string]interface{}{"file_path": "foo.yml"}, "foo.yml"},
		{"Read with path", "Read", map[string]interface{}{"path": "bar.yml"}, "bar.yml"},
		{"Read empty", "Read", map[string]interface{}{}, ""},
		{"Glob", "Glob", map[string]interface{}{"pattern": "**/*.sql"}, "**/*.sql"},
		{"Grep with path", "Grep", map[string]interface{}{"pattern": "hello", "path": "src/"}, `"hello" in src/`},
		{"Grep no path", "Grep", map[string]interface{}{"pattern": "hello"}, `"hello"`},
		{"Bash truncated", "Bash", map[string]interface{}{"command": strings.Repeat("x", 100)}, strings.Repeat("x", 77) + "..."},
		{"Unknown tool", "FooTool", map[string]interface{}{"x": "y"}, ""},
		{"nil input", "Read", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var raw interface{}
			if tt.input != nil {
				raw = tt.input
			}
			result := claudeToolDetail(tt.tool, raw)
			assert.Equal(t, tt.expected, result)
		})
	}
}
