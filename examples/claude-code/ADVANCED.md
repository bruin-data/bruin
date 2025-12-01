# Advanced Claude Code Asset Configuration

This guide covers all advanced configuration options available for the `agent.claude_code` asset type.

## Complete Parameter Reference

### Required Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `prompt` | string | The prompt to send to Claude (supports Jinja templates) |

### Model Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model` | string | - | Claude model to use: `opus`, `sonnet`, `haiku`, or full model names |
| `fallback_model` | string | - | Backup model if primary is overloaded |

### Output Configuration

| Parameter | Type | Default | Options | Description |
|-----------|------|---------|---------|-------------|
| `output_format` | string | text | `text`, `json`, `stream-json` | Format of Claude's response |
| `system_prompt` | string | - | - | Additional context to append to system prompt |

### Security & Permissions

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `allowed_directories` | string | - | Comma-separated list of directories Claude can access |
| `allowed_tools` | string | - | Comma-separated list of allowed tools (e.g., "Read,Grep") |
| `disallowed_tools` | string | - | Comma-separated list of disallowed tools |
| `skip_permissions` | boolean | false | Bypass permission checks (use with caution) |
| `permission_mode` | string | default | Options: `default`, `plan`, `acceptEdits`, `bypassPermissions` |

### Session Management

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `session_id` | string | - | UUID to maintain conversation context |
| `continue_session` | boolean | false | Continue the most recent conversation |

### Debugging

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `debug` | boolean | false | Enable debug mode |
| `verbose` | boolean | false | Enable verbose output |

## Usage Examples

### 1. JSON Output for Structured Data

```yaml
name: sales_analyzer
type: agent.claude_code
parameters:
  prompt: "Analyze sales data and return metrics"
  model: sonnet
  output_format: json
  system_prompt: "Return a JSON object with total_sales, growth_rate, and top_products fields"
```

The JSON output can be parsed by downstream tasks:
```json
{
  "content": "{\"total_sales\": 150000, \"growth_rate\": 12.5, \"top_products\": [\"Widget A\", \"Gadget B\"]}",
  "model": "claude-3-5-sonnet-20241022"
}
```

### 2. File Analysis with Restricted Permissions

```yaml
name: code_reviewer
type: agent.claude_code
parameters:
  prompt: "Review Python files for security issues"
  model: opus
  allowed_directories: "./src,./lib"
  allowed_tools: "Read,Grep,Glob"
  disallowed_tools: "Edit,Write,Bash"
  permission_mode: plan
```

This configuration:
- Limits Claude to specific directories
- Only allows reading and searching files
- Prevents any file modifications
- Uses "plan" mode to only suggest changes

### 3. Multi-Step Conversation with Session

First asset:
```yaml
name: analysis_step1
type: agent.claude_code
parameters:
  prompt: "Analyze customer churn data and identify patterns"
  model: sonnet
  session_id: "550e8400-e29b-41d4-a716-446655440000"
```

Second asset (continues conversation):
```yaml
name: analysis_step2
type: agent.claude_code
parameters:
  prompt: "Based on the patterns identified, suggest retention strategies"
  model: sonnet
  session_id: "550e8400-e29b-41d4-a716-446655440000"
  continue_session: "true"
```

### 4. Model Fallback for Reliability

```yaml
name: critical_analysis
type: agent.claude_code
parameters:
  prompt: "Generate critical business insights"
  model: opus
  fallback_model: sonnet
  output_format: json
```

If Opus is overloaded, automatically falls back to Sonnet.

### 5. CI/CD Pipeline Integration

```yaml
name: ci_validator
type: agent.claude_code
parameters:
  prompt: "Validate deployment configuration"
  model: haiku  # Faster, cheaper model for CI
  skip_permissions: "true"
  permission_mode: bypassPermissions
  debug: "true"
```

Optimized for automated pipelines with:
- Faster Haiku model
- Bypassed permissions for trusted environment
- Debug output for troubleshooting

## Security Best Practices

### Production Recommendations

1. **Never use `skip_permissions: true` in production** unless in isolated environments
2. **Always specify `allowed_directories`** when Claude needs file access
3. **Use `disallowed_tools`** to explicitly block dangerous operations
4. **Set `permission_mode: plan`** for analysis-only tasks

### Safe Configuration Example

```yaml
name: production_analyzer
type: agent.claude_code
parameters:
  prompt: "Analyze production metrics"
  model: sonnet
  fallback_model: haiku
  output_format: json
  allowed_directories: "./data/readonly"
  allowed_tools: "Read,Grep"
  disallowed_tools: "Edit,Write,Bash,TodoWrite"
  permission_mode: plan
```

## Performance Considerations

### Model Selection Guide

| Model | Speed | Cost | Quality | Use Cases |
|-------|-------|------|---------|-----------|
| Haiku | Fast | Low | Good | Quick checks, validation, simple analysis |
| Sonnet | Medium | Medium | Excellent | General purpose, complex analysis |
| Opus | Slow | High | Best | Critical analysis, complex reasoning |

### Optimization Tips

1. **Use Haiku for high-volume, simple tasks**
   ```yaml
   model: haiku
   ```

2. **Enable caching with session IDs for related tasks**
   ```yaml
   session_id: "consistent-uuid-here"
   ```

3. **Use JSON output for downstream processing**
   ```yaml
   output_format: json
   ```

4. **Limit file access scope**
   ```yaml
   allowed_directories: "./specific/path"
   ```

## Troubleshooting

### Enable Debug Mode

```yaml
parameters:
  prompt: "Debug my issue"
  debug: "true"
  verbose: "true"
```

### Common Issues

1. **"Invalid model" error**
   - Check model name spelling
   - Use one of: opus, sonnet, haiku, or full model names

2. **"Permission denied" errors**
   - Add required directories to `allowed_directories`
   - Check `permission_mode` setting

3. **JSON parsing errors**
   - Ensure `system_prompt` requests valid JSON
   - Check Claude's response in debug mode

4. **Session not continuing**
   - Verify `session_id` is consistent
   - Set `continue_session: "true"` for subsequent calls

## Integration with Bruin Pipelines

### Chaining Assets

Use Claude's output as input for SQL queries:

```yaml
# Step 1: Analyze with Claude
name: analyze_data
type: agent.claude_code
parameters:
  prompt: "Identify top performing segments"
  output_format: json

# Step 2: Use analysis in SQL
name: create_report
type: bq.sql
depends:
  - analyze_data
```

### Conditional Execution

Run Claude analysis only when needed:

```yaml
name: anomaly_detector
type: agent.claude_code
parameters:
  prompt: |
    {% if has_anomalies %}
    Investigate anomalies in {{start_date}} data
    {% else %}
    Generate standard daily report
    {% endif %}
```

## Migration Guide

### From Simple to Advanced Configuration

Before (simple):
```yaml
parameters:
  prompt: "Analyze data"
```

After (advanced):
```yaml
parameters:
  prompt: "Analyze data"
  model: sonnet
  fallback_model: haiku
  output_format: json
  system_prompt: "Be concise and structured"
  allowed_directories: "./data"
  permission_mode: plan
```

### Backward Compatibility

The simple configuration still works:
```yaml
parameters:
  prompt: "Your prompt here"
```

All other parameters are optional and have sensible defaults.