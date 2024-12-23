# `format` Command

The `format` command is used to process and format asset definition files in a project. It can handle a single asset file or process all asset files in a given path. 
The command supports two output types: plain text and JSON.

## Usage

```bash
bruin format [path-to-asset-or-project-root] [flags]
```

### Arguments

**path-to-asset-or-project-root** (optional):
- If the argument is a path to an asset definition file, the command processes and formats that single asset.
- If the argument is a path to a project root, it finds and formats all asset files within that path.
- Defaults to the current directory (`.`) if no argument is provided.

### Flags

**--output / -o** (optional):  
Specifies the output format for the command.  
Possible values:
- `plain` (default): Prints human-readable messages.
- `json`: Prints errors (if any) in JSON format.  
- `fail-if-changed`: fail the command if any of the assets need reformatting