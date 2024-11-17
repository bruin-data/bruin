# `clean`  Command

The `clean` command is used to remove temporary artifacts from your project. 
It helps maintain a clean development environment by deleting unnecessary `.log` files.

## Usage

```bash
bruin clean [path-to-project-root]
```
**path-to-project-root** (optional): Specifies the path to the root of the project. If omitted, the current directory (`.`) is used as the default.

## Example

### Cleaning the current directory

``` bash
bruin clean 
```
#### Output:
``` plaintext
Found 107 log files, cleaning them up...
Successfully removed 107 log files.
```
