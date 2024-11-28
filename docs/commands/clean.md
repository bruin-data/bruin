# `clean`  Command

The `clean` command is used to remove temporary artifacts related to Bruin.

It currently:
- cleans the cached virtualenv directories inside `~/.bruin` directory
- removes the `logs` directory from the project root

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


<img alt="Bruin - clean" src="/clean.gif" style="margin: 10px;" />
