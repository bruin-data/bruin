# `lineage` Command

The `lineage` command helps you understand how a specific asset fits into your pipeline by showing its dependencies. It tells you:

- What the asset relies on (upstream dependencies).
- What relies on the asset (downstream dependencies).

This is useful for understanding your pipeline’s structure, tracking relationships between components, and troubleshooting issues with asset connections.

```bash
 bruin lineage [flags] <path to the asset definition>
```
### Flags

- `--full`  
  Display all upstream and downstream dependencies, including indirect dependencies.

- `--output`, `-o`  
  Specify the output format. Possible values:
    - `plain` (default): Outputs a human-readable text summary.
    - `json`: Outputs the lineage as structured JSON.

  
## Example

### Understanding the dependencies of Chess template 

``` bash
bruin lineage chess/assets/player_summary.sql 
```
#### Output:


<img alt="Bruin - clean" src="/lineage2.gif" style="margin: 10px;" />

