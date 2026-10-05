# `docs` Command

The `docs` command generates a documentation website for your Bruin pipelines as a single, self-contained HTML file. The site lists every pipeline and asset with its description, owner, tags, schedule, columns, checks, materialization, lineage, and source code, and includes search.

The file has no external dependencies, so you can open it locally, attach it to a ticket, or host it as a static page.

## Usage

```bash
bruin docs [flags] [path to a repo, pipeline, or asset]
```

### Arguments

**path** (optional):

- A directory: every pipeline under it is included.
- An asset file: the pipeline that contains it is included.
- If omitted, Bruin uses the root of the current Git repository, or the current directory outside a repository.

### Flags

| Flag | Alias | Default | Description |
|------|-------|---------|-------------|
| `--output` | `-o` | `bruin-docs.html` | Path to write the generated HTML file. |
| `--title` | | `Bruin Docs` | Title shown in the generated site. |
| `--variant` | | | For [variant pipelines](/pipelines/variants), only materialize the given variant. |
| `--exclude-code` | | `false` | Leave asset source code out of the generated site. Custom check queries are still included. |
| `--open` | | `false` | Open the generated file in your default browser. |

## Examples

Generate docs for the whole repository:

```bash
bruin docs
```

```text
Generated documentation for 3 pipelines and 42 assets: /path/to/repo/bruin-docs.html
```

Generate docs for one pipeline with a custom title, and open them:

```bash
bruin docs pipelines/marketing --title "Marketing Pipelines" --open
```

Share docs without the assets' SQL or Python source:

```bash
bruin docs --exclude-code -o public/data-docs.html
```

## Related

- [`lineage`](/commands/lineage): print an asset's upstream and downstream dependencies in the terminal.
