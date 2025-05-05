# Policies

Bruin supports **policies** to verify that data transformation jobs follow best practices and organisation wide conventions. In addition to built-in lint rules, Bruin also allows users to define **custom lint rules** using a `policy.yml` file.

This document explains how to define, configure, and use custom linting policies.

> [!NOTE]
> For the purpose of this document, a `resource` means either an `asset` or a `pipeline`.
## Quick Start

1. Create a `policy.yml` file in your project root.
2. Define custom rules under `custom_rules` (optional if only using built-in rules).
3. Group rules into `rulesets`, specifying which resource they should apply to using selectors.

Example:

```yaml
rulesets:
  - name: ruleset-1
    selector:
      - path: .*/foo/.*
    rules:
      - asset-has-owner
      - asset-name-is-lowercase
      - asset-has-description
```

🚀 That's it! Bruin will now lint your assets according to these policies.

To verify that your assets satisfy your policies, you can run:

```sh
$ bruin validate /path/to/pipelines
```

> [!tip]
> `bruin run` normally runs lint before pipeline execution. So you can rest assured that any non-compliant resources will get stopped in it's tracks.

## Rulesets

A **ruleset** groups one or more rules together and specifies which resources they apply to, based on selectors.

Each ruleset must include:
- **name**: A unique name for the ruleset.
- **selector** (optional): One or more predicates to select the applicable resources.
- **rules**: List of rule names (built-in or custom) to apply.

If a **selector** is not specified, the ruleset applies to **all resources**.

>[!NOTE]
> Names be must alphanumeric or use dashes (`-`). This applies to both `rulesets` and `rules`.


### Selector Predicates

Selectors determine which resources a ruleset should apply to. Supported predicates are:

| Predicate | Target | Description |
| :--- | :--- | :--- |
| `path` | `asset`, `pipeline` | path of the asset/pipeline |
| `pipeline` | `asset`, `pipeline` | name of the pipeline |
| `asset` | `asset` | name of the asset |
| `tag` | `asset` | asset tags |

Each predicate is a regex string.

::: info
If multiple selectors are specified within a ruleset, **all selectors must match** for the ruleset to apply
:::

If no selectors are defined for a ruleset, **the ruleset applies to all resources**. Some selectors only work with certain
rule targets. For instance `tag` selector only works for rules that [target](#targets) assets. Pipeline level rules will just ignore
this selector. 

> [!TIP]
> If your ruleset only contains asset selectors, but uses `pipeline` rules, then those pipeline rules will apply to all pipelines. Make sure to define a `pipeline` or `path` selector if you don't intend for that to happen.

### Example

```yaml
rulesets:
  - name: production
    selector:
      - path: .*/prod/.*
      - tag: critical
    rules:
      - asset-has-owner
      - asset-name-is-lowercase
```

In this example:
- `production` applies **only** to resources that match both:
  - path regex `.*/prod/.*`
  - and have a tag matching `critical`.

## Custom Rules

Custom lint rules are defined inside the `custom_rules` section of `policy.yml`.

Each rule must include:
- **name**: A unique name for the rule. 
- **description**: A human-readable description of the rule.
- **criteria**: An [expr](https://expr-lang.org/) boolean expression. If the expression evalutes to `true` then the resource passes validation.

### Example

```yaml
custom_rules:
  - name: asset-has-owner
    description: every asset should have an owner
    criteria: asset.Owner != ""
```

### Targets

Custom rules can have an optional `target` attribute that defines what resource the rule acts on. Valid values are:
- `asset` (default)
- `pipeline`

**Example**
```yaml
custom_rules:

  - name: pipline-must-have-prefix-acme
    description: Pipeline names must start with the prefix 'acme'
    criteria: pipeline.Name startsWith 'acme'
    target: pipeline

  - name: asset-name-must-be-layer-dot-schema-dot-table
    description: Asset names must be of the form {layer}.{schema}.{table}
    criteria: len(split(asset.Name, '.')) == 3
    target: asset # optional

ruleset:
  - name: std
    rules:
      - pipeline-must-have-prefix-acme
      - asset-name-must-be-layer-dot-schema-dot-table
```

### Variables

`criteria` has the following variables available for use in your expressions:

| Name | Target | 
| ---  | --- | 
| [asset](https://github.com/bruin-data/bruin/blob/f9c7d0083d2f53538102e77126e55f9dfc8840a5/pkg/pipeline/pipeline.go#L622-L645) | `asset` | 
| [pipeline](https://github.com/bruin-data/bruin/blob/f9c7d0083d2f53538102e77126e55f9dfc8840a5/pkg/pipeline/pipeline.go#L1106-L1121) | `asset`, `pipeline` |

::: warning
The variables exposed here are direct Go structs, therefore it is recommended to check the latest version of these given structs. 

In the future we will create dedicated schemas for custom rules with standards around them.
:::

## Built-in Rules

Bruin provides a set of built-in lint rules that are ready to use without requiring a definition.

Here is your markdown table converted to an HTML table:

<table>
  <thead>
    <tr>
      <th width="45%">Rule</th>
      <th>Target</th>
      <th>Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><code>asset-name-is-lowercase</code></td>
      <td><code>asset</code></td>
      <td>Asset names must be in lowercase.</td>
    </tr>
    <tr>
      <td><code>asset-name-is-schema-dot-table</code></td>
      <td><code>asset</code></td>
      <td>Asset names must follow the format <code>schema.table</code>.</td>
    </tr>
    <tr>
      <td><code>asset-has-description</code></td>
      <td><code>asset</code></td>
      <td>Assets must have a description.</td>
    </tr>
    <tr>
      <td><code>asset-has-owner</code></td>
      <td><code>asset</code></td>
      <td>Assets must have an owner assigned.</td>
    </tr>
    <tr>
      <td><code>asset-has-columns</code></td>
      <td><code>asset</code></td>
      <td>Assets must define their columns.</td>
    </tr>
    <tr>
      <td><code>asset-has-primary-key</code></td>
      <td><code>asset</code></td>
      <td>Assets must define at least one column as a primary key.</td>
    </tr>
    <tr>
      <td><code>asset-has-checks</code></td>
      <td><code>asset</code></td>
      <td>Asset must have at least one custom check.</td>
    </tr>
    <tr>
      <td><code>asset-has-tags</code></td>
      <td><code>asset</code></td>
      <td>Asset must have at least one tag.</td>
    </tr>
    <tr>
      <td><code>column-has-description</code></td>
      <td><code>asset</code></td>
      <td>All columns declared by Asset must have description.</td>
    </tr>
    <tr>
      <td><code>column-name-is-snake-case</code></td>
      <td><code>asset</code></td>
      <td>Column names must be in <code>snake_case</code>.</td>
    </tr>
    <tr>
      <td><code>column-name-is-camel-case</code></td>
      <td><code>asset</code></td>
      <td>Column names must be in <code>camelCase</code>.</td>
    </tr>
    <tr>
      <td><code>column-type-is-valid-for-platform</code></td>
      <td><code>asset</code></td>
      <td>Ensure that column types declared by asset are valid types in the relevant platform (BigQuery and Snowflake only).</td>
    </tr>
    <tr>
      <td><code>description-must-not-be-placeholder</code></td>
      <td><code>asset</code></td>
      <td><code>asset</code> and <code>column</code> descriptions must not contain placeholder strings</td>
    </tr>
  </tbody>
</table>


You can directly reference these rules in `rulesets[*].rules`.

## Full Example

```yaml
custom_rules:
  - name: asset-has-owner
    description: every asset should have an owner
    criteria: asset.Owner != ""

rulesets:
  - name: production
    selector:
      - path: .*/production/.*
      - tag: critical
    rules:
      - asset-has-owner
      - asset-name-is-lowercase
      - asset-has-description
  - name: staging
    selector:
      - asset: stage.*
      - pipeline: staging
    rules:
      - asset-name-is-lowercase
```


## Further Reading

- [Expr Language Documentation](https://expr-lang.org/)
