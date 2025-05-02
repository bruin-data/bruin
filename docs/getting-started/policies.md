# Policies

Bruin supports **policies** to verify that data transformation jobs follow best practices and organisation wide conventions. In addition to built-in lint rules, Bruin also allows users to define **custom lint rules** using a `policy.yml` file.

This document explains how to define, configure, and use custom linting policies.

## Quick Start

1. Create a `policy.yml` file in your project root.
2. Define custom rules under `custom_rules` (optional if only using built-in rules).
3. Group rules into `rulesets`, specifying which assets they should apply to using selectors.

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

> 🚀 That's it! Bruin will now lint your assets according to these policies.

To verify that your assets satsify your policies, you can run:
```sh
$ bruin validate /path/to/pipelines
```

> [!tip]
> `bruin run` normally runs lint before pipeline execution. So you can rest assured that any non-compliant assets will get stopped in it's tracks.

## Rulesets

A **ruleset** groups one or more rules together and specifies which assets they apply to, based on selectors.

Each ruleset must include:
- **name**: A unique name for the ruleset.
- **selector** (optional): One or more predicates to select the applicable assets.
- **rules**: List of rule names (built-in or custom) to apply.

If a **selector** is not specified, the ruleset applies to **all assets**.

### Selector Predicates

Selectors determine which assets a ruleset should apply to. Supported predicates are:

| Predicate | Description |
| :--- | :--- |
| `path` | path of the asset |
| `asset` | name of the asset |
| `pipeline` | name of the pipeline |
| `tag` | asset tags |

Each predicate is a regex string.

> If multiple selectors are specified within a ruleset, **all selectors must match** for the ruleset to apply

If no selectors are defined for a ruleset, **the ruleset applies to all assets**.

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
- `production` applies **only** to assets that match both:
  - path regex `.*/production/.*`
  - and have a tag matching `critical`.

> [!note]
> Currently `policies` are only applied to `assets`. Support for `pipeline` level policies will be added in a future version.
## Custom Rules

Custom lint rules are defined inside the `custom_rules` section of `policy.yml`.

Each rule must include:
- **name**: A unique name for the rule.
- **description**: A human-readable description of the rule.
- **criteria**: An [expr](https://expr-lang.org/) boolean expression. If the expression evalutes to `true` then the asset passes validation.

### Example

```yaml
custom_rules:
  - name: asset-has-owner
    description: every asset should have an owner
    criteria: asset.Owner != ""
```

### Variables

`criteria` has the following variables available for use in your expressions:
| Name | Description |
| ---  | --- |
| [asset](https://github.com/bruin-data/bruin-vscode/blob/1726eda362f29bf95f5ffc6b50addf8b63f2128b/schemas/yaml-assets-schema.json) | The asset selected via selector |
| [pipeline](https://github.com/bruin-data/bruin-vscode/blob/1726eda362f29bf95f5ffc6b50addf8b63f2128b/schemas/pipeline-schema.json) | The pipeline the asset belongs to |

> [!tip]
> The variables above link to their [JSON Schema](https://json-schema.org/). You can use those as a reference for what fields are available on each variable and
> what their types are.


## Built-in Rules

Bruin provides a set of built-in lint rules that are ready to use without requiring a definition.

| Rule | Description |
| :--- | :--- |
| `asset-name-is-lowercase` | Asset names must be in lowercase. |
| `asset-name-is-schema-dot-table` | Asset names must follow the format `schema.table`. |
| `asset-has-description` | Assets must have a description. |
| `asset-has-owner` | Assets must have an owner assigned. |
| `asset-has-columns` | Assets must define their columns. |

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
