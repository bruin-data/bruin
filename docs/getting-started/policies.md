# Policies

Bruin supports **linting** to verify that data transformation jobs follow best practices and conventions. In addition to built-in lint rules, Bruin also allows users to define **custom lint rules** using a `policy.yml` file.

This document explains how to define, configure, and use custom linting policies.

## Quick Start

1. Create a `policy.yml` file.
2. Define custom rules under `define` (optional if only using built-in rules).
3. Group rules into `rulesets`, specifying which jobs they should apply to using selectors.

Example:

```yaml
define:
  - name: asset_has_owner
    description: every asset should have an owner
    criteria: asset.Owner != ""

rulesets:
  - name: ruleset_1
    selector:
      - path: .*/foo/.*
    rules:
      - asset_has_owner
      - asset_name_is_lowercase
      - asset_has_description
```

> 🚀 That's it! Bruin will now lint your jobs according to these policies.

## Defining Rulesets

A **ruleset** groups one or more rules together and specifies which jobs they apply to, based on selectors.

Each ruleset must include:
- **name**: A unique name for the ruleset.
- **selector** (optional): One or more predicates to select the applicable jobs.
- **rules**: List of rule names (built-in or custom) to apply.

If a **selector** is not specified, the ruleset applies to **all jobs**.

### Selector Predicates

Selectors determine which jobs a ruleset should apply to. Supported predicates are:

| Predicate | Description |
| :--- | :--- |
| `path` | Regex match against the assets's path. |
| `asset` | Regex match against the asset's name. |
| `pipeline` | Regex match against the pipeline name. |
| `tag` | Regex match against asset tags. |

Each predicate is a regex string.

> If multiple selectors are specified within a ruleset, **all selectors must match** for the ruleset to apply to a job.

If no selectors are defined for a ruleset, **the ruleset applies to all jobs**.

### Example

```yaml
rulesets:
  - name: production
    selector:
      - path: .*/prod/.*
      - tag: critical
    rules:
      - asset_has_owner
      - asset_name_is_lowercase
```

In this example:
- `production` applies **only** to jobs that match both:
  - path regex `.*/production/.*`
  - and have a tag matching `critical`.

## Defining Custom Rules

Custom lint rules are defined inside the `define` section of `policy.yml`.

Each rule must include:
- **name**: A unique name for the rule.
- **description**: A human-readable description of the rule.
- **criteria**: An [Expr](https://expr-lang.org/) expression that evaluates to a boolean.  
  If the result is `false`, the rule is considered violated.

### Example

```yaml
define:
  - name: asset_has_owner
    description: every asset should have an owner
    criteria: asset.Owner != ""
```


## Built-in Rules

Bruin provides a set of built-in lint rules that are ready to use without requiring a definition.

| Rule | Description |
| :--- | :--- |
| `asset_name_is_lowercase` | Asset names must be in lowercase. |
| `asset_name_is_schema_dot_table` | Asset names must follow the format `schema.table`. |
| `asset_has_description` | Assets must have a description. |
| `asset_has_owner` | Assets must have an owner assigned. |
| `asset_has_columns` | Assets must define their columns. |

You can directly reference these rules in `rulesets[*].rules`.

## Full Example

```yaml
define:
  - name: asset_has_owner
    description: every asset should have an owner
    criteria: asset.Owner != ""

rulesets:
  - name: production
    selector:
      - path: .*/production/.*
      - tag: critical
    rules:
      - asset_has_owner
      - asset_name_is_lowercase
      - asset_has_description
  - name: staging
    selector:
      - asset: stage.*
      - pipeline: staging
    rules:
      - asset_name_is_lowercase
```


## Further Reading

- [Expr Language Documentation](https://expr-lang.org/)
