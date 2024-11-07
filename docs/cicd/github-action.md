# CICD integration

## Overview
This document outlines the steps to integrate Bruin into GitHub Actions.

## GitHub Action: bruin-data/setup-bruin@main

The [`bruin-data/setup-bruin@main`](https://github.com/marketplace/actions/bruin-setup) action is used to set up the Bruin environment in your GitHub Actions workflow. This action ensures that all necessary dependencies and configurations are in place for your Bruin tasks to run smoothly.

### Usage
```yaml
- uses: bruin-data/setup-bruin@main
```

### Parameters
- **version** (optional): Specify the version of Bruin to install. If not provided, the latest version will be used.

### Example
```yaml
- uses: bruin-data/setup-bruin@main
  with:
    version: 'v0.11.62'
```

## Setting Up GitHub Actions
1. Create a `.github/workflows` directory in your repository.
2. Add a YAML file (e.g., `ci.yml`) to define your workflow.
3. 

```yaml
name: Bruin Test

on:
  push:
    branches: [ main ]
jobs:
  linux:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v2
    - uses: bruin-data/setup-bruin@main
    - run: bruin validate ./bruin-pipeline/
      name: Validate Pipeline
    - run: bruin format ./bruin-pipeline/
      name: Format Pipeline
    - name: Lineage
      run: |
        bruin lineage bruin-pipeline/assets/example.sql
        bruin lineage bruin-pipeline/assets/pythonsample/country_list.py
```

