# GitHub Actions

The [`bruin-data/setup-bruin@main`](https://github.com/marketplace/actions/bruin-setup) action is used to set up the Bruin environment in your GitHub Actions workflow. This action ensures that all necessary dependencies and configurations are in place for your Bruin tasks to run smoothly.

```yaml
- uses: bruin-data/setup-bruin@main
```

### Parameters
- **version** (optional): Specify the version of Bruin to install. If not provided, the latest version will be used.


## Setting Up GitHub Actions
1. Create a `.github/workflows` directory in your repository.
2. Inside that directory, create a YAML file (e.g., `ci.yml`) to define your workflow.

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
    
    # install Bruin CLI in your pipeline
    - uses: bruin-data/setup-bruin@main
    
    # validate your pipeline
    - run: bruin validate ./bruin-pipeline/
      name: Validate Pipeline
```

