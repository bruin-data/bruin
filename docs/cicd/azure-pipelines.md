# Azure Pipelines

Azure Pipelines is part of Azure DevOps and uses YAML files to define CI/CD workflows. This guide shows how to set up Bruin in your Azure Pipeline to validate your Bruin pipelines.

## Installation

Bruin can be installed in Azure Pipelines using the installation script. The CLI will be automatically available in subsequent steps.

```yaml
- bash: |
    curl -LsSf https://getbruin.com/install/cli | sh
    echo "##vso[task.prependpath]$HOME/.local/bin"
  displayName: 'Install Bruin'
```

## Setting Up Azure Pipelines

Create an `azure-pipelines.yml` file in the root of your repository:

```yaml
trigger:
  - main

pool:
  vmImage: 'ubuntu-latest'

steps:
- checkout: self

- bash: |
    curl -LsSf https://getbruin.com/install/cli | sh
    echo "##vso[task.prependpath]$HOME/.local/bin"
  displayName: 'Install Bruin'

- bash: |
    bruin validate
  displayName: 'Validate Pipelines'
```

## Configuration Options

### Specify Bruin Version

To install a specific version of Bruin, pass the version tag to the install script:

```yaml
trigger:
  - main

pool:
  vmImage: 'ubuntu-latest'

steps:
- checkout: self

- bash: |
    curl -LsSf https://getbruin.com/install/cli | sh -s v0.1.0
    echo "##vso[task.prependpath]$HOME/.local/bin"
  displayName: 'Install Bruin'

- bash: |
    bruin validate
  displayName: 'Validate Pipelines'
```

### Run on Pull Requests Only

To run validation only on pull requests:

```yaml
trigger: none

pr:
  - main

pool:
  vmImage: 'ubuntu-latest'

steps:
- checkout: self

- bash: |
    curl -LsSf https://getbruin.com/install/cli | sh
    echo "##vso[task.prependpath]$HOME/.local/bin"
  displayName: 'Install Bruin'

- bash: |
    bruin validate
  displayName: 'Validate Pipelines'
```

### Multi-Stage Pipeline

For more complex workflows with multiple stages:

```yaml
trigger:
  - main

pool:
  vmImage: 'ubuntu-latest'

stages:
- stage: Validate
  displayName: 'Validate Stage'
  jobs:
  - job: ValidateBruin
    displayName: 'Validate Bruin Pipelines'
    steps:
    - checkout: self

    - bash: |
        curl -LsSf https://getbruin.com/install/cli | sh
        echo "##vso[task.prependpath]$HOME/.local/bin"
      displayName: 'Install Bruin'

    - bash: |
        bruin validate
      displayName: 'Validate Pipelines'
```

### Windows Agent

If using a Windows agent:

```yaml
trigger:
  - main

pool:
  vmImage: 'windows-latest'

steps:
- checkout: self

- pwsh: |
    Invoke-WebRequest -Uri "https://getbruin.com/install/cli" -OutFile "install.sh"
    bash install.sh
    Write-Host "##vso[task.prependpath]$env:USERPROFILE\.local\bin"
  displayName: 'Install Bruin'

- pwsh: |
    bruin validate
  displayName: 'Validate Pipelines'
```
