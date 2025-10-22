# GitLab CI/CD

GitLab CI/CD is built directly into GitLab and uses a `.gitlab-ci.yml` file to define your pipeline. This guide shows how to set up Bruin in your GitLab CI/CD pipeline to validate your Bruin pipelines.

## Installation

Bruin can be installed in GitLab CI/CD using the installation script. Add this to your job steps to install the latest version of Bruin CLI.

```yaml
- curl -LsSf https://getbruin.com/install/cli | sh
- export PATH=$HOME/.local/bin:$PATH
```

## Setting Up GitLab CI/CD

Create a `.gitlab-ci.yml` file in the root of your repository:

```yaml
stages:
  - validate

bruin-validate:
  stage: validate
  image: ubuntu:latest
  before_script:
    # Install Bruin CLI
    - apt-get update && apt-get install -y curl
    - curl -LsSf https://getbruin.com/install/cli | sh
    - export PATH=$HOME/.local/bin:$PATH
  script:
    # Validate your pipelines
    - bruin validate
  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH

```

## Configuration Options

### Specify Bruin Version

To install a specific version of Bruin, pass the version tag to the install script:

```yaml
bruin-validate:
  stage: validate
  image: ubuntu:latest
  before_script:
    - apt-get update && apt-get install -y curl
    - curl -LsSf https://getbruin.com/install/cli | sh -s v0.1.0
    - export PATH=$HOME/.local/bin:$PATH
  script:
    - bruin validate
```
