# CircleCI

CircleCI uses a `.circleci/config.yml` file to define your pipeline. This guide shows how to set up Bruin in your CircleCI pipeline to validate your Bruin pipelines.

## Installation

Bruin can be installed in CircleCI using the installation script. The CLI will be automatically added to your PATH.

```yaml
- run:
    name: Install Bruin
    command: |
      curl -LsSf https://getbruin.com/install/cli | sh
      echo 'export PATH=$HOME/.local/bin:$PATH' >> $BASH_ENV
```

## Setting Up CircleCI

1. Create a `.circleci` directory in your repository.
2. Inside that directory, create a `config.yml` file:

```yaml
version: 2.1

jobs:
  validate:
    docker:
      - image: cimg/base:stable
    steps:
      - checkout

      # Install Bruin CLI
      - run:
          name: Install Bruin
          command: |
            curl -LsSf https://getbruin.com/install/cli | sh
            echo 'export PATH=$HOME/.local/bin:$PATH' >> $BASH_ENV

      # Validate your pipelines
      - run:
          name: Validate Pipelines
          command: bruin validate

workflows:
  version: 2
  validate-pipeline:
    jobs:
      - validate
```

## Configuration Options

### Specify Bruin Version

To install a specific version of Bruin, pass the version tag to the install script:

```yaml
version: 2.1

jobs:
  validate:
    docker:
      - image: cimg/base:stable
    steps:
      - checkout
      - run:
          name: Install Bruin
          command: |
            curl -LsSf https://getbruin.com/install/cli | sh -s v0.1.0
            echo 'export PATH=$HOME/.local/bin:$PATH' >> $BASH_ENV
      - run:
          name: Validate Pipelines
          command: bruin validate

workflows:
  version: 2
  validate-pipeline:
    jobs:
      - validate
```

### Running on Pull Requests Only

To run validation only on pull requests:

```yaml
version: 2.1

jobs:
  validate:
    docker:
      - image: cimg/base:stable
    steps:
      - checkout
      - run:
          name: Install Bruin
          command: |
            curl -LsSf https://getbruin.com/install/cli | sh
            echo 'export PATH=$HOME/.local/bin:$PATH' >> $BASH_ENV
      - run:
          name: Validate Pipelines
          command: bruin validate

workflows:
  version: 2
  validate-pipeline:
    jobs:
      - validate:
          filters:
            branches:
              ignore: main
```
