# Jenkins

Jenkins is a widely-used automation server that can be configured using Jenkinsfiles. This guide shows how to set up Bruin in your Jenkins pipeline to validate your Bruin pipelines.

## Installation

Bruin can be installed in Jenkins using the installation script. This works on any Jenkins agent with bash/sh support.

```groovy
sh 'curl -LsSf https://getbruin.com/install/cli | sh'
sh 'export PATH=$HOME/.local/bin:$PATH'
```

## Setting Up Jenkins

### Declarative Pipeline

Create a `Jenkinsfile` in the root of your repository:

```groovy
pipeline {
    agent any

    stages {
        stage('Install Bruin') {
            steps {
                sh '''
                    curl -LsSf https://getbruin.com/install/cli | sh
                    export PATH=$HOME/.local/bin:$PATH
                '''
            }
        }

        stage('Validate Pipelines') {
            steps {
                sh '''
                    export PATH=$HOME/.local/bin:$PATH
                    bruin validate
                '''
            }
        }
    }
}
```

### Scripted Pipeline

Alternatively, you can use a scripted pipeline:

```groovy
node {
    stage('Checkout') {
        checkout scm
    }

    stage('Install Bruin') {
        sh '''
            curl -LsSf https://getbruin.com/install/cli | sh
            export PATH=$HOME/.local/bin:$PATH
        '''
    }

    stage('Validate Pipelines') {
        sh '''
            export PATH=$HOME/.local/bin:$PATH
            bruin validate
        '''
    }
}
```

## Configuration Options

### Specify Bruin Version

To install a specific version of Bruin, pass the version tag to the install script:

```groovy
pipeline {
    agent any

    stages {
        stage('Install Bruin') {
            steps {
                sh '''
                    curl -LsSf https://getbruin.com/install/cli | sh -s v0.1.0
                    export PATH=$HOME/.local/bin:$PATH
                '''
            }
        }

        stage('Validate Pipelines') {
            steps {
                sh '''
                    export PATH=$HOME/.local/bin:$PATH
                    bruin validate
                '''
            }
        }
    }
}
```

### Using Docker Agent

If you prefer to use a Docker container:

```groovy
pipeline {
    agent {
        docker {
            image 'ubuntu:latest'
        }
    }

    stages {
        stage('Install Bruin') {
            steps {
                sh '''
                    apt-get update && apt-get install -y curl
                    curl -LsSf https://getbruin.com/install/cli | sh
                    export PATH=$HOME/.local/bin:$PATH
                '''
            }
        }

        stage('Validate Pipelines') {
            steps {
                sh '''
                    export PATH=$HOME/.local/bin:$PATH
                    bruin validate
                '''
            }
        }
    }
}
```

### Run on Pull Requests Only

To run validation only on pull requests (requires the GitHub Branch Source or similar plugin):

```groovy
pipeline {
    agent any

    when {
        changeRequest()
    }

    stages {
        stage('Install Bruin') {
            steps {
                sh '''
                    curl -LsSf https://getbruin.com/install/cli | sh
                    export PATH=$HOME/.local/bin:$PATH
                '''
            }
        }

        stage('Validate Pipelines') {
            steps {
                sh '''
                    export PATH=$HOME/.local/bin:$PATH
                    bruin validate
                '''
            }
        }
    }
}
```
