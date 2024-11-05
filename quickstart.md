### Here's a quick start guide to help you get started with Bruin in a few simple steps:

Step 1: Install the Bruin CLI on your machine

Step 2: Initialize a New Pipeline Project

Create a new Bruin project by running the following command:


    bruin init [template name] [folder name]
      
     For example: bruin init default my-pipeline   

This command will:
- Create a project named my-pipeline.
- Generate a folder called my-pipeline containing the following:
    - An assets folder
    - .bruin.yml file (where you will add connection and credentials )
    - pipeline.yml file to manage your pipeline.

Step 3: Add an asset file to the assets folder. The asset file should be in .yml format.

Step 4: Add credentials or connection details to .bruin.yml

step 5: Run your Assets or pipelines

```
bruin run
```

##### For more information, refer to below links :

[Project Initialization](https://bruin-data.github.io/bruin/commands/init.html)

[Read about asset](https://bruin-data.github.io/bruin/assets/definition-schema.html)

[Read about connections and the .bruin.yml file](https://bruin-data.github.io/bruin/commands/connections.html)

[Execute a Bruin pipeline or a specific asset within a pipeline](https://bruin-data.github.io/bruin/commands/run.html)

