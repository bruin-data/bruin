# Init Command

The `init` command initializes a new Bruin pipeline project. It creates a new Bruin pipeline project based on a specified template. If no template is specified, it uses the default template. The command creates a new folder with the project structure and necessary files.


```
bruin init [template name] [folder name]
```

## Arguments

1. `template name` (optional): The name of the template to use. Available templates: 
   - If not provided, the default template is used.

2. `folder name` (optional): The name of the folder where the pipeline will be created.
   - If not provided and using the default template, it will create a folder named "bruin-pipeline".
   - If not provided and using a custom template, it will create a folder with the same name as the template.

> [!INFO]
> If the specified folder already exists, the command will fail.

## Available Templates
In order to see the list of available templates, you can run:
```
bruin init --help
```

The output displays the list of available templates.
