---
outline: deep
---

# What is Bruin?

Bruin is a command-line tool that allows you to build end-to-end data pipelines that can:
- ingest data using Python
- transform data using Python and SQL
- add quality checks to your pipelines

You can use Bruin to build your data models inside your data warehouse.

<br>

# Installation
Bruin can be installed with a variety of methods.

## MacOS

If you are on macOS, you can use `brew` to install Bruin:

```shell
brew install bruin-data/tap/bruin
```

## Windows, Linux and MacOS

If you are on macOS, linux or windows, you can use `curl` to install Bruin:

```shell
curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/refs/heads/main/install.sh | sh
```

Or you can also use `wget` to install Bruin:

```shell
wget -qO- https://raw.githubusercontent.com/bruin-data/bruin/refs/heads/main/install.sh | sh
```


### Golang
You can install via Golang installer:

```shell
go install github.com/bruin-data/bruin@v0.1.5
```

### Pre-built binaries
You can download [one of the pre-built releases](https://github.com/bruin-data/bruin/releases) for your environment.

### Creating your first Bruin project

To create a bruin project basic structure you can just run

```
bruin init {folder name} [template name]
```

you can see the available template names by running `bruin help init`


# Troubleshooting

- Permission denied' error while installing the Bruin CLI, In this case user doesn't have permission to write the `binary` in `~/.local/bin` directory 

