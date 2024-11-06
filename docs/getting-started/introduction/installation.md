---
outline: deep
---

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

> [!IMPORTANT]
> If you are on Windows, make sure to run the command in the Git Bash or WSL terminal.


## Troubleshooting

### 'Permission Denied' error during the installation

**Issue**  
When installing the Bruin CLI, you may encounter a `'Permission Denied'` error. This typically happens if the user doesn't have permission to write the binary to the `~/.local/bin` directory.

**Solution**  
To resolve this, ensure that you have the necessary write permissions for the `~/.local/bin` directory. You can do this by running the following command with sudo:

```shell
curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/refs/heads/main/install.sh | sudo sh
```
