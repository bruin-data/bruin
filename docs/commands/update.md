# Update Command

The `bruin update` command allows you to update your Bruin CLI installation to the latest version directly from the command line.

## Usage

```shell
bruin update [flags]
```

## Description

The update command downloads and runs the official Bruin installation script, which:

1. Fetches the latest version of Bruin from GitHub releases
2. Downloads the appropriate binary for your operating system and architecture  
3. Installs it to your local binary directory (typically `~/.local/bin`)
4. Updates your shell configuration if needed

This eliminates the need to manually visit the website and run the installation command each time you want to update.

## Examples

### Update to Latest Version
Update to the latest version:
```shell
bruin update
```

## Prerequisites

The update command requires either `curl` or `wget` to be installed on your system:

- **curl**: Available by default on macOS and most Linux distributions
- **wget**: Available on most Linux distributions, can be installed on macOS via Homebrew

## Platform Notes

### Windows
On Windows, make sure to run the update command in:
- **Git Bash** (recommended)
- **WSL (Windows Subsystem for Linux)**

The command will warn you if you're on Windows to ensure you're using the correct terminal environment.

### Shell Configuration
After updating, you may need to:
- Restart your shell, or
- Run `source ~/.bashrc` (bash), `source ~/.zshrc` (zsh), or equivalent for your shell

## Error Handling

The command will fail gracefully if:
- Neither curl nor wget is available
- The download fails due to network issues
- The installation script encounters an error
- You don't have write permissions to the installation directory

## Security

The update command uses the same installation script that's recommended in the official documentation:
- Downloads from `https://getbruin.com/install/cli`
- Uses HTTPS for secure downloads
- The script is maintained by the Bruin team and hosted on GitHub

## See Also

- [bruin version] - Check your current Bruin version
- [Installation Guide](../getting-started/introduction/installation.md) - Initial installation instructions