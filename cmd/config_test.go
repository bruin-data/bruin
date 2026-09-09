package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestConfigFileSelection(t *testing.T) {
	t.Setenv("BRUIN_CONFIG_FILE", "")
	t.Setenv("BRUIN_CONFIG_FILE_CONTENT", "")

	commands := []struct {
		name string
		args []string
	}{
		{name: "list environments", args: []string{"environments", "list"}},
		{name: "create environment", args: []string{"environments", "create", "--name", "dev"}},
		{name: "list connections", args: []string{"connections", "list"}},
		{name: "add connection", args: []string{"connections", "add", "--env", "default", "--type", "generic", "--name", "example", "--credentials", `{"value":"test"}`}},
	}
	for _, command := range commands {
		for _, source := range []string{"default", "flag", "env"} {
			for _, exists := range []bool{false, true} {
				name := command.name + "/" + source + "/missing"
				if exists {
					name = command.name + "/" + source + "/existing"
				}
				t.Run(name, func(t *testing.T) {
					root := t.TempDir()
					initGitRepository(t, root)
					t.Chdir(root)
					configPath := filepath.Join(root, ".bruin.yml")
					if source != "default" {
						configPath = filepath.Join(root, "custom.yml")
					}
					if exists {
						require.NoError(t, os.WriteFile(configPath, []byte("default_environment: default\nenvironments:\n  default:\n    connections: {}\n"), 0o600))
					}
					args := append([]string{"bruin"}, command.args...)
					switch source {
					case "flag":
						args = append(args, "--config-file", configPath)
					case "env":
						t.Setenv("BRUIN_CONFIG_FILE", configPath)
					}
					debug := false
					app := &cli.Command{
						Commands:       []*cli.Command{Environments(&debug), Connections()},
						ExitErrHandler: func(context.Context, *cli.Command, error) {},
					}
					err := app.Run(t.Context(), args)
					if source != "default" && !exists {
						require.Error(t, err)
						require.NoFileExists(t, configPath)
						require.NoFileExists(t, filepath.Join(root, ".bruin.yml"))
						require.NoFileExists(t, filepath.Join(root, ".gitignore"))
						return
					}
					require.NoError(t, err)
					require.FileExists(t, configPath)
				})
			}
		}
	}
}
