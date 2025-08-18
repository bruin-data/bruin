package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	path2 "path"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func Environments(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "environments",
		Usage: "manage environments defined in .bruin.yml",
		Commands: []*cli.Command{
			ListEnvironments(isDebug),
			CreateEnvironment(isDebug),
			UpdateEnvironment(isDebug),
			DeleteEnvironment(isDebug),
			CloneEnvironment(isDebug),
		},
	}
}

func ListEnvironments(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "list",
		Usage: "list environments found in the current repo",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			r := EnvironmentListCommand{}
			logger := makeLogger(*isDebug)

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path2.Clean("."))
				if err != nil {
					printError(errors.Wrap(err, "Failed to find the git repository root"), strings.ToLower(c.String("output")), "")
					return cli.Exit("", 1)
				}
				logger.Debugf("found repo root '%s'", repoRoot.Path)
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			return r.Run(strings.ToLower(c.String("output")), configFilePath)
		},
	}
}

type EnvironmentListCommand struct{}

func (r *EnvironmentListCommand) Run(output, configFilePath string) error {
	defer RecoverFromPanic()

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		printError(err, output, "Failed to load the config file at "+configFilePath)
		return cli.Exit("", 1)
	}

	envs := cm.GetEnvironmentNames()
	if output == "json" {
		type environ struct {
			Name string `json:"name"`
		}

		type envResponse struct {
			SelectedEnvironment string    `json:"selected_environment"`
			Environments        []environ `json:"environments"`
		}
		resp := envResponse{
			SelectedEnvironment: cm.SelectedEnvironmentName,
			Environments:        make([]environ, len(envs)),
		}

		i := 0
		for _, env := range envs {
			resp.Environments[i] = environ{
				Name: env,
			}
			i++
		}

		js, err := json.Marshal(resp)
		if err != nil {
			printErrorJSON(err)
			return err
		}

		fmt.Println(string(js))
		return nil
	}

	fmt.Println()
	infoPrinter.Println("Selected environment: " + cm.SelectedEnvironmentName)
	infoPrinter.Println("Available environments:")
	for _, env := range envs {
		infoPrinter.Println("- " + env)
	}

	return nil
}

func CreateEnvironment(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "create",
		Usage: "create a new environment",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "name",
				Aliases:  []string{"n"},
				Usage:    "the name of the environment",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "schema-prefix",
				Aliases: []string{"s"},
				Usage:   "schema prefix for schema-based environments",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			envName := c.String("name")
			schemaPrefix := c.String("schema-prefix")
			output := strings.ToLower(c.String("output"))

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path2.Clean("."))
				if err != nil {
					printError(errors.Wrap(err, "Failed to find the git repository root"), output, "")
					return cli.Exit("", 1)
				}
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
			if err != nil {
				printError(err, output, "Failed to load the config file at "+configFilePath)
				return cli.Exit("", 1)
			}

			if _, exists := cm.Environments[envName]; exists {
				printError(fmt.Errorf("environment '%s' already exists", envName), output, "")
				return cli.Exit("", 1)
			}

			if err := cm.AddEnvironment(envName, schemaPrefix); err != nil {
				printError(err, output, "failed to add environment")
				return cli.Exit("", 1)
			}

			if err := cm.Persist(); err != nil {
				printError(err, output, "failed to persist config")
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, "Successfully created environment: "+envName)
			return nil
		},
	}
}

func UpdateEnvironment(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "update",
		Usage: "update an existing environment",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "name",
				Aliases:  []string{"n"},
				Usage:    "the current name of the environment",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "new-name",
				Usage: "the new name for the environment",
			},
			&cli.StringFlag{
				Name:    "schema-prefix",
				Aliases: []string{"s"},
				Usage:   "schema prefix for schema-based environments",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			r := EnvironmentUpdateCommand{}
			logger := makeLogger(*isDebug)

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path2.Clean("."))
				if err != nil {
					printError(errors.Wrap(err, "Failed to find the git repository root"), strings.ToLower(c.String("output")), "")
					return cli.Exit("", 1)
				}
				logger.Debugf("found repo root '%s'", repoRoot.Path)
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			return r.Run(c.String("name"), c.String("new-name"), c.String("schema-prefix"), strings.ToLower(c.String("output")), configFilePath)
		},
	}
}

type EnvironmentUpdateCommand struct{}

func (r *EnvironmentUpdateCommand) Run(name, newName, schemaPrefix, output, configFilePath string) error {
	defer RecoverFromPanic()

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		printError(err, output, "Failed to load the config file at "+configFilePath)
		return cli.Exit("", 1)
	}

	// Validate environment exists
	if !cm.EnvironmentExists(name) {
		printError(fmt.Errorf("environment '%s' does not exist", name), output, "")
		return cli.Exit("", 1)
	}

	// Use current name if new name is not provided
	if newName == "" {
		newName = name
	}

	// Validate new name doesn't conflict (unless it's the same as current name)
	if name != newName && cm.EnvironmentExists(newName) {
		printError(fmt.Errorf("environment '%s' already exists", newName), output, "")
		return cli.Exit("", 1)
	}

	// Update the environment
	if err := cm.UpdateEnvironment(name, newName, schemaPrefix); err != nil {
		printError(err, output, "failed to update environment")
		return cli.Exit("", 1)
	}

	// Persist changes
	if err := cm.Persist(); err != nil {
		printError(err, output, "failed to persist config")
		return cli.Exit("", 1)
	}

	if output == "json" {
		result := map[string]interface{}{
			"message":  "Environment updated successfully",
			"old_name": name,
			"new_name": newName,
		}
		if schemaPrefix != "" {
			result["schema_prefix"] = schemaPrefix
		}
		js, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			printError(err, output, "failed to marshal JSON")
			return cli.Exit("", 1)
		}
		fmt.Println(string(js))
		return nil
	}

	message := fmt.Sprintf("Successfully updated environment '%s'", name)
	if name != newName {
		message = fmt.Sprintf("Successfully updated environment '%s' to '%s'", name, newName)
	}
	printSuccessForOutput(output, message)
	return nil
}

func DeleteEnvironment(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "delete",
		Usage: "delete an existing environment",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "name",
				Aliases:  []string{"n"},
				Usage:    "the name of the environment to delete",
				Required: true,
			},
			&cli.BoolFlag{
				Name:    "force",
				Aliases: []string{"f"},
				Usage:   "skip confirmation prompt",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			r := EnvironmentDeleteCommand{}
			logger := makeLogger(*isDebug)

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path2.Clean("."))
				if err != nil {
					printError(errors.Wrap(err, "Failed to find the git repository root"), strings.ToLower(c.String("output")), "")
					return cli.Exit("", 1)
				}
				logger.Debugf("found repo root '%s'", repoRoot.Path)
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			return r.Run(c.String("name"), c.Bool("force"), strings.ToLower(c.String("output")), configFilePath)
		},
	}
}

type EnvironmentDeleteCommand struct{}

func (r *EnvironmentDeleteCommand) Run(name string, force bool, output, configFilePath string) error {
	defer RecoverFromPanic()

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		printError(err, output, "Failed to load the config file at "+configFilePath)
		return cli.Exit("", 1)
	}

	// Validate environment exists
	if !cm.EnvironmentExists(name) {
		printError(fmt.Errorf("environment '%s' does not exist", name), output, "")
		return cli.Exit("", 1)
	}

	// Check if it's the last environment
	if len(cm.Environments) == 1 {
		printError(errors.New("cannot delete the last environment"), output, "")
		return cli.Exit("", 1)
	}

	// Ask for confirmation unless force flag is set
	if !force {
		var response string
		fmt.Printf("Are you sure you want to delete environment '%s'? (y/N): ", name)
		_, err := fmt.Scanln(&response)
		if err != nil || (response != "y" && response != "Y" && response != "yes" && response != "YES") {
			if output == "json" {
				result := map[string]interface{}{
					"message": "Environment deletion cancelled",
					"name":    name,
				}
				js, err := json.MarshalIndent(result, "", "  ")
				if err != nil {
					printError(err, output, "failed to marshal JSON")
					return cli.Exit("", 1)
				}
				fmt.Println(string(js))
			} else {
				fmt.Println("Environment deletion cancelled.")
			}
			return nil
		}
	}

	// Delete the environment
	if err := cm.DeleteEnvironment(name); err != nil {
		printError(err, output, "failed to delete environment")
		return cli.Exit("", 1)
	}

	// Persist changes
	if err := cm.Persist(); err != nil {
		printError(err, output, "failed to persist config")
		return cli.Exit("", 1)
	}

	if output == "json" {
		result := map[string]interface{}{
			"message": "Environment deleted successfully",
			"name":    name,
		}
		js, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			printError(err, output, "failed to marshal JSON")
			return cli.Exit("", 1)
		}
		fmt.Println(string(js))
		return nil
	}

	printSuccessForOutput(output, fmt.Sprintf("Successfully deleted environment '%s'", name))
	return nil
}

func CloneEnvironment(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:  "clone",
		Usage: "clone an existing environment",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "source",
				Aliases: []string{"s"},
				Usage:   "the name of the environment to clone from (defaults to default environment)",
			},
			&cli.StringFlag{
				Name:     "target",
				Aliases:  []string{"t"},
				Usage:    "the name of the new environment",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "schema-prefix",
				Aliases: []string{"p"},
				Usage:   "schema prefix for the cloned environment (optional)",
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.StringFlag{
				Name:    "config-file",
				Sources: cli.EnvVars("BRUIN_CONFIG_FILE"),
				Usage:   "the path to the .bruin.yml file",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			r := EnvironmentCloneCommand{}
			logger := makeLogger(*isDebug)

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path2.Clean("."))
				if err != nil {
					printError(errors.Wrap(err, "Failed to find the git repository root"), strings.ToLower(c.String("output")), "")
					return cli.Exit("", 1)
				}
				logger.Debugf("found repo root '%s'", repoRoot.Path)
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			return r.Run(c.String("source"), c.String("target"), c.String("schema-prefix"), strings.ToLower(c.String("output")), configFilePath)
		},
	}
}

type EnvironmentCloneCommand struct{}

func (r *EnvironmentCloneCommand) Run(sourceName, targetName, schemaPrefix, output, configFilePath string) error {
	defer RecoverFromPanic()

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		printError(err, output, "Failed to load the config file at "+configFilePath)
		return cli.Exit("", 1)
	}

	// Use default environment if source is not specified
	if sourceName == "" {
		sourceName = cm.DefaultEnvironmentName
	}

	// Validate source environment exists
	if !cm.EnvironmentExists(sourceName) {
		printError(fmt.Errorf("source environment '%s' does not exist", sourceName), output, "")
		return cli.Exit("", 1)
	}

	// Validate target environment doesn't exist
	if cm.EnvironmentExists(targetName) {
		printError(fmt.Errorf("target environment '%s' already exists", targetName), output, "")
		return cli.Exit("", 1)
	}

	// Clone the environment
	if err := cm.CloneEnvironment(sourceName, targetName, schemaPrefix); err != nil {
		printError(err, output, "failed to clone environment")
		return cli.Exit("", 1)
	}

	// Persist changes
	if err := cm.Persist(); err != nil {
		printError(err, output, "failed to persist config")
		return cli.Exit("", 1)
	}

	if output == "json" {
		result := map[string]interface{}{
			"message":     "Environment cloned successfully",
			"source_name": sourceName,
			"target_name": targetName,
		}
		if schemaPrefix != "" {
			result["schema_prefix"] = schemaPrefix
		}
		js, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			printError(err, output, "failed to marshal JSON")
			return cli.Exit("", 1)
		}
		fmt.Println(string(js))
		return nil
	}

	message := fmt.Sprintf("Successfully cloned environment '%s' to '%s'", sourceName, targetName)
	if schemaPrefix != "" {
		message += fmt.Sprintf(" with schema prefix '%s'", schemaPrefix)
	}
	printSuccessForOutput(output, message)
	return nil
}
