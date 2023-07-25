package cmd

import (
	"fmt"
	"io"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/manifoldco/promptui"
	"github.com/urfave/cli/v2"
)

func switchEnvironment(c *cli.Context, cm *config.Config, stdin io.ReadCloser) error {
	env := c.String("environment")
	if env == "" {
		return nil
	}

	err := cm.SelectEnvironment(env)
	if err != nil {
		errorPrinter.Printf("Failed to use the environment '%s': %v\n", env, err)
		return cli.Exit("", 1)
	}

	// if env name is similar to "prod" ask for confirmation
	if !c.Bool("force") && strings.Contains(strings.ToLower(env), "prod") {
		prompt := promptui.Prompt{
			Label:     "You are using a production environment. Are you sure you want to continue?",
			IsConfirm: true,
			Stdin:     stdin,
		}

		_, err := prompt.Run()
		if err != nil {
			fmt.Printf("The operation is cancelled.\n")
			return cli.Exit("", 1)
		}
	}

	return nil
}
