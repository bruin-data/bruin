package cmd

import (
	"github.com/urfave/cli/v2"
	"log"
	"os"
	"path/filepath"
)

func Init(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "init",
		Usage:     "init a Bruin pipeline",
		ArgsUsage: "[name of the folder pipeline]",
		Flags:     []cli.Flag{},
		Action: func(c *cli.Context) error {
			defer func() {
				if err := recover(); err != nil {
					log.Println("=======================================")
					log.Println("Bruin encountered an unexpected error, please report the issue to the Bruin team.")
					log.Println(err)
					log.Println("=======================================")
				}
			}()

			inputPath := c.Args().Get(0)
			inputPath := c.Args().Get(0)
			if inputPath == "" {
				errorPrinter.Printf("Please provide a name for bruin to create a folder where the pipeline will be created: bruin init <name of folder to be created>)\n")
				return cli.Exit("", 1)
			}

			// Check if the folder already exists
			if _, err := os.Stat(inputPath); !os.IsNotExist(err) {
				errorPrinter.Printf("The folder %s already exists, please choose a different name\n", inputPath)
				return cli.Exit("", 1)
			}
			dir, _ := filepath.Split(inputPath)
			if dir != "" {
				errorPrinter.Printf("Traversing up or down in the folder structure is not allowed, provide base folder name only.\n")
				return cli.Exit("", 1)
			}

			err := os.Mkdir(inputPath, 0755)
			if err != nil {
				errorPrinter.Printf("Failed to create the folder %s: %v\n", inputPath, err)
				return cli.Exit("", 1)
			}

			return nil
		},
	}
}
