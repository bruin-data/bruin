package cmd

import (
	fs2 "io/fs"
	"log"
	"os"
	path2 "path"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/templates"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

const DefaultTemplate = "default"

func Init(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "init",
		Usage:     "init a Bruin pipeline",
		ArgsUsage: "[name of the folder pipeline]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "template",
				Aliases: []string{"t"},
				Value:   DefaultTemplate,
				Usage:   "bruin template to use",
			},
		},
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

			err := os.Mkdir(inputPath, 0o755)
			if err != nil {
				errorPrinter.Printf("Failed to create the folder %s: %v\n", inputPath, err)
				return cli.Exit("", 1)
			}

			templateName := c.String("template")
			_, err = templates.Templates.ReadDir(templateName)
			if err != nil {
				errorPrinter.Printf("Template %s not found\n", templateName)
				return cli.Exit("", 1)
			}

			err = fs2.WalkDir(templates.Templates, templateName, func(path string, d fs2.DirEntry, err error) error {
				if err != nil {
					return err
				}

				// Walk returns the root as if it was its own content
				if path == templateName {
					return nil
				}

				// Walk returns the root as if it was its own content
				if d.IsDir() {
					return nil
				}

				fileContents, err := templates.Templates.ReadFile(path)
				if err != nil {
					return err
				}

				relativePath, baseName := filepath.Split(path)
				relativePath = strings.TrimPrefix(relativePath, templateName)
				absolutePath := inputPath + relativePath

				// ignore the error
				_ = os.Mkdir(absolutePath, os.ModePerm)

				err = os.WriteFile(filepath.Join(absolutePath, baseName), fileContents, 0o644) //nolint:gosec
				if err != nil {
					errorPrinter.Printf("Could not write the %s file\n, %v", filepath.Join(absolutePath, baseName), err)
					return err
				}

				_, err = config.LoadOrCreate(afero.NewOsFs(), path2.Join(inputPath, ".bruin.yml"))
				if err != nil {
					errorPrinter.Printf("Could not write .bruin.yml file\n, %v", err)
					return err
				}

				return nil
			})
			if err != nil {
				errorPrinter.Printf("Could not copy template %s: %s\n", templateName, err)
				return cli.Exit("", 1)
			}

			return nil
		},
	}
}
