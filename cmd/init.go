package cmd

import (
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/urfave/cli/v2"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"path/filepath"
	"time"
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

			err = os.Mkdir(filepath.Join(inputPath, "assets"), 0755)
			if err != nil {
				errorPrinter.Printf("Failed to create the folder %s: %v\n", filepath.Join(inputPath, "assets"), err)
				return cli.Exit("", 1)
			}

			err = os.WriteFile(filepath.Join(inputPath, ".gitignore"), []byte(".bruin.yml\n"), 0644)
			if err != nil {
				errorPrinter.Printf("Could not create .gitignore file\n, %v", err)
				return cli.Exit("", 1)
			}

			pipeline := pipeline.Pipeline{
				LegacyID:          "",
				Name:              inputPath,
				Schedule:          "daily",
				StartDate:         time.Now().Format("2006-01-02"),
				DefinitionFile:    pipeline.DefinitionFile{},
				DefaultParameters: nil,
				DefaultConnections: map[string]string{
					"postgres": "example-postgres-connection",
				},
				Assets:        nil,
				Notifications: pipeline.Notifications{},
				Catchup:       false,
				Retries:       0,
				TasksByType:   nil,
			}

			yamlData, err := yaml.Marshal(&pipeline)
			if err != nil {
				errorPrinter.Printf("Failed to marshal the pipeline definition to yaml: %v\n", err)
				return cli.Exit("", 1)
			}

			err = os.WriteFile(filepath.Join(inputPath, "pipeline.yml"), yamlData, 0644)
			if err != nil {
				errorPrinter.Printf("Failed to create the pipeline.yml file: %v\n", err)
				return cli.Exit("", 1)
			}

			return nil
		},
	}
}
