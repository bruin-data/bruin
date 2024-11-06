package cmd

import (
	"fmt"
	fs2 "io/fs"
	"log"
	"os"
	"os/exec"
	path2 "path"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/templates"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

const (
	DefaultTemplate   = "default"
	DefaultFolderName = "bruin-pipeline"
)

func Init() *cli.Command {
	folders, err := templates.Templates.ReadDir(".")
	if err != nil {
		panic("Error retrieving bruin templates")
	}
	templateList := make([]string, 0)
	for _, entry := range folders {
		if entry.IsDir() {
			templateList = append(templateList, entry.Name())
		}
	}

	return &cli.Command{
		Name:  "init",
		Usage: "init a Bruin pipeline",
		ArgsUsage: fmt.Sprintf(
			"[template name to be used: %s] [name of the folder where the pipeline will be created]",
			strings.Join(templateList, "|"),
		),
		Flags: []cli.Flag{},
		Action: func(c *cli.Context) error {
			defer func() {
				if err := recover(); err != nil {
					log.Println("=======================================")
					log.Println("Bruin encountered an unexpected error, please report the issue to the Bruin team.")
					log.Println(err)
					log.Println("=======================================")
				}
			}()

			templateName := c.Args().Get(0)
			if templateName == "" {
				templateName = DefaultTemplate
			}

			_, err = templates.Templates.ReadDir(templateName)
			if err != nil {
				errorPrinter.Printf("Template '%s' not found\n", templateName)
				return cli.Exit("", 1)
			}

			inputPath := c.Args().Get(1)
			if inputPath == "" {
				if templateName == DefaultTemplate {
					inputPath = DefaultFolderName
				} else {
					inputPath = templateName
				}
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

			_, err = config.LoadOrCreate(afero.NewOsFs(), path2.Join(inputPath, ".bruin.yml"))
			if err != nil {
				errorPrinter.Printf("Could not write .bruin.yml file: %v\n", err)
				return err
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
				err = os.MkdirAll(absolutePath, os.ModePerm)
				if err != nil {
					errorPrinter.Printf("Could not create the %s folder: %v\n", absolutePath, err)
					return err
				}

				err = os.WriteFile(filepath.Join(absolutePath, baseName), fileContents, 0o644) //nolint:gosec
				if err != nil {
					errorPrinter.Printf("Could not write the %s file: %v\n", filepath.Join(absolutePath, baseName), err)
					return err
				}

				return nil
			})
			if err != nil {
				errorPrinter.Printf("Could not copy template %s: %s\n", templateName, err)
				return cli.Exit("", 1)
			}

			cmd := exec.Command("git", "init")
			cmd.Dir = inputPath
			out, err := cmd.CombinedOutput()
			if err != nil {
				errorPrinter.Printf("Could not initialize git repository: %s\n", string(out))
				return cli.Exit("", 1)
			}

			return nil
		},
	}
}
