package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/python"
	errors2 "github.com/pkg/errors"
	"github.com/sourcegraph/conc/pool"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func Format(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "format",
		Usage:     "format given asset definition files",
		ArgsUsage: "[path to project root]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "the output type, possible values are: plain, json",
			},
			&cli.BoolFlag{
				Name:  "fail-if-changed",
				Usage: "fail the command if any of the assets need reformatting",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "sqlfluff",
				Usage: "run sqlfluff to format SQL files",
				Value: false,
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			logger := makeLogger(*isDebug)

			repoOrAsset := c.Args().Get(0)
			if repoOrAsset == "" {
				repoOrAsset = "."
			}

			output := c.String("output")
			checkLint := c.Bool("fail-if-changed")
			runSqlfluff := c.Bool("sqlfluff")

			if isPathReferencingAsset(repoOrAsset) {
				if checkLint {
					return checkChangesForSingleAsset(repoOrAsset, output)
				}
				asset, err := formatAsset(repoOrAsset)
				if err != nil {
					if errors.Is(err, os.ErrNotExist) {
						printErrorForOutput(output, fmt.Errorf("the given file path '%s' does not seem to exist, are you sure you used the right path?", repoOrAsset))
					} else {
						printErrorForOutput(output, errors2.Wrap(err, "failed to format asset"))
					}
					return cli.Exit("", 1)
				}
				if output != "json" {
					infoPrinter.Printf("Successfully formatted asset '%s' %s\n", asset.Name, faint(fmt.Sprintf("(%s)", asset.DefinitionFile.Path)))
				}
			}

			assetFinderPool := pool.New().WithMaxGoroutines(32)
			assetPaths := path.GetAllPossibleAssetPaths(repoOrAsset, assetsDirectoryNames, pipeline.SupportedFileSuffixes)

			if len(assetPaths) == 0 {
				infoPrinter.Println("No assets were found in the given path, aborting.")
				return cli.Exit("", 1)
			}

			logger.Debugf("found %d asset paths", len(assetPaths))

			errorList := make([]error, 0)
			changedAssetpaths := make([]string, 0)
			processedAssetCount := 0

			for _, assetPath := range assetPaths {
				assetFinderPool.Go(func() {
					if checkLint {
						changed, err := shouldFileChange(assetPath)
						if err != nil {
							logger.Debugf("failed to process path '%s': %v", assetPath, err)
							errorList = append(errorList, errors2.Wrapf(err, "failed to check '%s'", assetPath))
							return
						}
						if changed {
							changedAssetpaths = append(changedAssetpaths, assetPath)
						}
					} else {
						asset, err := DefaultPipelineBuilder.CreateAssetFromFile(assetPath, nil)
						if err != nil {
							logger.Debugf("failed to process path '%s': %v", assetPath, err)
							return
						}

						if asset == nil {
							logger.Debugf("no asset found in path '%s': %v", assetPath, err)
							return
						}

						err = asset.Persist(afero.NewOsFs())
						if err != nil {
							logger.Debugf("failed to persist asset '%s': %v", assetPath, err)
							errorList = append(errorList, errors2.Wrapf(err, "failed to persist asset '%s'", assetPath))
						}

						processedAssetCount++
					}
				})
			}

			assetFinderPool.Wait()
			if checkLint && len(errorList) == 0 {
				if len(changedAssetpaths) == 0 {
					if output == "json" {
						return nil
					}
					infoPrinter.Printf("success: no asset in '%s' needs reformatting", repoOrAsset)
					return nil
				}
				errorPrinter.Println("failure: some Assets needs reformatting:")
				for _, assetPath := range changedAssetpaths {
					infoPrinter.Printf("'%s'\n", assetPath)
				}
				return cli.Exit("", 1)
			}

			if checkLint || len(errorList) > 0 {
				if output == "json" {
					jsMessage, err := json.Marshal(errorList)
					if err != nil {
						printErrorJSON(err)
					}
					fmt.Println(string(jsMessage))
					return cli.Exit("", 1)
				}

				errorPrinter.Println("Some errors occurred:")
				for _, err := range errorList {
					errorPrinter.Println("  - " + err.Error())
				}

				return cli.Exit("", 1)
			}

			if output == "json" {
				return nil
			}

			if processedAssetCount == 0 {
				infoPrinter.Println("no actual assets were found in the given path, nothing has changed.")
				return nil
			}

			assetStr := "asset"
			if processedAssetCount > 1 {
				assetStr += "s"
			}
			infoPrinter.Printf("Successfully formatted %d %s.\n", processedAssetCount, assetStr)

			// Run sqlfluff if requested
			if runSqlfluff {
				if err := runSqlfluffWithErrorHandling(repoOrAsset, output, logger); err != nil { //nolint:contextcheck
					return err
				}
			}

			return nil
		},
	}
}

func formatAsset(path string) (*pipeline.Asset, error) {
	asset, err := DefaultPipelineBuilder.CreateAssetFromFile(path, nil)
	if err != nil {
		return nil, errors2.Wrap(err, "failed to build the asset")
	}

	if asset == nil {
		return nil, errors2.New("no valid asset found in the file")
	}

	return asset, asset.Persist(afero.NewOsFs())
}

func shouldFileChange(path string) (bool, error) {
	fs := afero.NewOsFs()

	// Read the original content from the file
	originalContent, err := afero.ReadFile(fs, path)
	if err != nil {
		return false, errors2.Wrap(err, "failed to read original content")
	}
	normalizedOriginalContent := normalizeLineEndings(originalContent)

	// Create the asset
	asset, err := DefaultPipelineBuilder.CreateAssetFromFile(path, nil)
	if err != nil {
		return false, errors2.Wrap(err, "failed to build the asset")
	}

	if asset == nil {
		return false, errors2.New("no valid asset found in the file")
	}

	// Generate the new content without persisting
	newContent, err := asset.FormatContent()
	if err != nil {
		return false, errors2.Wrap(err, "failed to generate new content without persisting")
	}
	normalizedNewContent := normalizeLineEndings(newContent)

	// Compare the normalized original and new content
	return string(normalizedOriginalContent) != string(normalizedNewContent), nil
}

func checkChangesForSingleAsset(repoOrAsset, output string) error {
	changed, err := shouldFileChange(repoOrAsset)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			printErrorForOutput(output, fmt.Errorf("the given file path '%s' does not seem to exist, are you sure you used the right path?", repoOrAsset))
		} else {
			printErrorForOutput(output, errors2.Wrap(err, "failed to format asset"))
		}
		return cli.Exit("", 1)
	}
	if changed {
		printErrorForOutput(output, fmt.Errorf("failure: asset '%s' needs to be reformatted", repoOrAsset))
		return cli.Exit("", 1)
	}
	if output == "json" {
		return nil
	}
	infoPrinter.Printf("success: Asset '%s' doesn't need reformatting", repoOrAsset)
	return nil
}

func normalizeLineEndings(content []byte) []byte {
	content = bytes.ReplaceAll(content, []byte("\r\n"), []byte("\n"))
	return bytes.ReplaceAll(content, []byte("\r"), []byte("\n"))
}

func runSqlfluffFormatting(repoPath string, logger interface{}) error {
	// Find all SQL files with their asset information
	sqlFilesWithDialects, err := findSQLFilesWithDialects(repoPath)
	if err != nil {
		return errors2.Wrap(err, "failed to find SQL files")
	}

	if len(sqlFilesWithDialects) == 0 {
		return nil // No SQL files to format
	}

	// Find the git repo
	repoFinder := &git.RepoFinder{}
	repo, err := repoFinder.Repo(repoPath)
	if err != nil {
		return errors2.Wrap(err, "failed to find git repository")
	}

	// Create sqlfluff runner
	sqlfluffRunner := &python.SqlfluffRunner{
		UvInstaller: &python.UvChecker{},
	}

	// Run sqlfluff on all SQL files
	ctx := context.WithValue(context.Background(), executor.ContextLogger, logger)
	return sqlfluffRunner.RunSqlfluffWithDialects(ctx, sqlFilesWithDialects, repo)
}

func runSqlfluffFormattingForSingleAsset(assetPath string, repo *git.Repo, logger interface{}) error {
	// Parse the single asset to get its type
	asset, err := DefaultPipelineBuilder.CreateAssetFromFile(assetPath, nil)
	if err != nil {
		return errors2.Wrap(err, "failed to parse asset")
	}

	if asset == nil {
		return nil // Not a valid asset
	}

	// Detect dialect from asset type
	dialect := python.DetectDialectFromAssetType(string(asset.Type))

	// Convert to absolute path for now since we're working in a nested repo structure
	absolutePath, err := filepath.Abs(assetPath)
	if err != nil {
		return errors2.Wrap(err, "failed to get absolute path")
	}

	sqlFileInfo := python.SQLFileInfo{
		FilePath: absolutePath,
		Dialect:  dialect,
	}

	// Create sqlfluff runner
	sqlfluffRunner := &python.SqlfluffRunner{
		UvInstaller: &python.UvChecker{},
	}

	// Run sqlfluff on the single file
	ctx := context.WithValue(context.Background(), executor.ContextLogger, logger)
	return sqlfluffRunner.RunSqlfluffWithDialects(ctx, []python.SQLFileInfo{sqlFileInfo}, repo)
}

func findSQLFilesWithDialects(repoPath string) ([]python.SQLFileInfo, error) {
	// Check if we're formatting a single asset
	if isPathReferencingAsset(repoPath) {
		// Single asset - detect dialect from the asset itself
		asset, err := DefaultPipelineBuilder.CreateAssetFromFile(repoPath, nil)
		if err != nil {
			return nil, errors2.Wrap(err, "failed to parse asset")
		}

		if asset != nil {
			dialect := python.DetectDialectFromAssetType(string(asset.Type))
			return []python.SQLFileInfo{{
				FilePath: repoPath,
				Dialect:  dialect,
			}}, nil
		}
		return []python.SQLFileInfo{}, nil
	}

	// Full pipeline - analyze all SQL files and detect dialects in parallel
	sqlPaths := path.GetAllPossibleAssetPaths(repoPath, assetsDirectoryNames, []string{".sql"})

	if len(sqlPaths) == 0 {
		return []python.SQLFileInfo{}, nil
	}

	// Use conc pool to process assets in parallel with max 30 goroutines
	assetPool := pool.New().WithMaxGoroutines(30)
	sqlFilesWithDialects := make([]python.SQLFileInfo, len(sqlPaths))
	errorList := make([]error, 0)

	for i, sqlPath := range sqlPaths {
		assetPool.Go(func() {
			index := i
			// Check if file exists
			if _, err := os.Stat(sqlPath); err != nil {
				return
			}

			// Try to parse asset to get type information
			asset, err := DefaultPipelineBuilder.CreateAssetFromFile(sqlPath, nil)
			if err != nil || asset == nil {
				// If we can't parse the asset, use ANSI as fallback
				sqlFilesWithDialects[index] = python.SQLFileInfo{
					FilePath: sqlPath,
					Dialect:  "ansi",
				}
				return
			}

			dialect := python.DetectDialectFromAssetType(string(asset.Type))
			sqlFilesWithDialects[index] = python.SQLFileInfo{
				FilePath: sqlPath,
				Dialect:  dialect,
			}
		})
	}

	assetPool.Wait()

	// Filter out empty entries (from files that didn't exist)
	result := make([]python.SQLFileInfo, 0, len(sqlFilesWithDialects))
	for _, sqlFile := range sqlFilesWithDialects {
		if sqlFile.FilePath != "" {
			result = append(result, sqlFile)
		}
	}

	if len(errorList) > 0 {
		return result, errorList[0] // Return first error if any occurred
	}

	return result, nil
}

func runSqlfluffWithErrorHandling(repoOrAsset, output string, logger interface{}) error {
	infoPrinter.Println("Running sqlfluff...")

	var err error
	// For single asset formatting, we need to handle it differently
	if isPathReferencingAsset(repoOrAsset) {
		// Find the repository root for the single asset
		// Use the directory containing the asset to find the repo
		repoFinder := &git.RepoFinder{}
		assetDir := filepath.Dir(repoOrAsset)
		repo, repoErr := repoFinder.Repo(assetDir)
		if repoErr != nil {
			if output == "json" {
				jsMessage, err := json.Marshal([]error{repoErr})
				if err != nil {
					printErrorJSON(err)
				}
				fmt.Println(string(jsMessage))
				return cli.Exit("", 1)
			}
			errorPrinter.Printf("Error finding repository for sqlfluff: %v\n", repoErr)
			return cli.Exit("", 1)
		}
		err = runSqlfluffFormattingForSingleAsset(repoOrAsset, repo, logger)
	} else {
		err = runSqlfluffFormatting(repoOrAsset, logger)
	}

	if err != nil {
		if output == "json" {
			jsMessage, jsonErr := json.Marshal([]error{err})
			if jsonErr != nil {
				printErrorJSON(jsonErr)
			}
			fmt.Println(string(jsMessage))
			return cli.Exit("", 1)
		}
		errorPrinter.Printf("Error running sqlfluff: %v\n", err)
		return cli.Exit("", 1)
	}

	infoPrinter.Println("Successfully formatted SQL files with sqlfluff.")
	return nil
}
