package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	lineagepackage "github.com/bruin-data/bruin/pkg/lineage"
	"github.com/bruin-data/bruin/pkg/path"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/bruin-data/bruin/pkg/sqlparser"
	color2 "github.com/fatih/color"
	errors2 "github.com/pkg/errors"
	"github.com/sourcegraph/conc"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v2"
)

func Internal() *cli.Command {
	return &cli.Command{
		Name:   "internal",
		Hidden: true,
		Subcommands: []*cli.Command{
			ParseAsset(),
			ParsePipeline(),
			PatchAsset(),
			ConnectionSchemas(),
		},
	}
}

func ParseAsset() *cli.Command {
	return &cli.Command{
		Name:      "parse-asset",
		Usage:     "parse a single Bruin asset",
		ArgsUsage: "[path to the asset definition]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "column-lineage",
				Aliases:     []string{"c"},
				Usage:       "return the column lineage for the given asset",
				Required:    false,
				DefaultText: "false",
			},
		},
		Action: func(c *cli.Context) error {
			r := ParseCommand{
				builder:      DefaultPipelineBuilder,
				errorPrinter: errorPrinter,
			}

			return r.Run(c.Context, c.Args().Get(0), c.Bool("column-lineage"))
		},
	}
}

func ParsePipeline() *cli.Command {
	return &cli.Command{
		Name:      "parse-pipeline",
		Usage:     "parse a full Bruin pipeline",
		ArgsUsage: "[path to the any asset or anywhere in the pipeline]",
		Flags: []cli.Flag{
			&cli.BoolFlag{
				Name:        "column-lineage",
				Aliases:     []string{"c"},
				Usage:       "return the column lineage for the given asset",
				Required:    false,
				DefaultText: "false",
			},
			&cli.BoolFlag{
				Name:        "exp-slim-response",
				Usage:       "experimental flag to return a slim response",
				Required:    false,
				DefaultText: "false",
			},
		},
		Action: func(c *cli.Context) error {
			r := ParseCommand{
				builder:      DefaultPipelineBuilder,
				errorPrinter: errorPrinter,
			}

			return r.ParsePipeline(c.Context, c.Args().Get(0), c.Bool("column-lineage"), c.Bool("exp-slim-response"))
		},
	}
}

func ConnectionSchemas() *cli.Command {
	return &cli.Command{
		Name:  "connections",
		Usage: "return all the possible connection types and their schemas",
		Action: func(c *cli.Context) error {
			jsonStringSchema, err := config.GetConnectionsSchema()
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			fmt.Println(jsonStringSchema)
			return nil
		},
	}
}

type ParseCommand struct {
	builder      taskCreator
	errorPrinter *color2.Color
}

func (r *ParseCommand) ParsePipeline(ctx context.Context, assetPath string, lineage bool, slimResponse bool) error {
	// defer RecoverFromPanic()
	var lineageWg conc.WaitGroup
	var sqlParser *sqlparser.SQLParser

	if lineage {
		lineageWg.Go(func() { //nolint:contextcheck
			var err error
			sqlParser, err = sqlparser.NewSQLParser(false)
			if err != nil {
				printErrorJSON(err)
				panic(err)
			}

			err = sqlParser.Start()
			if err != nil {
				printErrorJSON(err)
				panic(err)
			}
		})
	}

	if assetPath == "" {
		errorPrinter.Printf("Please give an asset path to parse: bruin render <path to the asset file>)\n")
		return cli.Exit("", 1)
	}

	pipelinePath, err := path.GetPipelineRootFromTask(assetPath, PipelineDefinitionFiles)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	foundPipeline, err := DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelinePath, pipeline.WithMutate())
	if err != nil {
		printErrorJSON(err)

		return cli.Exit("", 1)
	}

	if lineage {
		panics := lineageWg.WaitAndRecover()
		if panics != nil {
			return cli.Exit("", 1)
		}

		issues := &lineagepackage.LineageError{
			Issues:   make([]*lineagepackage.LineageIssue, 0),
			Pipeline: foundPipeline,
		}

		defer sqlParser.Close()
		processedAssets := make(map[string]bool)
		lineage := lineagepackage.NewLineageExtractor(sqlParser)
		for _, asset := range foundPipeline.Assets {
			errIssues := lineage.ColumnLineage(foundPipeline, asset, processedAssets)
			if errIssues != nil {
				issues.Issues = append(issues.Issues, errIssues.Issues...)
			}
		}
	}

	foundPipeline.WipeContentOfAssets()

	if !slimResponse {
		js, err := json.Marshal(foundPipeline)
		if err != nil {
			printErrorJSON(err)
			return cli.Exit("", 1)
		}

		fmt.Println(string(js))
		return nil
	}

	type assetSummary struct {
		ID             string                       `json:"id"`
		Name           string                       `json:"name"`
		Type           pipeline.AssetType           `json:"type"`
		ExecutableFile *pipeline.ExecutableFile     `json:"executable_file"`
		DefinitionFile *pipeline.TaskDefinitionFile `json:"definition_file"`
		Upstreams      []pipeline.Upstream          `json:"upstreams"`
	}

	type pipelineSummary struct {
		*pipeline.Pipeline
		Assets []*assetSummary `json:"assets"`
	}

	ps := pipelineSummary{
		Pipeline: foundPipeline,
		Assets:   make([]*assetSummary, len(foundPipeline.Assets)),
	}

	for i, asset := range foundPipeline.Assets {
		ps.Assets[i] = &assetSummary{
			ID:             asset.ID,
			Name:           asset.Name,
			Type:           asset.Type,
			ExecutableFile: &asset.ExecutableFile,
			DefinitionFile: &asset.DefinitionFile,
			Upstreams:      asset.Upstreams,
		}
	}

	js, err := json.Marshal(ps)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	fmt.Println(string(js))

	return err
}

func (r *ParseCommand) Run(ctx context.Context, assetPath string, lineage bool) error {
	defer RecoverFromPanic()

	var lineageWg conc.WaitGroup
	var sqlParser *sqlparser.SQLParser

	if lineage {
		lineageWg.Go(func() { //nolint:contextcheck
			var err error
			sqlParser, err = sqlparser.NewSQLParser(false)
			if err != nil {
				printErrorJSON(err)
				panic(err)
			}

			err = sqlParser.Start()
			if err != nil {
				printErrorJSON(err)
				panic(err)
			}
		})
	}

	if assetPath == "" {
		errorPrinter.Printf("Please give an asset path to parse: bruin render <path to the asset file>)\n")
		return cli.Exit("", 1)
	}

	absoluteAssetPath, err := filepath.Abs(assetPath)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	pipelinePath, err := path.GetPipelineRootFromTask(absoluteAssetPath, PipelineDefinitionFiles)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	repoRoot, err := git.FindRepoFromPath(absoluteAssetPath)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	pipelineDefinitionPath, err := getPipelineDefinitionFullPath(pipelinePath)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	var foundPipeline *pipeline.Pipeline
	asset, err := DefaultPipelineBuilder.CreateAssetFromFile(absoluteAssetPath, nil)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}
	// column-level lineage requires the whole pipeline to be parsed by nature, therefore we need to use the default pipeline builder.
	// however, since the primary usecase of this command requires speed, we'll use a faster alternative if there's no lineage requested.
	if lineage {
		foundPipeline, err = DefaultPipelineBuilder.CreatePipelineFromPath(ctx, pipelineDefinitionPath, pipeline.WithMutate())
	} else {
		foundPipeline, err = pipeline.PipelineFromPath(pipelineDefinitionPath, afero.NewOsFs())
		if err != nil {
			printErrorJSON(err)
			return cli.Exit("", 1)
		}
		err = DefaultPipelineBuilder.SetAssetColumnFromGlossary(asset, pipelineDefinitionPath)
	}

	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	asset, err = DefaultPipelineBuilder.MutateAsset(ctx, asset, foundPipeline)
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	type lineageIssueSummary struct {
		Asset string `json:"name"`
		Error string `json:"error"`
	}

	lineageIssues := make([]*lineageIssueSummary, 0)

	if lineage {
		panics := lineageWg.WaitAndRecover()
		if panics != nil {
			return cli.Exit("", 1)
		}

		processedAssets := make(map[string]bool)
		lineageExtractor := lineagepackage.NewLineageExtractor(sqlParser)
		lineageErrors := lineageExtractor.ColumnLineage(foundPipeline, asset, processedAssets)
		if lineageErrors != nil {
			for _, issue := range lineageErrors.Issues {
				lineageIssues = append(lineageIssues, &lineageIssueSummary{
					Asset: issue.Task.Name,
					Error: issue.Description,
				})
			}
		}
	}

	type pipelineSummary struct {
		Name     string            `json:"name"`
		Schedule pipeline.Schedule `json:"schedule"`
	}

	js, err := json.Marshal(struct {
		Asset         *pipeline.Asset        `json:"asset"`
		Pipeline      pipelineSummary        `json:"pipeline"`
		Repo          *git.Repo              `json:"repo"`
		LineageIssues []*lineageIssueSummary `json:"lineage_issues,omitempty"`
	}{
		Asset: asset,
		Pipeline: pipelineSummary{
			Name:     foundPipeline.Name,
			Schedule: foundPipeline.Schedule,
		},
		LineageIssues: lineageIssues,
		Repo:          repoRoot,
	})
	if err != nil {
		printErrorJSON(err)
		return cli.Exit("", 1)
	}

	fmt.Println(string(js))

	return err
}

func PatchAsset() *cli.Command {
	return &cli.Command{
		Name:      "patch-asset",
		Usage:     "patch a single Bruin asset with the given fields",
		ArgsUsage: "[path to the asset definition]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "body",
				Usage:    "the JSON object containing the patch body",
				Required: false,
			},
			&cli.BoolFlag{
				Name:        "convert",
				Usage:       "convert a SQL or Python file into a Bruin asset",
				Required:    false,
				DefaultText: "false",
			},
		},
		Action: func(c *cli.Context) error {
			assetPath := c.Args().Get(0)
			if assetPath == "" {
				printErrorJSON(errors.New("empty asset path given, you must provide an existing asset path"))
				return cli.Exit("", 1)
			}

			if c.Bool("convert") {
				return convertToBruinAsset(afero.NewOsFs(), assetPath)
			}

			asset, err := DefaultPipelineBuilder.CreateAssetFromFile(assetPath, nil)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to create asset from the given path"))
				return cli.Exit("", 1)
			}

			asset, err = DefaultPipelineBuilder.MutateAsset(c.Context, asset, nil)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to patch the asset with the given json body"))
				return cli.Exit("", 1)
			}

			if asset == nil {
				printErrorJSON(errors2.New("the file in the given path does not seem to be an asset"))
				return cli.Exit("", 1)
			}

			err = json.Unmarshal([]byte(c.String("body")), &asset)
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to patch the asset with the given json body"))
				return cli.Exit("", 1)
			}

			err = asset.Persist(afero.NewOsFs())
			if err != nil {
				printErrorJSON(errors2.Wrap(err, "failed to save the asset to the file"))
				return cli.Exit("", 1)
			}

			return nil
		},
	}
}

func convertToBruinAsset(fs afero.Fs, filePath string) error {
	// Check if file exists
	exists, err := afero.Exists(fs, filePath)
	if err != nil {
		printErrorJSON(errors2.Wrap(err, "failed to check if file exists"))
		return cli.Exit("", 1)
	}
	if !exists {
		printErrorJSON(errors.New("file does not exist"))
		return cli.Exit("", 1)
	}

	content, err := afero.ReadFile(fs, filePath)
	if err != nil {
		printErrorJSON(errors2.Wrap(err, "failed to read file"))
		return cli.Exit("", 1)
	}

	fileName := filepath.Base(filePath)
	assetName := strings.TrimSuffix(fileName, filepath.Ext(fileName))

	var bruinHeader string
	ext := strings.ToLower(filepath.Ext(filePath))

	switch ext {
	case ".sql":
		bruinHeader = fmt.Sprintf("/* @bruin\nname: %s\ntype: bq.sql\n@bruin */\n\n", assetName)
	case ".py":
		bruinHeader = fmt.Sprintf("\"\"\" @bruin\nname: %s\n@bruin \"\"\"\n\n", assetName)
	default:
		return nil // unsupported file types
	}
	newContent := bruinHeader + string(content)
	err = afero.WriteFile(fs, filePath, []byte(newContent), 0o644)
	if err != nil {
		printErrorJSON(errors2.Wrap(err, "failed to write file"))
		return cli.Exit("", 1)
	}

	return nil
}
