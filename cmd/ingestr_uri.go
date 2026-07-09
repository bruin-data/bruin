package cmd

import (
	"context"
	"os"
	path2 "path"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/ingestruri"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func IngestrURI() *cli.Command {
	return &cli.Command{
		Name:      "ingestr-uri",
		Usage:     "resolve a connection name to an ingestr URI and write it to a file",
		ArgsUsage: "[output file] [connection name]",
		Description: "Resolves the connection name against the active secrets backend, or " +
			"against .bruin.yml when no backend is selected, and writes the resulting ingestr " +
			"URI to the output file as plaintext, with no trailing newline.\n\n" +
			"The URI contains credentials in plaintext. The file is created with 0600 " +
			"permissions and the command refuses to write to a path that already exists. " +
			"Deleting the file once it has been read is the caller's responsibility.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config-file",
				Usage: "the path to the .bruin.yml file, ignored when a secrets backend is selected",
			},
			&cli.StringFlag{
				Name:  "environment",
				Usage: "the environment to use, ignored when a secrets backend is selected",
			},
			&cli.BoolFlag{
				Name:  "cdc",
				Usage: "rewrite the URI onto its change data capture scheme, e.g. 'postgres+cdc'",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			args := c.Args().Slice()
			if len(args) != 2 {
				printErrorJSON(errors.New("exactly one output file and one connection name are required"))
				return cli.Exit("", 1)
			}
			outputPath, connectionName := args[0], args[1]

			ctx, cm, err := ingestrURIConfig(ctx, c)
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			manager, errs := connectionManagerFromConfig(ctx, cm, makeLogger(false))
			if len(errs) > 0 {
				printErrorJSON(errors.Wrap(errs[0], "failed to create connection manager"))
				return cli.Exit("", 1)
			}

			uri, err := ingestruri.ForConnection(ctx, manager, "", connectionName)
			if err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			uri = ingestruri.Normalize(uri)
			if c.Bool("cdc") {
				uri, err = ingestruri.CDC(uri)
				if err != nil {
					printErrorJSON(errors.Wrapf(err, "connection '%s'", connectionName))
					return cli.Exit("", 1)
				}
			}

			if err := writeSecretFile(outputPath, uri); err != nil {
				printErrorJSON(err)
				return cli.Exit("", 1)
			}

			return nil
		},
	}
}

// ingestrURIConfig loads .bruin.yml, which is only needed when connections are
// read from it. A secrets backend supplies its own connections, so requiring a
// config file there would break deployments that ship without one.
func ingestrURIConfig(ctx context.Context, c *cli.Command) (context.Context, *config.Config, error) {
	if secretsBackendFromContext(ctx) != "" {
		return ctx, &config.Config{}, nil
	}

	configFilePath := c.String("config-file")
	if configFilePath == "" {
		repoRoot, err := git.FindRepoFromPath(".")
		if err != nil {
			return ctx, nil, errors.Wrap(err, "failed to find the git repository root")
		}
		configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
	}

	cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
	if err != nil {
		return ctx, nil, errors.Wrap(err, "failed to load or create config")
	}

	environment := c.String("environment")
	if environment == "" {
		environment = cm.DefaultEnvironmentName
	}
	if err := cm.SelectEnvironment(environment); err != nil {
		return ctx, nil, errors.Wrap(err, "failed to select the environment")
	}

	ctx = context.WithValue(ctx, config.ConfigFilePathContextKey, configFilePath)
	ctx = context.WithValue(ctx, config.EnvironmentNameContextKey, cm.SelectedEnvironmentName)

	return ctx, cm, nil
}

// writeSecretFile writes uri to path, owner-readable only.
//
// O_EXCL rejects an existing path so that a pre-planted symlink cannot redirect
// the credentials somewhere world-readable, and the mode is set at creation so
// the file is never briefly readable by others.
//
// On Windows the mode only maps to the read-only bit; the file inherits the
// directory ACL instead.
func writeSecretFile(path string, uri string) error {
	contents := []byte(uri)

	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return errors.Wrapf(err, "failed to create the output file '%s'", path)
	}

	if _, err := file.Write(contents); err != nil {
		file.Close()
		os.Remove(path)
		return errors.Wrapf(err, "failed to write the output file '%s'", path)
	}

	if err := file.Close(); err != nil {
		os.Remove(path)
		return errors.Wrapf(err, "failed to close the output file '%s'", path)
	}

	return nil
}
