package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func Auth() *cli.Command {
	return &cli.Command{Name: "auth", Usage: "Inspect Bruin Cloud authentication", Commands: []*cli.Command{
		{Name: "status", Usage: "Show the active credential source without printing its token", Flags: []cli.Flag{apiKeyFlag(), teamFlag()}, Action: func(_ context.Context, c *cli.Command) error {
			auth, err := resolveCloudAuth(c)
			if err != nil {
				return err
			}
			writer := c.Root().Writer
			_, _ = fmt.Fprintf(writer, "Credential source: %s\n", auth.source)
			if auth.connection != "" {
				_, _ = fmt.Fprintf(writer, "Connection: %s/%s\n", auth.environment, auth.connection)
			}
			team := resolveTeam(c)
			if team == "" && auth.source == cloudAuthGlobal {
				team = auth.team
			}
			if team != "" {
				_, _ = fmt.Fprintf(writer, "Default team: %s\n", team)
			}
			_, _ = fmt.Fprintln(writer, "Local credential found. API validity and permissions are checked by Cloud on each request.")
			return nil
		}},
	}}
}

func Logout() *cli.Command {
	return &cli.Command{Name: "logout", Usage: "Remove a repository or global Bruin Cloud login", Flags: []cli.Flag{
		&cli.BoolFlag{Name: "repo", Usage: "Remove the selected repository bruin connection"},
		&cli.BoolFlag{Name: cloudAuthGlobal, Usage: "Remove the login from the global configuration file"},
		&cli.BoolFlag{Name: "revoke", Usage: "Also revoke the personal token in Cloud"},
	}, Action: func(ctx context.Context, c *cli.Command) error {
		if c.Bool("repo") == c.Bool(cloudAuthGlobal) {
			return errors.New("select exactly one of --repo or --global")
		}
		if c.Bool("repo") {
			if os.Getenv("BRUIN_CONFIG_FILE_CONTENT") != "" {
				return errors.New("unset BRUIN_CONFIG_FILE_CONTENT before removing a repository login")
			}
			path, err := cloudConfigFilePath()
			if err != nil {
				return err
			}
			if err := checkRepoLoginFile(ctx, path); err != nil {
				return err
			}
			expected, err := os.ReadFile(path)
			if errors.Is(err, os.ErrNotExist) {
				return errors.New("no repository login found")
			}
			if err != nil {
				return err
			}
			cm, err := config.LoadFromFileOrEnv(afero.NewOsFs(), path)
			if err != nil {
				return err
			}
			ref, err := cm.ResolveCloudConnection()
			if err != nil {
				return err
			}
			if ref == nil {
				return errors.New("no repository login found")
			}
			if c.Bool("revoke") {
				if err := cloudauth.CheckDestination(ref.Connection.APIURL, cloudauth.APIURL()); err != nil {
					return err
				}
				if err := cloudauth.Revoke(ctx, &cloudauth.Credential{Token: ref.Connection.APIToken, APIURL: cloudauth.APIURL()}); err != nil {
					return err
				}
			}
			if err := config.RemoveCloudConnection(afero.NewOsFs(), path, expected, *ref); err != nil {
				return err
			}
			_, _ = fmt.Fprintln(c.Root().Writer, "Repository login removed. Global credentials will be used if no other repository connection or explicit token takes precedence.")
			return nil
		}
		store, err := cloudauth.DefaultStore()
		if err != nil {
			return err
		}
		if c.Bool("revoke") {
			credential, err := store.Read(cloudauth.APIURL())
			if err != nil {
				return err
			}
			if credential == nil {
				return errors.New("no global login found")
			}
			if err := cloudauth.Revoke(ctx, credential); err != nil {
				return err
			}
		}
		if err := store.Delete(); err != nil {
			return err
		}
		_, _ = fmt.Fprintln(c.Root().Writer, "Global login removed. Repository connections and environment variables are unchanged.")
		return nil
	}}
}
