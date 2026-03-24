package cmd

import (
	"context"
	"fmt"
	path2 "path"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/connection"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/bruin-data/bruin/pkg/notify"
	"github.com/bruin-data/bruin/pkg/secrets"
	"github.com/bruin-data/bruin/pkg/telemetry"
	errors2 "github.com/pkg/errors"
	"github.com/spf13/afero"
	"github.com/urfave/cli/v3"
)

func Notify(isDebug *bool) *cli.Command {
	return &cli.Command{
		Name:      "notify",
		Usage:     "Send a notification via Slack, Discord, MS Teams, or a generic webhook",
		ArgsUsage: "[path to project root, defaults to current folder]",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "type",
				Aliases:  []string{"t"},
				Usage:    "notification type: slack, discord, ms_teams, webhook",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "connection",
				Aliases:  []string{"c"},
				Usage:    "connection name from .bruin.yml or secrets backend",
				Required: true,
			},
			&cli.StringFlag{
				Name:  "channel",
				Usage: "Slack channel to post to (required for slack type)",
			},
			&cli.StringFlag{
				Name:    "message",
				Aliases: []string{"m"},
				Usage:   "notification message",
			},
			&cli.StringFlag{
				Name:  "status",
				Usage: "notification status: success or failure (default: success)",
				Value: "success",
			},
			&cli.StringFlag{
				Name:  "pipeline",
				Usage: "pipeline name to include in the notification",
			},
			&cli.StringFlag{
				Name:  "asset",
				Usage: "asset name to include in the notification",
			},
			&cli.StringFlag{
				Name:  "run-id",
				Usage: "run ID to include in the notification",
			},
			&cli.StringFlag{
				Name:    "environment",
				Aliases: []string{"e", "env"},
				Usage:   "the environment to use",
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
			&cli.StringFlag{
				Name:    "secrets-backend",
				Sources: cli.EnvVars("BRUIN_SECRETS_BACKEND"),
				Usage:   "the source of secrets if different from .bruin.yml. Possible values: 'vault', 'doppler', 'aws', 'azure'",
			},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			defer RecoverFromPanic()

			output := c.String("output")
			notifType := c.String("type")
			connName := c.String("connection")

			if notifType != "slack" && notifType != "discord" && notifType != "ms_teams" && notifType != "webhook" {
				printErrorForOutput(output, fmt.Errorf("unsupported notification type: %s (must be slack, discord, ms_teams, or webhook)", notifType))
				return cli.Exit("", 1)
			}

			if notifType == "slack" && c.String("channel") == "" {
				printErrorForOutput(output, fmt.Errorf("--channel is required for slack notifications"))
				return cli.Exit("", 1)
			}

			status := c.String("status")
			if status != "success" && status != "failure" {
				printErrorForOutput(output, fmt.Errorf("--status must be 'success' or 'failure'"))
				return cli.Exit("", 1)
			}

			path := "."
			if c.Args().Present() {
				path = c.Args().First()
			}

			configFilePath := c.String("config-file")
			if configFilePath == "" {
				repoRoot, err := git.FindRepoFromPath(path)
				if err != nil {
					printErrorForOutput(output, errors2.Wrap(err, "failed to find the git repository root"))
					return cli.Exit("", 1)
				}
				configFilePath = path2.Join(repoRoot.Path, ".bruin.yml")
			}

			cm, err := config.LoadOrCreate(afero.NewOsFs(), configFilePath)
			if err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to load config"))
				return cli.Exit("", 1)
			}

			if env := c.String("environment"); env != "" {
				if err := cm.SelectEnvironment(env); err != nil {
					printErrorForOutput(output, errors2.Wrapf(err, "failed to select environment '%s'", env))
					return cli.Exit("", 1)
				}
			}

			l := makeLogger(*isDebug)

			var connManager config.ConnectionAndDetailsGetter
			var connErrs []error

			secretsBackend := c.String("secrets-backend")
			switch secretsBackend {
			case "vault":
				var sErr error
				connManager, sErr = secrets.NewVaultClientFromEnv(l) //nolint:contextcheck
				if sErr != nil {
					connErrs = append(connErrs, errors2.Wrap(sErr, "failed to initialize vault client"))
				}
			case "doppler":
				var sErr error
				connManager, sErr = secrets.NewDopplerClientFromEnv(l)
				if sErr != nil {
					connErrs = append(connErrs, errors2.Wrap(sErr, "failed to initialize doppler client"))
				}
			case "aws":
				var sErr error
				connManager, sErr = secrets.NewAWSSecretsManagerClientFromEnv(l)
				if sErr != nil {
					connErrs = append(connErrs, errors2.Wrap(sErr, "failed to initialize AWS Secrets Manager client"))
				}
			case "azure":
				var sErr error
				connManager, sErr = secrets.NewAzureKeyVaultClientFromEnv(l)
				if sErr != nil {
					connErrs = append(connErrs, errors2.Wrap(sErr, "failed to initialize Azure Key Vault client"))
				}
			default:
				connManager, connErrs = connection.NewManagerFromConfigWithContext(ctx, cm)
			}

			if len(connErrs) > 0 {
				printErrors(connErrs, output, "failed to initialize connections")
				return cli.Exit("", 1)
			}

			sender, err := buildSender(connManager, notifType, connName, c.String("channel"))
			if err != nil {
				printErrorForOutput(output, err)
				return cli.Exit("", 1)
			}

			payload := notify.Payload{
				Pipeline: c.String("pipeline"),
				Asset:    c.String("asset"),
				RunID:    c.String("run-id"),
				Status:   status,
				Message:  c.String("message"),
			}

			if err := sender.Send(ctx, payload); err != nil {
				printErrorForOutput(output, errors2.Wrap(err, "failed to send notification"))
				return cli.Exit("", 1)
			}

			printSuccessForOutput(output, fmt.Sprintf("Notification sent successfully via %s", notifType))
			return nil
		},
		Before: telemetry.BeforeCommand,
		After:  telemetry.AfterCommand,
	}
}

func buildSender(cm config.ConnectionAndDetailsGetter, notifType, connName, channel string) (notify.Sender, error) {
	switch notifType {
	case "slack":
		// Slack connections store a *slack.Client in availableConnections but we need the
		// raw config to get the API key, so we use GetConnectionDetails instead.
		connDetails := cm.GetConnectionDetails(connName)
		if connDetails == nil {
			return nil, fmt.Errorf("connection '%s' not found", connName)
		}
		slackConn, ok := connDetails.(*config.SlackConnection)
		if !ok {
			return nil, fmt.Errorf("connection '%s' is not a slack connection", connName)
		}
		return notify.NewSlackSender(slackConn.APIKey, channel), nil
	case "discord":
		conn := cm.GetConnection(connName)
		if conn == nil {
			return nil, fmt.Errorf("connection '%s' not found", connName)
		}
		discordConn, ok := conn.(*config.DiscordConnection)
		if !ok {
			return nil, fmt.Errorf("connection '%s' is not a discord connection", connName)
		}
		return notify.NewDiscordSender(discordConn.WebhookURL), nil
	case "ms_teams":
		conn := cm.GetConnection(connName)
		if conn == nil {
			return nil, fmt.Errorf("connection '%s' not found", connName)
		}
		teamsConn, ok := conn.(*config.MSTeamsConnection)
		if !ok {
			return nil, fmt.Errorf("connection '%s' is not an ms_teams connection", connName)
		}
		return notify.NewMSTeamsSender(teamsConn.WebhookURL), nil
	case "webhook":
		conn := cm.GetConnection(connName)
		if conn == nil {
			return nil, fmt.Errorf("connection '%s' not found", connName)
		}
		webhookConn, ok := conn.(*config.WebhookConnection)
		if !ok {
			return nil, fmt.Errorf("connection '%s' is not a webhook connection", connName)
		}
		return notify.NewWebhookSender(webhookConn.URL, webhookConn.Login, webhookConn.Password), nil
	default:
		return nil, fmt.Errorf("unsupported notification type: %s", notifType)
	}
}
