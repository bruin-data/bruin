package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/urfave/cli/v3"
)

type actAsDependencies struct {
	globalStore func() (cloudauth.Store, error)
}

type actAsStatus struct {
	Active      bool        `json:"active"`
	Agent       *actAsAgent `json:"agent,omitempty"`
	Team        string      `json:"team,omitempty"`
	ExpiresAt   *time.Time  `json:"expires_at,omitempty"`
	Permissions []string    `json:"permissions,omitempty"`
}

type actAsAgent struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func CloudActAs() *cli.Command {
	return actAsCommand(actAsDependencies{globalStore: cloudauth.DefaultStore})
}

func actAsCommand(deps actAsDependencies) *cli.Command {
	return &cli.Command{
		Name:  "act-as",
		Usage: "Run cloud commands as one of your team's agents, with the agent's permissions",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "agent", Aliases: []string{"agent-id"}, Usage: "ID of the agent to act as (see 'bruin cloud agents list')"},
			&cli.BoolFlag{Name: "stop", Usage: "stop acting as an agent and use your own login again"},
			outputFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			output := c.String("output")
			writer := c.Root().Writer
			if writer == nil {
				writer = os.Stdout
			}
			err := runActAs(ctx, c, deps, writer, output)
			if err != nil && output == "json" {
				data, _ := json.Marshal(ErrorResponse{Error: err.Error()})
				_, _ = fmt.Fprintln(writer, string(data))
				return cli.Exit("", 1)
			}
			return err
		},
	}
}

func runActAs(ctx context.Context, c *cli.Command, deps actAsDependencies, writer io.Writer, output string) error {
	if c.Args().Present() {
		return errors.New("unexpected argument; use 'bruin cloud act-as --help'")
	}
	agentID := c.Int("agent")
	if c.Bool("stop") && c.IsSet("agent") {
		return errors.New("--agent and --stop cannot be used together")
	}
	if c.IsSet("agent") && agentID <= 0 {
		return fmt.Errorf("--agent must be a positive agent ID, got %d", agentID)
	}
	store, err := deps.globalStore()
	if err != nil {
		return err
	}
	switch {
	case c.Bool("stop"):
		session, err := store.ClearActAs()
		if err != nil {
			return err
		}
		if output == "json" {
			return writeActAsJSON(writer, nil)
		}
		if session == nil {
			_, _ = fmt.Fprintln(writer, "Not acting as an agent.")
			return nil
		}
		_, _ = fmt.Fprintf(writer, "Stopped acting as agent %s. Cloud commands use your own login again.\n", actAsLabel(session))
		return nil
	case c.IsSet("agent"):
		session, err := requestActAs(ctx, store, agentID, resolveTeam(c))
		if err != nil {
			return fmt.Errorf("could not act as agent %d: %w", agentID, err)
		}
		if output == "json" {
			return writeActAsJSON(writer, session)
		}
		permissions := "."
		if len(session.Permissions) > 0 {
			permissions = ": " + strings.Join(session.Permissions, ", ") + "."
		}
		_, _ = fmt.Fprintf(writer, "Acting as agent %s%s until %s. Cloud commands now use the agent's permissions%s Run 'bruin cloud act-as --stop' to go back.\n", actAsLabel(session), actAsTeam(session), actAsClock(session.ExpiresAt), permissions)
		if os.Getenv("BRUIN_CLOUD_API_KEY") != "" {
			_, _ = fmt.Fprintln(writer, "BRUIN_CLOUD_API_KEY is set and takes precedence over acting as the agent.")
		}
		return nil
	default:
		session, err := store.LoadActAs()
		if err != nil {
			return err
		}
		if output == "json" {
			return writeActAsJSON(writer, session)
		}
		if session == nil {
			_, _ = fmt.Fprintln(writer, "Not acting as an agent.")
			return nil
		}
		expiry := actAsClock(session.ExpiresAt)
		if time.Now().After(session.ExpiresAt) {
			_, _ = fmt.Fprintf(writer, "Acting as agent %s%s. The agent token expired at %s and is renewed with your own login on the next cloud command.\n", actAsLabel(session), actAsTeam(session), expiry)
			return nil
		}
		_, _ = fmt.Fprintf(writer, "Acting as agent %s%s until %s.\n", actAsLabel(session), actAsTeam(session), expiry)
		if len(session.Permissions) > 0 {
			_, _ = fmt.Fprintf(writer, "Permissions: %s\n", strings.Join(session.Permissions, ", "))
		}
		return nil
	}
}

func actAsTeam(session *cloudauth.ActAsSession) string {
	if session.Team == "" {
		return ""
	}
	return " on team " + session.Team
}

func writeActAsJSON(writer io.Writer, session *cloudauth.ActAsSession) error {
	status := actAsStatus{}
	if session != nil {
		expiresAt := session.ExpiresAt
		status = actAsStatus{
			Active:      true,
			Agent:       &actAsAgent{ID: session.AgentID, Name: session.AgentName},
			Team:        session.Team,
			ExpiresAt:   &expiresAt,
			Permissions: session.Permissions,
		}
	}
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	_, _ = fmt.Fprintln(writer, string(data))
	return nil
}
