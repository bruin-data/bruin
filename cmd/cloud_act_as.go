package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/urfave/cli/v3"
)

const cloudAuthActAs = "act-as"

func CloudActAs() *cli.Command {
	return actAsCommand(cloudauth.DefaultStore)
}

func actAsCommand(openStore func() (cloudauth.Store, error)) *cli.Command {
	return &cli.Command{
		Name:  "act-as",
		Usage: "Run cloud commands as one of your team's agents, with the agent's permissions",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "agent", Usage: "ID of the agent to act as (see 'bruin cloud agents list')"},
			&cli.BoolFlag{Name: "stop", Usage: "stop acting as an agent and use your own login again"},
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			if c.Bool("stop") && c.IsSet("agent") {
				return errors.New("--agent and --stop cannot be used together")
			}
			store, err := openStore()
			if err != nil {
				return err
			}
			w := c.Root().Writer
			switch {
			case c.Bool("stop"):
				session, err := store.ClearActAs(ctx)
				if err != nil {
					return err
				}
				if session == nil {
					_, _ = fmt.Fprintln(w, "Not acting as an agent.")
					return nil
				}
				_, _ = fmt.Fprintf(w, "Stopped acting as agent %s. Cloud commands use your own login again.\n", actAsLabel(session))
			case c.IsSet("agent"):
				session, permissions, err := fetchActAs(ctx, store, c.Int("agent"), resolveTeam(c))
				if err != nil {
					return fmt.Errorf("could not act as agent %d: %w", c.Int("agent"), err)
				}
				unlock, err := store.LockActAs(ctx)
				if err != nil {
					return err
				}
				defer unlock()
				if err := store.SaveActAs(*session); err != nil {
					return err
				}
				_, _ = fmt.Fprintf(w, "Acting as agent %s on team %s until %s. Cloud commands now use the agent's permissions: %s. Run 'bruin cloud act-as --stop' to go back.\n", actAsLabel(session), session.Team, actAsClock(session.ExpiresAt), strings.Join(permissions, ", "))
			default:
				session, err := store.LoadActAs()
				if err != nil {
					return err
				}
				if session == nil {
					_, _ = fmt.Fprintln(w, "Not acting as an agent.")
					return nil
				}
				_, _ = fmt.Fprintf(w, "Acting as agent %s on team %s until %s.\n", actAsLabel(session), session.Team, actAsClock(session.ExpiresAt))
			}
			return nil
		},
	}
}

// resolveActAs reports ok=false when no session is active, so the caller falls back to the person's own login.
func resolveActAs(c *cli.Command, store cloudauth.Store) (resolvedCloudAuth, bool, error) {
	session, err := store.LoadActAs()
	if err != nil || session == nil {
		return resolvedCloudAuth{}, false, err
	}
	if team := c.String("team"); team != "" && team != session.Team {
		return resolvedCloudAuth{}, true, fmt.Errorf("acting as agent %s is pinned to team %q; run 'bruin cloud act-as --stop' before using --team %q", actAsLabel(session), session.Team, team)
	}
	if err := cloudauth.CheckDestination(session.APIURL, cloudauth.APIURL()); err != nil {
		return resolvedCloudAuth{}, true, errors.New("act-as session belongs to a different Cloud API destination; run 'bruin cloud act-as --stop'")
	}
	if time.Until(session.ExpiresAt) < 2*time.Minute {
		if session, err = renewActAs(store, session); err != nil || session == nil {
			return resolvedCloudAuth{}, err != nil, err
		}
	}
	return resolvedCloudAuth{token: session.Token, source: cloudAuthActAs, team: session.Team}, true, nil
}

// renewActAs saves the new token only if the session wasn't stopped or replaced while it was fetched.
// nolint: contextcheck // auth resolution has no caller context, so the renewal carries its own timeout.
func renewActAs(store cloudauth.Store, old *cloudauth.ActAsSession) (*cloudauth.ActAsSession, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	renewed, _, fetchErr := fetchActAs(ctx, store, old.AgentID, old.Team)
	unlock, err := store.LockActAs(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	current, err := store.LoadActAs()
	if err != nil {
		return nil, err
	}
	if current == nil || current.AgentID != old.AgentID || current.Team != old.Team || current.APIURL != old.APIURL || !current.ExpiresAt.Equal(old.ExpiresAt) {
		return current, nil
	}
	if fetchErr != nil {
		return nil, fmt.Errorf("acting as agent %s ended: %s. Run 'bruin cloud act-as --stop' to use your own login again", actAsLabel(old), strings.TrimRight(fetchErr.Error(), "."))
	}
	return renewed, store.SaveActAs(*renewed)
}

// fetchActAs always authenticates with the person's global login, never a repo connection or agent token.
func fetchActAs(ctx context.Context, store cloudauth.Store, agentID int, team string) (*cloudauth.ActAsSession, []string, error) {
	credential, err := store.Read(cloudauth.APIURL())
	if err != nil {
		return nil, nil, err
	}
	if credential == nil {
		return nil, nil, errors.New("acting as an agent needs your own global login; run 'bruin cloud login --global'")
	}
	if team == "" {
		team = credential.DefaultTeam
	}
	client := bruincloud.NewAPIClient(credential.Token)
	if team != "" {
		client.SetTeam(team)
	}
	result, err := client.ActAsAgent(ctx, agentID)
	if err != nil {
		return nil, nil, err
	}
	if result.Team != "" {
		team = result.Team
	}
	session := &cloudauth.ActAsSession{AgentID: agentID, AgentName: result.Agent.Name, Team: team, APIURL: cloudauth.APIURL(), Token: result.Token, ExpiresAt: result.ExpiresAt}
	return session, result.Permissions, nil
}

func actAsClock(t time.Time) string {
	return t.Local().Format("15:04") //nolint:gosmopolitan // expiry is shown on the user's own clock
}

func actAsLabel(session *cloudauth.ActAsSession) string {
	return fmt.Sprintf("%q (%d)", session.AgentName, session.AgentID)
}
