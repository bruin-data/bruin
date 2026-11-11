package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/urfave/cli/v3"
)

const cloudAuthActAs = "act-as"

type actAsStatus struct {
	Active      bool       `json:"active"`
	AgentID     int        `json:"agent_id,omitempty"`
	AgentName   string     `json:"agent_name,omitempty"`
	Team        string     `json:"team,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
	Expired     bool       `json:"expired,omitempty"`
	Permissions []string   `json:"permissions,omitempty"`
	Warning     string     `json:"warning,omitempty"`
}

func CloudActAs() *cli.Command {
	return actAsCommand(cloudauth.DefaultStore)
}

func actAsCommand(openStore func() (cloudauth.Store, error)) *cli.Command {
	return &cli.Command{
		Name:  "act-as",
		Usage: "Run cloud commands as one of your team's agents, with the agent's permissions",
		Flags: []cli.Flag{
			&cli.IntFlag{Name: "agent-id", Usage: "agent ID to act as (see 'bruin cloud agents list')"},
			&cli.BoolFlag{Name: "stop", Usage: "stop acting as an agent and use your own login again"},
			outputFlag(),
		},
		Action: func(ctx context.Context, c *cli.Command) error {
			output := c.String("output")
			if err := runActAs(ctx, c, openStore, output); err != nil {
				printError(err, output, "Act-as failed")
				return cli.Exit("", 1)
			}
			return nil
		},
	}
}

func runActAs(ctx context.Context, c *cli.Command, openStore func() (cloudauth.Store, error), output string) error {
	agentID := c.Int("agent-id")
	if c.IsSet("agent-id") && agentID <= 0 {
		return fmt.Errorf("--agent-id must be a positive integer, got %d", agentID)
	}
	if c.Bool("stop") && c.IsSet("agent-id") {
		return errors.New("--agent-id and --stop cannot be used together")
	}
	store, err := openStore()
	if err != nil {
		return err
	}
	switch {
	case c.Bool("stop"):
		session, err := store.ClearActAs(ctx)
		if err != nil {
			return err
		}
		if output == "json" {
			return printActAsJSON(nil, nil, "")
		}
		if session == nil {
			fmt.Println("Not acting as an agent.")
			return nil
		}
		fmt.Println(actAsStoppedMessage(session) + " Cloud commands use your own login again.")
	case c.IsSet("agent-id"):
		session, permissions, err := fetchActAs(ctx, store, agentID, resolveTeam(c))
		if err != nil {
			if strings.Contains(err.Error(), "agent:act-as") {
				return fmt.Errorf("could not act as agent %d: your login doesn't allow acting as agents; run 'bruin cloud login --global --reauth' and choose read and write, or tick agent:act-as", agentID)
			}
			return fmt.Errorf("could not act as agent %d: %w", agentID, err)
		}
		unlock, err := store.LockActAs(ctx)
		if err != nil {
			return err
		}
		defer unlock()
		_, current, _ := store.LoadActAs()
		if err := store.SaveActAs(*session, current); err != nil {
			return err
		}
		warning := ""
		if os.Getenv("BRUIN_CLOUD_API_KEY") != "" {
			warning = "BRUIN_CLOUD_API_KEY is set and takes precedence over acting as the agent."
		}
		if output == "json" {
			return printActAsJSON(session, permissions, warning)
		}
		if warning != "" {
			printWarningForOutput(output, warning)
		}
		fmt.Printf("Acting as agent %s on team %s until %s. Cloud commands now use the agent's permissions: %s. Run 'bruin cloud act-as --stop' to go back.\n", actAsLabel(session), session.Team, actAsClock(session.ExpiresAt), strings.Join(permissions, ", "))
	default:
		session, _, err := store.LoadActAs()
		if err != nil {
			return err
		}
		if output == "json" {
			return printActAsJSON(session, nil, "")
		}
		switch {
		case session == nil:
			fmt.Println("Not acting as an agent.")
		case time.Now().After(session.ExpiresAt):
			fmt.Printf("Acting as agent %s on team %s; the agent token expired at %s and renews on the next cloud command.\n", actAsLabel(session), session.Team, actAsClock(session.ExpiresAt))
		default:
			fmt.Printf("Acting as agent %s on team %s until %s.\n", actAsLabel(session), session.Team, actAsClock(session.ExpiresAt))
		}
	}
	return nil
}

func printActAsJSON(session *cloudauth.ActAsSession, permissions []string, warning string) error {
	status := actAsStatus{Warning: warning}
	if session != nil {
		status = actAsStatus{Active: true, AgentID: session.AgentID, AgentName: session.AgentName, Team: session.Team, ExpiresAt: &session.ExpiresAt, Expired: time.Now().After(session.ExpiresAt), Permissions: permissions, Warning: warning}
	}
	data, err := json.MarshalIndent(status, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

// resolveActAs reports ok=false when no session is active, so the caller falls back to the person's own login.
func resolveActAs(c *cli.Command, store cloudauth.Store) (resolvedCloudAuth, bool, error) {
	for attempt := 0; ; attempt++ {
		session, raw, err := store.LoadActAs()
		if err != nil || session == nil {
			return resolvedCloudAuth{}, err != nil, err
		}
		if team := c.String("team"); team != "" && team != session.Team {
			return resolvedCloudAuth{}, true, fmt.Errorf("acting as agent %s is pinned to team %q; run 'bruin cloud act-as --stop' before using --team %q", actAsLabel(session), session.Team, team)
		}
		if err := cloudauth.CheckDestination(session.APIURL, cloudauth.APIURL()); err != nil {
			return resolvedCloudAuth{}, true, errors.New("act-as session belongs to a different Cloud API destination; run 'bruin cloud act-as --stop'")
		}
		if time.Until(session.ExpiresAt) >= 2*time.Minute {
			return resolvedCloudAuth{token: session.Token, source: cloudAuthActAs, team: session.Team}, true, nil
		}
		renewed, err := renewActAs(store, session, raw)
		if err != nil {
			return resolvedCloudAuth{}, true, err
		}
		if renewed != nil {
			return resolvedCloudAuth{token: renewed.Token, source: cloudAuthActAs, team: renewed.Team}, true, nil
		}
		if attempt > 0 {
			return resolvedCloudAuth{}, true, errors.New("the act-as session keeps changing; try again")
		}
	}
}

// renewActAs returns nil when the session changed while renewing, so the caller re-checks the new one.
//
//nolint:contextcheck // auth resolution has no caller context, so the renewal carries its own timeout.
func renewActAs(store cloudauth.Store, old *cloudauth.ActAsSession, raw []byte) (*cloudauth.ActAsSession, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	renewed, _, fetchErr := fetchActAs(ctx, store, old.AgentID, old.Team)
	unlock, err := store.LockActAs(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	if _, current, _ := store.LoadActAs(); string(current) != string(raw) {
		return nil, nil
	}
	var apiErr *bruincloud.APIError
	refused := errors.As(fetchErr, &apiErr) && (apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden)
	switch {
	case fetchErr != nil && !refused && time.Now().Before(old.ExpiresAt):
		return old, nil
	case fetchErr != nil:
		return nil, fmt.Errorf("acting as agent %s ended: %s. Run 'bruin cloud act-as --stop' to use your own login again", actAsLabel(old), strings.TrimRight(fetchErr.Error(), "."))
	}
	return renewed, store.SaveActAs(*renewed, raw)
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

func actAsStoppedMessage(session *cloudauth.ActAsSession) string {
	if session.AgentID == 0 {
		return "Stopped acting as an agent."
	}
	return "Stopped acting as agent " + actAsLabel(session) + "."
}
