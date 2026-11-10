package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/bruincloud"
	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/urfave/cli/v3"
)

const (
	cloudAuthGlobal         = "global"
	cloudAuthActAs          = "act-as"
	defaultCloudEnvironment = "default"
	actAsRefreshMargin      = 2 * time.Minute
)

var errActAsNeedsGlobalLogin = errors.New("acting as an agent needs your own global login; run 'bruin cloud login --global'")

type resolvedCloudAuth struct {
	token       string
	source      string
	team        string
	connection  string
	environment string
	actAs       *cloudauth.ActAsSession
}

func (a resolvedCloudAuth) teamFor(c *cli.Command) string {
	if a.source == cloudAuthActAs {
		return a.team
	}
	team := resolveTeam(c)
	if team == "" && a.source == cloudAuthGlobal {
		team = a.team
	}
	return team
}

func resolveCloudAuth(c *cli.Command) (resolvedCloudAuth, error) {
	return resolveCloudAuthWithActAs(c, cloudauth.DefaultStore)
}

func resolveCloudAuthWithActAs(c *cli.Command, openStore func() (cloudauth.Store, error)) (resolvedCloudAuth, error) {
	if auth, ok := explicitCloudAuth(c); ok {
		return auth, nil
	}
	store, err := openStore()
	if err != nil {
		return resolveCloudAuthWithStore(c, func() (*cloudauth.Credential, error) { return nil, err })
	}
	session, err := store.LoadActAs()
	if err != nil {
		return resolvedCloudAuth{}, err
	}
	if session != nil {
		return resolveActAs(c, store, session)
	}
	return resolveCloudAuthWithStore(c, func() (*cloudauth.Credential, error) { return store.Read(cloudauth.APIURL()) })
}

func explicitCloudAuth(c *cli.Command) (resolvedCloudAuth, bool) {
	if token := c.String("api-key"); token != "" {
		return resolvedCloudAuth{token: token, source: "flag or environment"}, true
	}
	if token := os.Getenv("BRUIN_CLOUD_API_KEY"); token != "" {
		return resolvedCloudAuth{token: token, source: "environment"}, true
	}
	return resolvedCloudAuth{}, false
}

func resolveActAs(c *cli.Command, store cloudauth.Store, session *cloudauth.ActAsSession) (resolvedCloudAuth, error) {
	if team := c.String("team"); team != "" && team != session.Team {
		return resolvedCloudAuth{}, fmt.Errorf("acting as agent %s is pinned to team %q; run 'bruin cloud act-as --stop' before using --team %q", actAsLabel(session), session.Team, team)
	}
	if err := cloudauth.CheckDestination(session.APIURL, cloudauth.APIURL()); err != nil {
		return resolvedCloudAuth{}, errors.New("act-as session belongs to a different Cloud API destination; run 'bruin cloud act-as --stop'")
	}
	var err error
	if time.Until(session.ExpiresAt) < actAsRefreshMargin {
		session, err = refreshActAs(store, session)
	} else {
		session, err = store.ReadActAs(cloudauth.APIURL())
	}
	if err != nil {
		return resolvedCloudAuth{}, err
	}
	return resolvedCloudAuth{token: session.Token, source: cloudAuthActAs, team: session.Team, actAs: session}, nil
}

// nolint: contextcheck // auth resolution has no caller context, so the refresh carries its own timeout.
func refreshActAs(store cloudauth.Store, session *cloudauth.ActAsSession) (*cloudauth.ActAsSession, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	refreshed, err := requestActAs(ctx, store, session.AgentID, session.Team)
	if err != nil {
		return nil, fmt.Errorf("acting as agent %s ended: %s. Run 'bruin cloud act-as --stop' to use your own login again", actAsLabel(session), strings.TrimRight(err.Error(), "."))
	}
	return refreshed, nil
}

// requestActAs always authenticates with the person's global login, never a repo connection or agent token.
func requestActAs(ctx context.Context, store cloudauth.Store, agentID int, team string) (*cloudauth.ActAsSession, error) {
	credential, err := store.Read(cloudauth.APIURL())
	if err != nil {
		return nil, err
	}
	if credential == nil {
		return nil, errActAsNeedsGlobalLogin
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
		return nil, err
	}
	if result.Token == "" {
		return nil, errors.New("the act-as response contained no agent token")
	}
	session := cloudauth.ActAsSession{
		AgentID:     agentID,
		AgentName:   result.Agent.Name,
		Team:        result.Team,
		ExpiresAt:   result.ExpiresAt,
		APIURL:      cloudauth.APIURL(),
		Permissions: result.Permissions,
		Token:       result.Token,
	}
	if session.Team == "" {
		session.Team = team
	}
	if err := store.SaveActAs(session); err != nil {
		return nil, err
	}
	return &session, nil
}

func actAsClock(t time.Time) string {
	return t.Local().Format("15:04") //nolint:gosmopolitan // expiry is shown on the user's own clock
}

func actAsLabel(session *cloudauth.ActAsSession) string {
	if session.AgentID == 0 {
		return "(unknown)"
	}
	if session.AgentName == "" {
		return strconv.Itoa(session.AgentID)
	}
	return fmt.Sprintf("%q (%d)", session.AgentName, session.AgentID)
}

func resolveCloudAuthWithStore(c *cli.Command, global func() (*cloudauth.Credential, error)) (resolvedCloudAuth, error) {
	if auth, ok := explicitCloudAuth(c); ok {
		return auth, nil
	}
	cm, err := loadCloudConfig()
	if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, git.ErrNoGitRepoFound) {
		return resolvedCloudAuth{}, err
	}
	if cm != nil {
		ref, err := cm.ResolveCloudConnection()
		if err != nil {
			return resolvedCloudAuth{}, err
		}
		if ref != nil {
			if err := cloudauth.CheckDestination(ref.Connection.APIURL, cloudauth.APIURL()); err != nil {
				return resolvedCloudAuth{}, err
			}
			return resolvedCloudAuth{token: ref.Connection.APIToken, source: "repo", connection: ref.Connection.Name, environment: ref.Environment}, nil
		}
	}
	credential, err := global()
	if err != nil {
		return resolvedCloudAuth{}, err
	}
	if credential != nil {
		return resolvedCloudAuth{token: credential.Token, source: cloudAuthGlobal, team: credential.DefaultTeam}, nil
	}
	return resolvedCloudAuth{}, errors.New("API key is required: run 'bruin cloud login', use --api-key or BRUIN_CLOUD_API_KEY, or configure a bruin connection in .bruin.yml")
}

func emptyCloudConfig() *config.Config {
	return &config.Config{DefaultEnvironmentName: defaultCloudEnvironment, Environments: map[string]config.Environment{}}
}
