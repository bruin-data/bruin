package cmd

import (
	"errors"
	"os"

	"github.com/bruin-data/bruin/pkg/cloudauth"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/git"
	"github.com/urfave/cli/v3"
)

const (
	cloudAuthGlobal         = "global"
	defaultCloudEnvironment = "default"
)

type resolvedCloudAuth struct {
	token       string
	source      string
	team        string
	connection  string
	environment string
}

func resolveCloudAuth(c *cli.Command) (resolvedCloudAuth, error) {
	return resolveCloudAuthWithStore(c, func() (*cloudauth.Credential, error) {
		store, err := cloudauth.DefaultStore()
		if err != nil {
			return nil, err
		}
		return store.Read(cloudauth.APIURL())
	})
}

func resolveCloudAuthWithStore(c *cli.Command, global func() (*cloudauth.Credential, error)) (resolvedCloudAuth, error) {
	if token := c.String("api-key"); token != "" {
		return resolvedCloudAuth{token: token, source: "flag or environment"}, nil
	}
	if token := os.Getenv("BRUIN_CLOUD_API_KEY"); token != "" {
		return resolvedCloudAuth{token: token, source: "environment"}, nil
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
