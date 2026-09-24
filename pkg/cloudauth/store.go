package cloudauth

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

const DefaultAPIURL = "https://cloud.getbruin.com/api/v1"

type Credential struct {
	Token       string    `json:"access_token"`
	TokenType   string    `json:"token_type"`
	TokenID     string    `json:"token_id"`
	Account     string    `json:"account"`
	DefaultTeam string    `json:"default_team"`
	ExpiresAt   time.Time `json:"expires_at"`
	Abilities   []string  `json:"abilities"`
	Teams       []string  `json:"teams"`
	APIURL      string    `json:"-"`
	connection  config.CloudConnectionRef
}

type Store struct {
	Path string
}

func DefaultStore() (Store, error) {
	root := os.Getenv("XDG_CONFIG_HOME")
	if root == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return Store{}, err
		}
		root = filepath.Join(home, ".config")
	}
	if !filepath.IsAbs(root) {
		return Store{}, errors.New("XDG_CONFIG_HOME must be an absolute path")
	}
	return Store{Path: filepath.Join(root, "bruin", "cloud.yml")}, nil
}

func (s Store) Load() (*Credential, []byte, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var cm config.Config
	if err := yaml.Unmarshal(data, &cm); err != nil {
		return nil, nil, errors.New("invalid global Bruin Cloud configuration")
	}
	ref, err := cm.ResolveCloudConnection()
	if err != nil {
		return nil, nil, err
	}
	if ref == nil {
		return nil, data, nil
	}
	return &Credential{
		Token:       ref.Connection.APIToken,
		APIURL:      ref.Connection.APIURL,
		DefaultTeam: cm.GetDefaultTeam(),
		connection:  *ref,
	}, data, nil
}

func (s Store) Read(apiURL string) (*Credential, error) {
	credential, _, err := s.Load()
	if err != nil || credential == nil {
		return nil, err
	}
	if err := CheckDestination(credential.APIURL, apiURL); err != nil {
		return nil, err
	}
	credential.APIURL = apiURL
	return credential, nil
}

func (s Store) Save(credential Credential, expected []byte) error {
	if credential.Token == "" {
		return errors.New("cannot store an empty token")
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return err
	}
	previous, _, err := s.Load()
	if err != nil {
		return err
	}
	ref := config.CloudConnectionRef{Environment: "default", Connection: config.BruinCloudConnection{ConnectionMetadata: config.ConnectionMetadata{Name: "cloud"}}}
	if previous != nil {
		ref = previous.connection
	}
	ref.Connection.APIToken = credential.Token
	ref.Connection.APIURL = credential.APIURL
	data, err := config.MarshalCloudConnection(expected, ref, credential.DefaultTeam)
	if err != nil {
		return err
	}
	return config.WriteCloudFile(afero.NewOsFs(), s.Path, expected, data)
}

func APIURL() string {
	if value := os.Getenv("BRUIN_CLOUD_BASE_URL"); value != "" {
		return strings.TrimRight(value, "/")
	}
	return DefaultAPIURL
}

func CheckDestination(stored, current string) error {
	if stored == "" {
		return nil
	}
	a, err := url.Parse(stored)
	if err != nil {
		return errors.New("invalid stored Cloud API destination")
	}
	b, err := url.Parse(current)
	if err != nil {
		return errors.New("invalid Cloud API destination")
	}
	if a.User != nil || b.User != nil || a.Scheme != b.Scheme || !strings.EqualFold(a.Host, b.Host) || strings.TrimRight(a.Path, "/") != strings.TrimRight(b.Path, "/") || a.RawQuery != b.RawQuery {
		return errors.New("stored token belongs to a different Cloud API destination; log in for the requested destination or supply --api-key")
	}
	return nil
}

func (s Store) Delete() error {
	credential, expected, err := s.Load()
	if err != nil || credential == nil {
		return err
	}
	return config.RemoveCloudConnection(afero.NewOsFs(), s.Path, expected, credential.connection)
}
