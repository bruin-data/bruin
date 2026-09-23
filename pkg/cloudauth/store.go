package cloudauth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/99designs/keyring"
	"github.com/bruin-data/bruin/pkg/config"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v3"
)

const DefaultAPIURL = "https://cloud.getbruin.com/api/v1"

type Credential struct {
	Token       string    `json:"access_token" yaml:"-"`
	TokenType   string    `json:"token_type" yaml:"-"`
	TokenID     string    `json:"token_id" yaml:"token_id"`
	Account     string    `json:"account" yaml:"account"`
	DefaultTeam string    `json:"default_team" yaml:"default_team"`
	ExpiresAt   time.Time `json:"expires_at" yaml:"expires_at"`
	Abilities   []string  `json:"abilities" yaml:"abilities"`
	Teams       []string  `json:"teams" yaml:"teams"`
	APIURL      string    `json:"-" yaml:"api_url"`
	Key         string    `json:"-" yaml:"credential"`
}

type SecretStore interface {
	Get(string) (keyring.Item, error)
	Set(keyring.Item) error
	Remove(string) error
}

type Store struct {
	Path string
	Open func() (SecretStore, error)
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
	return Store{Path: filepath.Join(root, "bruin", "cloud.yml"), Open: openKeyring}, nil
}

func openKeyring() (SecretStore, error) {
	store, err := keyring.Open(keyring.Config{
		ServiceName:                    "bruin-cloud",
		AllowedBackends:                []keyring.BackendType{keyring.KeychainBackend, keyring.WinCredBackend, keyring.SecretServiceBackend},
		KeychainAccessibleWhenUnlocked: true,
		KeychainSynchronizable:         false,
	})
	if err != nil {
		return nil, errors.New("OS credential store is unavailable; unlock it or use BRUIN_CLOUD_API_KEY")
	}
	return store, nil
}

func (s Store) Metadata() (*Credential, []byte, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var credential Credential
	if err := yaml.Unmarshal(data, &credential); err != nil {
		return nil, nil, errors.New("invalid global Bruin Cloud configuration")
	}
	if credential.Key == "" || credential.APIURL == "" || credential.TokenID == "" {
		return nil, nil, errors.New("incomplete global Bruin Cloud configuration; run 'bruin login oauth --global'")
	}
	return &credential, data, nil
}

func (s Store) Read(apiURL string) (*Credential, error) {
	credential, _, err := s.Metadata()
	if err != nil || credential == nil {
		return nil, err
	}
	if err := s.validateReference(credential); err != nil {
		return nil, err
	}
	if err := CheckDestination(credential.APIURL, apiURL); err != nil {
		return nil, err
	}
	if !credential.ExpiresAt.IsZero() && !time.Now().Before(credential.ExpiresAt) {
		return nil, errors.New("global Bruin Cloud token has expired; run 'bruin login oauth --global'")
	}
	store, err := s.Open()
	if err != nil {
		return nil, err
	}
	item, err := store.Get(credential.Key)
	if err != nil {
		return nil, errors.New("cannot read global token from OS credential store; unlock it or log in again")
	}
	if len(item.Data) == 0 {
		return nil, errors.New("global credential is empty; log in again")
	}
	credential.Token = string(item.Data)
	return credential, nil
}

func (s Store) Save(credential Credential, expected []byte) error {
	if credential.Token == "" || credential.TokenID == "" {
		return errors.New("cannot store an incomplete credential")
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return err
	}
	key, err := s.key(credential.TokenID, credential.APIURL)
	if err != nil {
		return err
	}
	credential.Key = key
	data, err := yaml.Marshal(credential)
	if err != nil {
		return err
	}
	previous, _, err := s.Metadata()
	if err != nil {
		return err
	}
	if previous != nil && previous.Key == key {
		return errors.New("login returned the existing token identity; refusing to overwrite its secret")
	}
	store, err := s.Open()
	if err != nil {
		return err
	}
	if err := store.Set(keyring.Item{Key: key, Data: []byte(credential.Token), Label: "Bruin Cloud personal token", KeychainNotSynchronizable: true}); err != nil {
		return errors.New("could not save token in OS credential store")
	}
	if err := config.WriteCloudFile(afero.NewOsFs(), s.Path, expected, data); err != nil {
		_ = store.Remove(key)
		return err
	}
	if previous != nil && s.validateReference(previous) == nil {
		_ = store.Remove(previous.Key)
	}
	return nil
}

func (s Store) key(tokenID, apiURL string) (string, error) {
	path, err := filepath.Abs(s.Path)
	if err != nil {
		return "", err
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(filepath.Join(parent, filepath.Base(path)) + "\x00" + apiURL + "\x00" + tokenID))
	return "bruin-cloud-" + hex.EncodeToString(sum[:]), nil
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
	credential, _, err := s.Metadata()
	if err != nil || credential == nil {
		return err
	}
	if err := s.validateReference(credential); err != nil {
		return err
	}
	store, err := s.Open()
	if err != nil {
		return err
	}
	if err := store.Remove(credential.Key); err != nil && !errors.Is(err, keyring.ErrKeyNotFound) {
		return errors.New("could not remove token from OS credential store")
	}
	return os.Remove(s.Path)
}

func (s Store) validateReference(credential *Credential) error {
	expected, err := s.key(credential.TokenID, credential.APIURL)
	if err != nil {
		return err
	}
	if credential.Key != expected {
		return errors.New("global credential reference does not belong to this configuration; log in again")
	}
	return nil
}
