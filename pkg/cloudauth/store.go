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
	key         string
}

type SecretStore interface {
	Get(key string) (keyring.Item, error)
	Set(item keyring.Item) error
	Remove(key string) error
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
	return Store{
		Path: filepath.Join(root, "bruin", "cloud.yml"),
		Open: func() (SecretStore, error) {
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
		},
	}, nil
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
	credential := &Credential{
		APIURL:      ref.Connection.APIURL,
		DefaultTeam: cm.GetDefaultTeam(),
		connection:  *ref,
	}
	reference, err := url.Parse(ref.Connection.APIToken)
	if err == nil && reference.Scheme == "keyring" && reference.Host != "" && reference.User == nil && reference.RawQuery == "" && reference.Fragment == "" {
		tokenID := strings.TrimPrefix(reference.Path, "/")
		if tokenID != "" && !strings.Contains(tokenID, "/") {
			credential.key = reference.Host
			credential.TokenID = tokenID
		}
	}
	return credential, data, nil
}

func (s Store) Read(apiURL string) (*Credential, error) {
	credential, _, err := s.Load()
	if err != nil || credential == nil {
		return nil, err
	}
	if err := s.validateReference(credential); err != nil {
		return nil, err
	}
	if err := CheckDestination(credential.APIURL, apiURL); err != nil {
		return nil, err
	}
	store, err := s.Open()
	if err != nil {
		return nil, err
	}
	item, err := store.Get(credential.key)
	if err != nil {
		return nil, errors.New("cannot read global token from OS credential store; unlock it or log in again")
	}
	if len(item.Data) == 0 {
		return nil, errors.New("global credential is empty; log in again")
	}
	credential.Token = string(item.Data)
	credential.APIURL = apiURL
	return credential, nil
}

func (s Store) Save(credential Credential, expected []byte) error {
	if credential.Token == "" || credential.TokenID == "" || credential.APIURL == "" {
		return errors.New("cannot store an incomplete credential")
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return err
	}
	key, err := s.key(credential.TokenID, credential.APIURL)
	if err != nil {
		return err
	}
	previous, _, err := s.Load()
	if err != nil {
		return err
	}
	if previous != nil && previous.key == key {
		return errors.New("login returned the existing token identity; refusing to overwrite its secret")
	}
	ref := config.CloudConnectionRef{Environment: "default", Connection: config.BruinCloudConnection{ConnectionMetadata: config.ConnectionMetadata{Name: "cloud"}}}
	if previous != nil {
		ref = previous.connection
	}
	reference := url.URL{Scheme: "keyring", Host: key, Path: "/" + credential.TokenID}
	ref.Connection.APIToken = reference.String()
	ref.Connection.APIURL = credential.APIURL
	data, err := config.MarshalCloudConnection(expected, ref, credential.DefaultTeam)
	if err != nil {
		return err
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
		_ = store.Remove(previous.key)
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

func (s Store) validateReference(credential *Credential) error {
	if credential.key == "" || credential.TokenID == "" || credential.APIURL == "" {
		return errors.New("global login requires a credential-store reference; run 'bruin cloud login --global --reauth'")
	}
	expected, err := s.key(credential.TokenID, credential.APIURL)
	if err != nil {
		return err
	}
	if credential.key != expected {
		return errors.New("global credential reference does not belong to this configuration; log in again")
	}
	return nil
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
	if err != nil {
		return err
	}
	if credential == nil {
		return errors.New("no global login found")
	}
	if err := s.validateReference(credential); err != nil {
		return err
	}
	store, err := s.Open()
	if err != nil {
		return err
	}
	if err := config.RemoveCloudConnection(afero.NewOsFs(), s.Path, expected, credential.connection); err != nil {
		return err
	}
	if err := store.Remove(credential.key); err != nil && !errors.Is(err, keyring.ErrKeyNotFound) {
		return errors.New("global login removed from configuration, but could not remove token from OS credential store")
	}
	return nil
}
