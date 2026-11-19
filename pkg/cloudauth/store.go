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

// Store keeps the global login in cloud.yml. The token is written to the file itself (0600 inside a 0700
// directory). Older versions kept the token in the OS credential store and wrote a keyring:// pointer to the
// file instead; such logins are moved into the file the first time they are read.
type Store struct {
	Path string
	// Open opens the OS credential store. It is only used to migrate logins saved by older versions.
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
				return nil, errors.New("OS credential store is unavailable; unlock it to move your existing global login, or log in again")
			}
			return store, nil
		},
	}, nil
}

// Load returns the configured global login without contacting the OS credential store, so a login saved by
// an older version comes back with an empty Token and a credential-store pointer.
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
	if key, tokenID, ok := parseKeyringReference(ref.Connection.APIToken); ok {
		credential.key = key
		credential.TokenID = tokenID
	} else {
		credential.Token = ref.Connection.APIToken
	}
	return credential, data, nil
}

func parseKeyringReference(token string) (string, string, bool) {
	if !strings.HasPrefix(token, "keyring://") {
		return "", "", false
	}
	reference, err := url.Parse(token)
	if err != nil || reference.Host == "" || reference.User != nil || reference.RawQuery != "" || reference.Fragment != "" {
		return "", "", true
	}
	tokenID := strings.TrimPrefix(reference.Path, "/")
	if tokenID == "" || strings.Contains(tokenID, "/") {
		return "", "", true
	}
	return reference.Host, tokenID, true
}

func (s Store) Read(apiURL string) (*Credential, error) {
	credential, data, err := s.Load()
	if err != nil || credential == nil {
		return nil, err
	}
	if err := CheckDestination(credential.APIURL, apiURL); err != nil {
		return nil, err
	}
	if credential.Token == "" {
		if err := s.migrate(credential, data); err != nil {
			return nil, err
		}
	}
	credential.APIURL = apiURL
	return credential, nil
}

// migrate reads a token that an older version kept in the OS credential store, writes it into cloud.yml and
// removes it from the credential store, so later commands no longer need credential-store access.
func (s Store) migrate(credential *Credential, data []byte) error {
	if err := s.validateReference(credential); err != nil {
		return err
	}
	if s.Open == nil {
		return errors.New("cannot read global token from OS credential store; log in again")
	}
	store, err := s.Open()
	if err != nil {
		return err
	}
	item, err := store.Get(credential.key)
	if err != nil {
		return errors.New("cannot read global token from OS credential store; unlock it or log in again")
	}
	if len(item.Data) == 0 {
		return errors.New("global credential is empty; log in again")
	}
	credential.Token = string(item.Data)
	ref := credential.connection
	ref.Connection.APIToken = credential.Token
	updated, err := config.MarshalCloudConnection(data, ref, credential.DefaultTeam)
	if err != nil {
		return err
	}
	// A concurrent change to cloud.yml only postpones the move to the next command; the token is still usable.
	if err := config.WriteCloudFile(afero.NewOsFs(), s.Path, data, updated); err != nil {
		return nil //nolint:nilerr
	}
	_ = store.Remove(credential.key)
	credential.connection = ref
	credential.key = ""
	return nil
}

func (s Store) Save(credential Credential, expected []byte) error {
	if credential.Token == "" || credential.APIURL == "" {
		return errors.New("cannot store an incomplete credential")
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
	if err := config.WriteCloudFile(afero.NewOsFs(), s.Path, expected, data); err != nil {
		return err
	}
	_ = s.removeLegacyToken(previous)
	return nil
}

// removeLegacyToken deletes a token an older version kept in the OS credential store, if it belongs to this file.
func (s Store) removeLegacyToken(credential *Credential) error {
	if credential == nil || credential.key == "" || s.validateReference(credential) != nil || s.Open == nil {
		return nil
	}
	store, err := s.Open()
	if err != nil {
		return err
	}
	if err := store.Remove(credential.key); err != nil && !errors.Is(err, keyring.ErrKeyNotFound) {
		return err
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
		return errors.New("global login has an invalid credential-store reference; run 'bruin cloud login --global --reauth'")
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
	if err := config.RemoveCloudConnection(afero.NewOsFs(), s.Path, expected, credential.connection); err != nil {
		return err
	}
	if err := s.removeLegacyToken(credential); err != nil {
		return errors.New("global login removed from configuration, but could not remove token from OS credential store")
	}
	return nil
}
