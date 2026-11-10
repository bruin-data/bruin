package cloudauth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/99designs/keyring"
	"github.com/gofrs/flock"
	"gopkg.in/yaml.v3"
)

type ActAsSession struct {
	AgentID     int       `yaml:"agent_id"`
	AgentName   string    `yaml:"agent_name"`
	Team        string    `yaml:"team"`
	ExpiresAt   time.Time `yaml:"expires_at"`
	APIURL      string    `yaml:"api_url"`
	Permissions []string  `yaml:"permissions,omitempty"`
	TokenKey    string    `yaml:"token_key"`
	Token       string    `yaml:"-"`
}

func (s Store) ActAsPath() string {
	return filepath.Join(filepath.Dir(s.Path), "cloud-act-as.yml")
}

// LoadActAs returns the session metadata without touching the credential store.
func (s Store) LoadActAs() (*ActAsSession, error) {
	data, err := os.ReadFile(s.ActAsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var session ActAsSession
	if err := yaml.Unmarshal(data, &session); err != nil || session.AgentID == 0 {
		return nil, errors.New("invalid act-as session; run 'bruin cloud act-as --stop'")
	}
	return &session, nil
}

func (s Store) ReadActAs(apiURL string) (*ActAsSession, error) {
	session, err := s.LoadActAs()
	if err != nil || session == nil {
		return nil, err
	}
	if err := CheckDestination(session.APIURL, apiURL); err != nil {
		return nil, errors.New("act-as session belongs to a different Cloud API destination; run 'bruin cloud act-as --stop'")
	}
	owned, err := s.ownsActAsKey(session.TokenKey)
	if err != nil {
		return nil, err
	}
	if !owned {
		return nil, errors.New("act-as session does not belong to this configuration; run 'bruin cloud act-as --stop'")
	}
	store, err := s.Open()
	if err != nil {
		return nil, err
	}
	item, err := store.Get(session.TokenKey)
	if err != nil || len(item.Data) == 0 {
		return nil, errors.New("cannot read the act-as token from the OS credential store; run 'bruin cloud act-as --stop'")
	}
	session.Token = string(item.Data)
	return session, nil
}

// SaveActAs stages the token under a fresh key so a failed write leaves the previous session usable.
func (s Store) SaveActAs(session ActAsSession) error {
	if session.Token == "" || session.AgentID == 0 || session.APIURL == "" {
		return errors.New("cannot store an incomplete act-as session")
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return err
	}
	previous, _ := s.LoadActAs()
	key, err := s.newActAsKey()
	if err != nil {
		return err
	}
	session.TokenKey = key
	data, err := yaml.Marshal(session)
	if err != nil {
		return err
	}
	store, err := s.Open()
	if err != nil {
		return err
	}
	if err := store.Set(keyring.Item{Key: key, Data: []byte(session.Token), Label: "Bruin Cloud act-as agent token", KeychainNotSynchronizable: true}); err != nil {
		return errors.New("could not save the agent token in OS credential store")
	}
	if err := writePrivateFile(s.ActAsPath(), data); err != nil {
		_ = store.Remove(key)
		return err
	}
	if previous != nil {
		s.removeActAsToken(store, previous.TokenKey)
	}
	return nil
}

// ClearActAs removes the session and returns it, or nil when none was active.
func (s Store) ClearActAs() (*ActAsSession, error) {
	session, err := s.LoadActAs()
	if session == nil && err == nil {
		return nil, nil
	}
	if session != nil && session.TokenKey != "" {
		store, openErr := s.Open()
		if openErr != nil {
			return nil, openErr
		}
		if owned, _ := s.ownsActAsKey(session.TokenKey); owned {
			if removeErr := store.Remove(session.TokenKey); removeErr != nil && !errors.Is(removeErr, keyring.ErrKeyNotFound) {
				return nil, errors.New("could not remove the agent token from OS credential store")
			}
		}
	}
	if removeErr := os.Remove(s.ActAsPath()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return nil, removeErr
	}
	if session == nil {
		session = &ActAsSession{}
	}
	return session, nil
}

// WithActAsLock serialises session changes across bruin processes.
func (s Store) WithActAsLock(ctx context.Context, fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	lock := flock.New(filepath.Join(filepath.Dir(s.Path), "cloud-act-as.lock"))
	locked, err := lock.TryLockContext(ctx, 20*time.Millisecond)
	if err != nil || !locked {
		return errors.New("another bruin command is changing the act-as session; try again")
	}
	defer func() { _ = lock.Unlock() }()
	return fn()
}

func (s Store) removeActAsToken(store SecretStore, key string) {
	if owned, _ := s.ownsActAsKey(key); owned {
		_ = store.Remove(key)
	}
}

func (s Store) newActAsKey() (string, error) {
	prefix, err := s.actAsKeyPrefix()
	if err != nil {
		return "", err
	}
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(suffix), nil
}

func (s Store) ownsActAsKey(key string) (bool, error) {
	prefix, err := s.actAsKeyPrefix()
	if err != nil {
		return false, err
	}
	return strings.HasPrefix(key, prefix) && len(key) > len(prefix), nil
}

func (s Store) actAsKeyPrefix() (string, error) {
	path, err := filepath.Abs(s.ActAsPath())
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(path))
	return "bruin-cloud-act-as-" + hex.EncodeToString(sum[:]) + "-", nil
}

func writePrivateFile(path string, data []byte) error {
	temporary, err := os.CreateTemp(filepath.Dir(path), ".bruin-act-as-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer func() { _ = os.Remove(name) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}
