package cloudauth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/99designs/keyring"
	"gopkg.in/yaml.v3"
)

type ActAsSession struct {
	AgentID     int       `yaml:"agent_id"`
	AgentName   string    `yaml:"agent_name"`
	Team        string    `yaml:"team"`
	ExpiresAt   time.Time `yaml:"expires_at"`
	APIURL      string    `yaml:"api_url"`
	Permissions []string  `yaml:"permissions,omitempty"`
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
	key, err := s.actAsKey()
	if err != nil {
		return nil, err
	}
	store, err := s.Open()
	if err != nil {
		return nil, err
	}
	item, err := store.Get(key)
	if err != nil || len(item.Data) == 0 {
		return nil, errors.New("cannot read the act-as token from the OS credential store; run 'bruin cloud act-as --stop'")
	}
	session.Token = string(item.Data)
	return session, nil
}

func (s Store) SaveActAs(session ActAsSession) error {
	if session.Token == "" || session.AgentID == 0 || session.APIURL == "" {
		return errors.New("cannot store an incomplete act-as session")
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return err
	}
	data, err := yaml.Marshal(session)
	if err != nil {
		return err
	}
	key, err := s.actAsKey()
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
	return nil
}

// ClearActAs removes the session and returns it, or nil when none was active.
func (s Store) ClearActAs() (*ActAsSession, error) {
	session, err := s.LoadActAs()
	if session == nil && err == nil {
		return nil, nil
	}
	key, keyErr := s.actAsKey()
	if keyErr != nil {
		return nil, keyErr
	}
	store, openErr := s.Open()
	if openErr != nil {
		return nil, openErr
	}
	if removeErr := store.Remove(key); removeErr != nil && !errors.Is(removeErr, keyring.ErrKeyNotFound) {
		return nil, errors.New("could not remove the agent token from OS credential store")
	}
	if removeErr := os.Remove(s.ActAsPath()); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return nil, removeErr
	}
	if session == nil {
		session = &ActAsSession{}
	}
	return session, nil
}

func (s Store) actAsKey() (string, error) {
	path, err := filepath.Abs(s.ActAsPath())
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256([]byte(path))
	return "bruin-cloud-act-as-" + hex.EncodeToString(sum[:]), nil
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
