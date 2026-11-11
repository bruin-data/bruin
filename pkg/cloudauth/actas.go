package cloudauth

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	"gopkg.in/yaml.v3"
)

type ActAsSession struct {
	AgentID   int       `yaml:"agent_id"`
	AgentName string    `yaml:"agent_name"`
	Team      string    `yaml:"team"`
	APIURL    string    `yaml:"api_url"`
	Token     string    `yaml:"token"`
	ExpiresAt time.Time `yaml:"expires_at"`
}

func (s Store) ActAsPath() string {
	return filepath.Join(filepath.Dir(s.Path), "cloud-act-as.yml")
}

func (s Store) LoadActAs() (*ActAsSession, error) {
	data, err := os.ReadFile(s.ActAsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var session ActAsSession
	if err := yaml.Unmarshal(data, &session); err != nil || session.AgentID == 0 || session.Token == "" {
		return nil, errors.New("invalid act-as session; run 'bruin cloud act-as --stop'")
	}
	return &session, nil
}

// SaveActAs replaces the session file atomically, so a failed write keeps the previous session.
func (s Store) SaveActAs(session ActAsSession) error {
	data, err := yaml.Marshal(session)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp(filepath.Dir(s.Path), ".cloud-act-as-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(file.Name()) }()
	_, err = file.Write(data)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(file.Name(), s.ActAsPath())
}

// LockActAs serialises session changes across bruin processes; reads need no lock.
func (s Store) LockActAs(ctx context.Context) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	lock := flock.New(filepath.Join(filepath.Dir(s.Path), "cloud-act-as.lock"))
	if locked, err := lock.TryLockContext(ctx, 20*time.Millisecond); err != nil || !locked {
		return nil, errors.New("another bruin command is changing the act-as session; try again")
	}
	return func() { _ = lock.Unlock() }, nil
}

func (s Store) ClearActAs(ctx context.Context) (*ActAsSession, error) {
	unlock, err := s.LockActAs(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	session, _ := s.LoadActAs()
	if err := os.Remove(s.ActAsPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return session, nil
}
