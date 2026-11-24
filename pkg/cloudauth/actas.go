package cloudauth

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/gofrs/flock"
	"github.com/spf13/afero"
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

// LoadActAs also returns the raw file so a later save can detect concurrent changes.
func (s Store) LoadActAs() (*ActAsSession, []byte, error) {
	data, err := os.ReadFile(s.ActAsPath())
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var session ActAsSession
	if err := yaml.Unmarshal(data, &session); err != nil || session.AgentID == 0 || session.Token == "" || session.APIURL == "" {
		return nil, data, errors.New("invalid act-as session; run 'bruin cloud act-as --stop'")
	}
	return &session, data, nil
}

// SaveActAs fails without writing if the file no longer matches expected.
func (s Store) SaveActAs(session ActAsSession, expected []byte) error {
	data, err := yaml.Marshal(session)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return err
	}
	return config.WriteCloudFile(afero.NewOsFs(), s.ActAsPath(), expected, data)
}

// LockActAs serialises session changes across bruin processes; reads need no lock.
func (s Store) LockActAs(ctx context.Context) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0o700); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	lock := flock.New(filepath.Join(filepath.Dir(s.Path), "cloud-act-as.lock"))
	if _, err := lock.TryLockContext(ctx, 20*time.Millisecond); err != nil {
		return nil, fmt.Errorf("could not lock the act-as session: %w", err)
	}
	return func() { _ = lock.Unlock() }, nil
}

// ClearActAs removes the session file even when it is corrupt, returning an empty session in that case.
func (s Store) ClearActAs(ctx context.Context) (*ActAsSession, error) {
	unlock, err := s.LockActAs(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	session, data, _ := s.LoadActAs()
	if session == nil && data != nil {
		session = &ActAsSession{}
	}
	if err := os.Remove(s.ActAsPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return session, nil
}
