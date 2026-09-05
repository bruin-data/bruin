package backfill

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/gofrs/flock"
)

const Version = 1

type Manifest struct {
	Version   int       `json:"version"`
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	Plan      Plan      `json:"plan"`
}

type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Cancelled Status = "cancelled"
)

type Attempt struct {
	RunID      string     `json:"run_id"`
	Status     Status     `json:"status"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Error      string     `json:"error,omitempty"`
}

type Record struct {
	Interval
	Status   Status    `json:"status"`
	Attempts []Attempt `json:"attempts"`
}

// Store writes independent atomic partition snapshots. The OS lock protects a
// backfill from concurrent executors and is automatically released after a crash.
type Store struct{ Dir string }

var safeID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,128}$`)

func Open(root, id string) (*Store, error) {
	if !safeID.MatchString(id) {
		return nil, fmt.Errorf("invalid backfill ID %q", id)
	}
	return &Store{Dir: filepath.Join(root, id)}, nil
}

func (s *Store) Lock() (*flock.Flock, error) {
	if err := os.MkdirAll(s.Dir, 0o700); err != nil {
		return nil, err
	}
	lock := flock.New(filepath.Join(s.Dir, ".lock"))
	ok, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("backfill is already running: %s", s.Dir)
	}
	return lock, nil
}

func (s *Store) Create(m Manifest) error {
	if _, err := os.Stat(filepath.Join(s.Dir, "manifest.json")); err == nil {
		return fmt.Errorf("backfill %s already exists", m.ID)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.MkdirAll(filepath.Join(s.Dir, "partitions"), 0o700); err != nil {
		return err
	}
	return writeJSON(filepath.Join(s.Dir, "manifest.json"), m)
}

func (s *Store) Manifest() (Manifest, error) {
	var m Manifest
	err := readJSON(filepath.Join(s.Dir, "manifest.json"), &m)
	if err != nil {
		return m, err
	}
	if m.Version != Version {
		return m, fmt.Errorf("unsupported backfill store version %d", m.Version)
	}
	if m.ID != filepath.Base(s.Dir) {
		return m, fmt.Errorf("backfill manifest ID does not match its directory")
	}
	return m, m.Plan.Validate()
}

func (s *Store) Read(i Interval) (Record, error) {
	r := Record{Interval: i, Status: Queued, Attempts: []Attempt{}}
	if s == nil {
		return r, nil
	}
	err := readJSON(filepath.Join(s.Dir, "partitions", i.ID+".json"), &r)
	if errors.Is(err, os.ErrNotExist) {
		return Record{Interval: i, Status: Queued, Attempts: []Attempt{}}, nil
	}
	if err != nil {
		return r, err
	}
	if r.ID != i.ID || !r.Start.Equal(i.Start) || !r.End.Equal(i.End) {
		return r, fmt.Errorf("partition record does not match plan: %s", i.ID)
	}
	switch r.Status {
	case Queued, Running, Succeeded, Failed, Cancelled:
	default:
		return r, fmt.Errorf("unknown partition status %q", r.Status)
	}
	return r, nil
}

func (s *Store) Save(r Record) error {
	return writeJSON(filepath.Join(s.Dir, "partitions", r.ID+".json"), r)
}

func readJSON(path string, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	if err = dec.Decode(v); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}
	if err = dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return fmt.Errorf("unexpected trailing data in %s", path)
	}
	return nil
}

func writeJSON(path string, v any) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".backfill-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if err = json.NewEncoder(f).Encode(v); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// Records pages through materialized records only. Queued partitions are implicit
// in the plan, so even a huge mostly-unstarted backfill has a cheap summary.
func (s *Store) Records(ctx context.Context) iter.Seq2[Record, error] {
	return func(yield func(Record, error) bool) {
		if s == nil {
			return
		}
		dir, err := os.Open(filepath.Join(s.Dir, "partitions"))
		if err != nil {
			yield(Record{}, err)
			return
		}
		defer dir.Close()
		for {
			entries, err := dir.ReadDir(128)
			if err != nil && !errors.Is(err, io.EOF) {
				yield(Record{}, err)
				return
			}
			for _, entry := range entries {
				if err := ctx.Err(); err != nil {
					yield(Record{}, err)
					return
				}
				if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
					continue
				}
				var r Record
				if err := readJSON(filepath.Join(s.Dir, "partitions", entry.Name()), &r); err != nil {
					yield(r, err)
					return
				}
				if entry.Name() != r.ID+".json" {
					yield(r, fmt.Errorf("partition filename does not match its ID"))
					return
				}
				if !yield(r, nil) {
					return
				}
			}
			if errors.Is(err, io.EOF) {
				return
			}
		}
	}
}
