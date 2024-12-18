package state

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

var (
	version = "dev"
)

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
	StatusSkip    Status = "skiped"
)

type State struct {
	sync.RWMutex
	Parameters map[string]string `json:"parameters"`
	Metadata   Metadata          `json:"metadata"`
	State      []*SchedulerState `json:"state"`
	Version    string            `json:"version"`
	LastRun    time.Time         `json:"last_run"`
	LastRunID  string            `json:"last_run_id"`
}

type Metadata struct {
	Version string `json:"version"`
	OS      string `json:"os"`
}

type SchedulerState struct {
	Upstream  []pipeline.Upstream `json:"upstream"`
	Status    Status              `json:"status"`
	Error     string              `json:"error"`
	StartTime time.Time           `json:"start_time"`
	EndTime   time.Time           `json:"end_time"`
	Name      string              `json:"name"`
	Id        string              `json:"id"`
}

type Status string

func createStateFile(runid string, stateFileDir string) (*os.File, error) {
	if _, err := os.Stat(stateFileDir); os.IsNotExist(err) {
		os.Mkdir(stateFileDir, 0755)
	}
	timestamp := time.Now().Format(time.RFC3339)
	filePath := fmt.Sprintf("%s/%s_%s.json", stateFileDir, timestamp, runid)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %v", err)
	}
	return file, nil
}

func NewState(runid string, parameters map[string]string, stateFileDir string, foundPipelines []pipeline.Pipeline) *State {
	states := []*SchedulerState{}
	for _, pipeline := range foundPipelines {
		for _, asset := range pipeline.Assets {
			states = append(states, &SchedulerState{
				Upstream:  asset.Upstreams,
				Status:    StatusPending,
				Error:     "",
				StartTime: time.Time{},
				EndTime:   time.Time{},
				Name:      asset.Name,
				Id:        asset.ID,
			})
		}
	}
	return &State{
		Parameters: parameters,
		Metadata: Metadata{
			Version: version,
			OS:      runtime.GOOS,
		},
		State:     states,
		Version:   "1.0.0",
		LastRun:   time.Time{},
		LastRunID: runid,
	}
}

func (s *State) GetState(name string) *SchedulerState {
	s.RLock()
	defer s.RUnlock()
	for _, state := range s.State {
		if state.Name == name {
			return state
		}
	}
	return nil
}

func (s *State) SetState(name string, status Status, error string) *SchedulerState {
	s.Lock()
	defer s.Unlock()
	for _, state := range s.State {
		if state.Name == name {
			state.Status = status
			state.Error = error
			return state
		}
	}
	return nil
}

func (s *State) SaveState(stateFileDir string) error {
	s.RLock()
	defer s.RUnlock()
	file, err := createStateFile(s.LastRunID, stateFileDir)
	if err != nil {
		slog.Error("failed to create state file", "error", err)
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(s); err != nil {
		slog.Error("failed to encode state to JSON", "error", err)
		return err
	}

	return nil
}
