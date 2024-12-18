package state

import (
	"runtime"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/scheduler"
)

var version = "dev"

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
	StatusSkipped Status = "skipped"
)

type State struct {
	sync.RWMutex
	Parameters map[string]string                     `json:"parameters"`
	Metadata   Metadata                              `json:"metadata"`
	State      map[string][]*scheduler.AssetInstance `json:"state"`
	Version    string                                `json:"version"`
	TimeStamp  time.Time                             `json:"timestamp"`
	RunID      string                                `json:"run_id"`
}

type Metadata struct {
	Version string `json:"version"`
	OS      string `json:"os"`
}

type Status string

func NewState(runid string, parameters map[string]string) *State {
	return &State{
		Parameters: parameters,
		Metadata: Metadata{
			Version: version,
			OS:      runtime.GOOS,
		},
		State:     map[string][]*scheduler.AssetInstance{},
		Version:   "1.0.0",
		TimeStamp: time.Time{},
		RunID:     runid,
	}
}

func (s *State) GetAssetState(name string, pipelineName string) *scheduler.AssetInstance {
	s.RLock()
	defer s.RUnlock()
	for foundPipelineName, states := range s.State {
		if foundPipelineName == pipelineName {
			for _, state := range states {
				if state.Asset.Name == name {
					return state
				}
			}
		}
	}
	return nil
}

func (s *State) GetPipelineState(name string) []*scheduler.AssetInstance {
	s.RLock()
	defer s.RUnlock()
	for foundPipelineName, states := range s.State {
		if foundPipelineName == name {
			return states
		}
	}
	return []*scheduler.AssetInstance{}
}

func (s *State) SetAssetState(name, pipelineName string, instance *scheduler.AssetInstance) error {
	s.RLock()
	defer s.RUnlock()
	for foundPipelineName, states := range s.State {
		if foundPipelineName == pipelineName {
			for key, state := range states {
				if state.Asset.Name == name {
					s.State[pipelineName][key] = instance
					return nil
				}
			}
		}
	}
	return nil
}

func (s *State) SaveState(stateFileDir string) error {
	// TODO: Implement state saving
	return nil
}

func (s *State) CompareState() error {
	// TODO: Implement state saving
	return nil
}
