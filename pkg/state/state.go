package state

import (
	"runtime"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
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
	Parameters map[string]string            `json:"parameters"`
	Metadata   Metadata                     `json:"metadata"`
	State      map[string][]*SchedulerState `json:"state"`
	Version    string                       `json:"version"`
	LastRun    time.Time                    `json:"last_run"`
	LastRunID  string                       `json:"last_run_id"`
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
	ID        string              `json:"id"`
}

type Status string

func NewState(runid string, parameters map[string]string) *State {
	return &State{
		Parameters: parameters,
		Metadata: Metadata{
			Version: version,
			OS:      runtime.GOOS,
		},
		State:     map[string][]*SchedulerState{},
		Version:   "1.0.0",
		LastRun:   time.Time{},
		LastRunID: runid,
	}
}

func (s *State) InitState(foundPipelines []pipeline.Pipeline) error {
	states := make(map[string][]*SchedulerState)
	for _, foundPipeline := range foundPipelines {
		states[foundPipeline.Name] = make([]*SchedulerState, 0)
		for _, asset := range foundPipeline.Assets {
			states[foundPipeline.Name] = append(states[foundPipeline.Name], &SchedulerState{
				Upstream:  asset.Upstreams,
				Status:    StatusPending,
				Error:     "",
				StartTime: time.Time{},
				EndTime:   time.Time{},
				Name:      asset.Name,
				ID:        asset.ID,
			})
		}
	}
	s.State = states
	return nil
}

func (s *State) GetState(name string, pipelineName string) *SchedulerState {
	s.RLock()
	defer s.RUnlock()
	for foundPipelineName, states := range s.State {
		if foundPipelineName == pipelineName {
			for _, state := range states {
				if state.Name == name {
					return state
				}
			}
		}
	}
	return nil
}

func (s *State) SetState(name, pipelineName string, status Status, err error) *SchedulerState {
	s.RLock()
	defer s.RUnlock()
	for foundPipelineName, states := range s.State {
		if foundPipelineName == pipelineName {
			for _, state := range states {
				if state.Name == name {
					state.Status = status
					state.Error = err.Error()
					state.EndTime = time.Now()
					return state
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
