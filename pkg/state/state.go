package state

import (
	"runtime"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/scheduler"
)

var version = "dev"

type State struct {
	sync.RWMutex
	Parameters map[string]string          `json:"parameters"`
	Metadata   Metadata                   `json:"metadata"`
	State      []*scheduler.AssetInstance `json:"state"`
	Version    string                     `json:"version"`
	TimeStamp  time.Time                  `json:"timestamp"`
	RunID      string                     `json:"run_id"`
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
		State:     []*scheduler.AssetInstance{},
		Version:   "1.0.0",
		TimeStamp: time.Time{},
		RunID:     runid,
	}
}
