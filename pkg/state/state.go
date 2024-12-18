package state

import "time"

type State struct {
	Parameters map[string]string
	Metadata   Metadata
	State      SchedulerState
	Version    string
	LastRun    time.Time
	LastRunID  string
}

type Metadata struct {
	Version string
	OS      string
}

type SchedulerState struct {
}

func NewState() *State {
	return &State{}
}
