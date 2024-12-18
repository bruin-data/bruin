package state

import "time"

const (
	StatusPending Status = "pending"
	StatusRunning Status = "running"
	StatusSuccess Status = "success"
	StatusFailed  Status = "failed"
	StatusSkip    Status = "skiped"
)

type State struct {
	Parameters map[string]string
	Metadata   Metadata
	State      []SchedulerState
	Version    string
	LastRun    time.Time
	LastRunID  string
}

type Metadata struct {
	Version string
	OS      string
}

type SchedulerState struct {
	Upstream   []string
	Downstream []string
	Status     Status
	Error      string
	StartTime  time.Time
	EndTime    time.Time
	Name       string
	Id         string
}

type Status string

func NewState() *State {
	return &State{}
}
