package state

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

var version = "dev"

type State struct {
	sync.RWMutex
	Parameters map[string]string `json:"parameters"`
	Metadata   Metadata          `json:"metadata"`
	State      []*AssetInstance  `json:"state"`
	Name       string            `json:"name"`
	Version    string            `json:"version"`
	TimeStamp  time.Time         `json:"timestamp"`
	RunID      string            `json:"run_id"`
}

type Metadata struct {
	Version string `json:"version"`
	OS      string `json:"os"`
}

type AssetInstance struct {
	ID       string              `json:"id"`
	HumanID  string              `json:"human_id"`
	Name     string              `json:"name"`
	Pipeline string              `json:"pipeline"`
	Status   string              `json:"status"`
	Upstream []pipeline.Upstream `json:"upstream"`
}

func NewState(runid string, parameters map[string]string, name string) *State {
	return &State{
		Parameters: parameters,
		Metadata: Metadata{
			Version: version,
			OS:      runtime.GOOS,
		},
		State:     []*AssetInstance{},
		Version:   "1.0.0",
		TimeStamp: time.Time{},
		RunID:     runid,
		Name:      name,
	}
}

func (s *State) SetState(results []*AssetInstance) {
	s.Lock()
	defer s.Unlock()
	s.State = results
}

func (s *State) Save(directory string) []*AssetInstance {
	filePath := fmt.Sprintf("%s/%s_%s.json", directory, s.TimeStamp.Format(time.RFC3339), s.RunID)
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		log.Fatalf("failed to create directory: %v", err)
	}
	fileContent, err := json.Marshal(s)
	if err != nil {
		log.Fatalf("failed to marshal state to JSON: %v", err)
	}
	err = os.WriteFile(filePath, fileContent, 0o600)
	if err != nil {
		log.Fatalf("failed to write file: %v", err)
	}
	return s.State
}
