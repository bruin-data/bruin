package state

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

func (s *State) Load(directory string) error {
	s.Lock()
	defer s.Unlock()

	files, err := os.ReadDir(directory)
	if err != nil {
		return err
	}

	var latestFile os.DirEntry
	var latestTime time.Time

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		fileName := file.Name()
		parts := strings.Split(fileName, "_")
		if len(parts) < 2 {
			log.Printf("invalid file name format: %s", fileName)
			continue
		}

		timestampStr := parts[0]
		timestamp, err := time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			log.Printf("failed to parse timestamp from file name: %v", err)
			continue
		}

		if timestamp.After(latestTime) {
			latestTime = timestamp
			latestFile = file
		}
	}

	if latestFile == nil {
		return nil
	}

	filePath := filepath.Join(directory, latestFile.Name())
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	err = json.Unmarshal(fileContent, s)
	if err != nil {
		return err
	}
	return nil
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
