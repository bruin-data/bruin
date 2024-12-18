package state

import (
	"encoding/json"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/hmdsefi/gograph"
	"github.com/hmdsefi/gograph/traverse"
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

func NewState(runid, name string) *State {
	return &State{
		Metadata: Metadata{
			Version: version,
			OS:      runtime.GOOS,
		},
		State:     []*AssetInstance{},
		Version:   "1.0.0",
		TimeStamp: time.Now(),
		RunID:     runid,
		Name:      name,
	}
}

func (s *State) SetParam(parameters map[string]string) {
	s.Lock()
	defer s.Unlock()
	s.Parameters = parameters
}

func (s *State) Get() []*AssetInstance {
	s.Lock()
	defer s.Unlock()
	return s.State
}

func (s *State) GetAssetState(name string) *AssetInstance {
	s.Lock()
	defer s.Unlock()
	for _, asset := range s.State {
		if asset.Name == name {
			return asset
		}
	}
	return nil
}

func (s *State) Set(results []*AssetInstance) {
	s.Lock()
	defer s.Unlock()
	s.State = results
}

func (s *State) Load(directory string) error {
	s.Lock()
	defer s.Unlock()

	filePath := filepath.Join(directory, "latest.json")

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
	err := os.MkdirAll(directory, os.ModePerm)
	if err != nil {
		log.Fatalf("failed to create directory: %v", err)
	}
	fileContent, err := json.Marshal(s)
	if err != nil {
		log.Fatalf("failed to marshal state to JSON: %v", err)
	}
	latestFilePath := filepath.Join(directory, "latest.json")
	err = os.WriteFile(latestFilePath, fileContent, 0o600)
	if err != nil {
		log.Fatalf("failed to write latest file: %v", err)
	}
	return s.State
}

func (s *State) GetBFSToAsset(name string) (string, error) {
	result := ""
	graph := gograph.New[string](gograph.Directed())
	for _, asset := range s.State {
		for _, upstream := range asset.Upstream {
			if _, err := graph.AddEdge(gograph.NewVertex(asset.Name), gograph.NewVertex(upstream.Value)); err != nil {
				return "", err
			}
		}
	}
	bfs, err := traverse.NewBreadthFirstIterator[string](graph, name)
	if err != nil {
		return "", err
	}
	for bfs.HasNext() {
		result += bfs.Next().Label() + "->"
	}
	return result, nil
}

func (s *State) GetParam(key string) string {
	return s.Parameters[key]
}
