package e2e

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/bruin-data/bruin/pkg/helpers"
	"github.com/google/uuid"
)

type Task struct {
	Name          string
	Command       string
	Retries       int
	Args          []string
	Env           []string
	WorkingDir    string
	Expected      Output
	Actual        Output
	SkipJSONNodes []string
	Asserts       []func(*Task) error
}

type Output struct {
	ExitCode int
	Output   string
	Error    string
	Contains []string
}

func (s *Task) Run() error {
	if s.Name == "" {
		s.Name = "task-" + uuid.New().String()
	}
	if s.Retries == 0 {
		s.Retries = 1
	}
	log.Printf("Running task: %s", s.Name)

	for attempt := 1; attempt <= s.Retries; attempt++ {
		log.Printf("Attempt %d of %d for task: %s", attempt, s.Retries, s.Name)

		if err := s.runAttempt(); err != nil {
			if attempt < s.Retries {
				log.Printf("Retrying task %s after failure", s.Name)
				time.Sleep(1 * time.Second)
				continue
			}
			fmt.Println("Command: ", s.Command, strings.Join(s.Args, " "))
			return fmt.Errorf("assertion failed for task %s: %w", s.Name, err)
		}
		return nil
	}
	return nil
}

func (s *Task) runAttempt() error {
	cmd := exec.Command(s.Command, s.Args...) //nolint:gosec
	cmd.Env = append(os.Environ(), s.Env...)
	if s.WorkingDir != "" {
		cmd.Dir = s.WorkingDir
	}
	output, err := cmd.CombinedOutput()
	s.Actual.ExitCode = helpers.GetExitCode(err)
	s.Actual.Output = string(output)

	if err != nil {
		s.Actual.Error = err.Error()
	}

	for _, assert := range s.Asserts {
		if err := assert(s); err != nil {
			return err
		}
	}
	return nil
}
