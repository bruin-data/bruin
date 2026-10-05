package ssis

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/afero"
)

// These fields mirror msdb.dbo.sysjobs and msdb.dbo.sysjobsteps. Explicit
// control-flow fields are required so incomplete exports cannot change behavior.
type agentJob struct {
	Name        string      `json:"name"`
	Enabled     *int        `json:"enabled"`
	StartStepID int         `json:"start_step_id"`
	Steps       []agentStep `json:"steps"`
}

type agentStep struct {
	ID              int    `json:"step_id"`
	Name            string `json:"step_name"`
	Subsystem       string `json:"subsystem"`
	Command         string `json:"command"`
	Database        string `json:"database_name"`
	OnSuccessAction int    `json:"on_success_action"`
	OnFailAction    int    `json:"on_fail_action"`
}

func parseJobs(fs afero.Fs, source string, data []byte) ([]workflow, error) {
	var jobs []agentJob
	if err := json.Unmarshal(data, &jobs); err != nil {
		return nil, fmt.Errorf("expected a SQL Server Agent job JSON array: %w", err)
	}
	var workflows []workflow
	for _, job := range jobs {
		if job.Name == "" || len(job.Steps) == 0 || job.Enabled == nil || *job.Enabled != 1 {
			return nil, errors.New("expected a named, enabled job with steps")
		}
		sort.Slice(job.Steps, func(i, j int) bool { return job.Steps[i].ID < job.Steps[j].ID })
		if job.StartStepID != job.Steps[0].ID {
			return nil, fmt.Errorf("job %q: start_step_id must reference the first step", job.Name)
		}
		w := workflow{name: job.Name}
		for i, step := range job.Steps {
			if strings.TrimSpace(step.Command) == "" || strings.Contains(step.Command, "$(") {
				return nil, fmt.Errorf("job %q step %d: empty commands and Agent tokens require manual migration", job.Name, step.ID)
			}
			expectedSuccess := 3 // Go to the next step.
			if i == len(job.Steps)-1 {
				expectedSuccess = 1 // Quit reporting success.
			}
			if step.ID <= 0 || step.OnSuccessAction != expectedSuccess || step.OnFailAction != 2 {
				return nil, fmt.Errorf("job %q step %d: only sequential success / quit-on-failure flow is supported", job.Name, step.ID)
			}
			t := task{id: strconv.Itoa(step.ID), name: step.Name, code: step.Command}
			if i > 0 {
				t.upstream = []string{strconv.Itoa(job.Steps[i-1].ID)}
			}
			switch strings.ToUpper(step.Subsystem) {
			case "TSQL":
				if step.Database != "" {
					t.code = "USE [" + strings.ReplaceAll(step.Database, "]", "]]") + "];\n" + t.code
				}
			case "CMDEXEC":
				executable, arguments, ok := strings.Cut(strings.TrimSpace(step.Command), " ")
				if !ok {
					return nil, fmt.Errorf("job %q step %d: expected python script.py", job.Name, step.ID)
				}
				var err error
				t.code, err = pythonScript(fs, source, executable, arguments)
				if err != nil {
					return nil, fmt.Errorf("job %q step %d: %w", job.Name, step.ID, err)
				}
				t.python = true
			default:
				return nil, fmt.Errorf("job %q step %d: unsupported subsystem %q", job.Name, step.ID, step.Subsystem)
			}
			w.tasks = append(w.tasks, t)
		}
		workflows = append(workflows, w)
	}
	return workflows, nil
}
