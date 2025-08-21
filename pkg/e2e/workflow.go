package e2e

import (
	"fmt"
	"log"
	"os"

	"github.com/google/uuid"
)

type WorkflowError struct {
	StepName     string
	WorkflowName string
	Err          error
}

func (e *WorkflowError) Error() string {
	return fmt.Sprintf("error in step '%s' of workflow '%s': %v", e.StepName, e.WorkflowName, e.Err)
}

type Workflow struct {
	Name  string
	Steps []Task
}

func (w *Workflow) Run() error {

	silent := os.Getenv("SILENT") == "1"
	if !silent {
		log.Printf("Starting workflow: %s", w.Name)
	}

	if w.Name == "" {
		w.Name = "workflow-" + uuid.New().String()
		if !silent {
			log.Printf("Generated new workflow name: %s", w.Name)
		}
	}

	for _, task := range w.Steps {
		if err := task.Run(); err != nil {
			wfErr := &WorkflowError{
				StepName:     task.Name,
				WorkflowName: w.Name,
				Err:          err,
			}
			log.Printf("Error: %v", wfErr)
			return wfErr
		}
	}

	if !silent {
		log.Printf("Completed workflow: %s", w.Name)
	}
	return nil
}
