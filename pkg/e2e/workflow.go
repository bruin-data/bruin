package e2e

import (
	"fmt"
	"log"

	"github.com/google/uuid"
)

// Define a custom error type for workflow errors
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
	log.Printf("Starting workflow: %s", w.Name)

	if w.Name == "" {
		w.Name = "workflow-" + uuid.New().String()
		log.Printf("Generated new workflow name: %s", w.Name)
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

	log.Printf("Completed workflow: %s", w.Name)
	return nil
}
