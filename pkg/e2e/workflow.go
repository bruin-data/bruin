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
	log.Printf("Starting workflow: %s", w.Name)

	if w.Name == "" {
		w.Name = "workflow-" + uuid.New().String()
		log.Printf("Generated new workflow name: %s", w.Name)
	}

	for _, task := range w.Steps {
		// Set up task-specific environment for each workflow step
		// This ensures each step gets its own UV installation directory if needed
		if os.Getenv("BRUIN_INTEGRATION_TEST") == "1" {
			// Create a unique temporary directory for this workflow step
			tempDir, err := os.MkdirTemp("", "workflow-step-*")
			if err == nil {
				os.Setenv("BRUIN_TEST_TEMP_DIR", tempDir)
			}
		}

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
