package bigquery

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/manifoldco/promptui"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
)

// ensureADCCredentials proactively checks for ADC credentials before executing a BigQuery operation.
// If the connection uses ADC and credentials are not found, it prompts the user to run
// `gcloud auth application-default login`. Returns an error if credentials are still not
// available after prompting, or nil if credentials are available or not needed.
func ensureADCCredentials(ctx context.Context, connName string, conn DB) error {
	// Check if the connection uses ADC
	if !conn.UsesApplicationDefaultCredentials() {
		// Connection doesn't use ADC, no need to check
		return nil
	}

	// ADC is enabled - proactively check if credentials are available
	_, err := google.FindDefaultCredentials(ctx, scopes...)
	if err == nil {
		// Credentials are available, proceed
		return nil
	}

	// ADC credentials not found - prompt the user
	writer := ctx.Value(executor.KeyPrinter)
	var output io.Writer = os.Stdout
	if writer != nil {
		if w, ok := writer.(io.Writer); ok {
			output = w
		}
	}

	fmt.Fprintf(output, "\n⚠️  Application Default Credentials (ADC) not found for BigQuery connection '%s'.\n", connName)
	fmt.Fprintf(output, "   This connection is configured to use ADC but credentials are not available.\n\n")

	// Check if gcloud is available
	if !isGcloudAvailable() {
		fmt.Fprintf(output, "   gcloud CLI is not available. Please install it and run:\n")
		fmt.Fprintf(output, "   $ gcloud auth application-default login\n\n")
		return &ADCCredentialError{
			ClientType:  "BigQuery client",
			OriginalErr: err,
		}
	}

	// Prompt the user
	prompt := promptui.Prompt{
		Label:     "Would you like to run 'gcloud auth application-default login' now?",
		IsConfirm: true,
		Stdin:     os.Stdin,
	}

	result, promptErr := prompt.Run()
	if promptErr != nil || strings.ToLower(result) != "y" {
		fmt.Fprintf(output, "   Operation cancelled. Please run 'gcloud auth application-default login' manually.\n\n")
		return &ADCCredentialError{
			ClientType:  "BigQuery client",
			OriginalErr: err,
		}
	}

	// Run gcloud command
	fmt.Fprintf(output, "   Running: gcloud auth application-default login\n")
	cmd := exec.Command("gcloud", "auth", "application-default", "login")
	cmd.Stdout = output
	cmd.Stderr = output
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "failed to run 'gcloud auth application-default login': %v", err)
	}

	fmt.Fprintf(output, "   ✓ Successfully authenticated with gcloud.\n\n")

	// Verify credentials are now available
	_, err = google.FindDefaultCredentials(ctx, scopes...)
	if err != nil {
		return errors.Wrap(err, "ADC credentials still not available after authentication")
	}

	return nil
}

// isGcloudAvailable checks if the gcloud CLI is available in the system PATH.
func isGcloudAvailable() bool {
	_, err := exec.LookPath("gcloud")
	return err == nil
}
