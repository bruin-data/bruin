package bigquery

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/manifoldco/promptui"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
)

// adcPromptMutex ensures only one ADC credential prompt happens at a time across all BigQuery assets.
// This prevents multiple parallel assets from all prompting the user simultaneously.
var adcPromptMutex sync.Mutex

// checkADCCredentials checks if ADC credentials are available. Returns the error from
// FindDefaultCredentials if credentials are not available, or nil if available or not needed.
func checkADCCredentials(ctx context.Context, conn DB) error {
	if !conn.UsesApplicationDefaultCredentials() {
		return nil
	}
	_, err := google.FindDefaultCredentials(ctx, scopes...)
	return err
}

// ensureADCCredentialsWithPrompt checks for ADC credentials and prompts the user if needed.
// Uses a global mutex to ensure only one prompt happens at a time when multiple BigQuery
// assets run in parallel.
func ensureADCCredentialsWithPrompt(ctx context.Context, connName string, conn DB) error {
	// Quick check without lock - if credentials are available, no need to acquire mutex
	err := checkADCCredentials(ctx, conn)
	if err == nil {
		return nil
	}

	// Acquire mutex to ensure only one prompt at a time
	adcPromptMutex.Lock()
	defer adcPromptMutex.Unlock()

	// Re-check after acquiring lock - another goroutine may have completed authentication
	err = checkADCCredentials(ctx, conn)
	if err == nil {
		return nil
	}

	// Check if gcloud is available
	if !isGcloudAvailable() {
		writer := ctx.Value(executor.KeyPrinter)
		var output io.Writer = os.Stdout
		if writer != nil {
			if w, ok := writer.(io.Writer); ok {
				output = w
			}
		}
		fmt.Fprintf(output, "ADC credentials not found for BigQuery connection '%s'.\n", connName)
		fmt.Fprintf(output, "gcloud CLI not available. Install it and run: gcloud auth application-default login\n")
		if flusher, ok := output.(interface{ Flush() }); ok {
			flusher.Flush()
		}
		return &ADCCredentialError{
			ClientType:  "BigQuery client",
			OriginalErr: err,
		}
	}

	// Write prompt message to stderr for better visibility
	fmt.Fprintf(os.Stderr, "ADC credentials not found for connection '%s'.\n", connName)

	// Prompt the user (promptui uses stderr by default, but we'll be explicit)
	// Note: promptui automatically adds "?" and "[y/N]" when IsConfirm is true
	prompt := promptui.Prompt{
		Label:     "Run 'gcloud auth application-default login'",
		IsConfirm: true,
		Stdin:     os.Stdin,
	}

	result, promptErr := prompt.Run()
	if promptErr != nil || strings.ToLower(result) != "y" {
		fmt.Fprintf(os.Stderr, "Cancelled. Run 'gcloud auth application-default login' manually.\n")
		return &ADCCredentialError{
			ClientType:  "BigQuery client",
			OriginalErr: err,
		}
	}

	// Run gcloud command
	fmt.Fprintf(os.Stderr, "Running: gcloud auth application-default login\n")

	cmd := exec.Command("gcloud", "auth", "application-default", "login")
	cmd.Stdout = os.Stderr // gcloud output to stderr for visibility
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "failed to run 'gcloud auth application-default login': %v", err)
	}

	fmt.Fprintf(os.Stderr, "Successfully authenticated with gcloud.\n")

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
