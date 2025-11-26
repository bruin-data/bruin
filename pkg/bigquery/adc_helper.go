package bigquery

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/bruin-data/bruin/pkg/config"
	"github.com/bruin-data/bruin/pkg/executor"
	"github.com/bruin-data/bruin/pkg/pipeline"
	"github.com/manifoldco/promptui"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
)

// checkADCCredentials checks if ADC credentials are available. Returns the error from
// FindDefaultCredentials if credentials are not available, or nil if available or not needed.
func checkADCCredentials(ctx context.Context, conn DB) error {
	if !conn.UsesApplicationDefaultCredentials() {
		return nil
	}
	_, err := google.FindDefaultCredentials(ctx, scopes...)
	return err
}

// ensureADCCredentials verifies that ADC credentials are available for a BigQuery operation.
// This is a verification step - credentials should already be checked before pipeline execution
// via CheckADCCredentialsForPipeline. Returns an error if credentials are not available.
func ensureADCCredentials(ctx context.Context, connName string, conn DB) error {
	if err := checkADCCredentials(ctx, conn); err != nil {
		return errors.Wrapf(err, "ADC credentials not available for BigQuery connection '%s' (should have been checked before execution)", connName)
	}
	return nil
}

// ensureADCCredentialsWithPrompt checks for ADC credentials and prompts the user if needed.
// This is used before pipeline execution starts to ensure credentials are available.
func ensureADCCredentialsWithPrompt(ctx context.Context, connName string, conn DB) error {
	err := checkADCCredentials(ctx, conn)
	if err == nil {
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

	// Flush any pending output and add visual separation
	fmt.Fprintf(output, "\n")
	if flusher, ok := output.(interface{ Flush() }); ok {
		flusher.Flush()
	}

	// Write warning message to stdout (for logging)
	fmt.Fprintf(output, "⚠️  Application Default Credentials (ADC) not found for BigQuery connection '%s'.\n", connName)
	fmt.Fprintf(output, "   This connection is configured to use ADC but credentials are not available.\n\n")
	if flusher, ok := output.(interface{ Flush() }); ok {
		flusher.Flush()
	}

	// Check if gcloud is available
	if !isGcloudAvailable() {
		fmt.Fprintf(output, "   gcloud CLI is not available. Please install it and run:\n")
		fmt.Fprintf(output, "   $ gcloud auth application-default login\n\n")
		if flusher, ok := output.(interface{ Flush() }); ok {
			flusher.Flush()
		}
		return &ADCCredentialError{
			ClientType:  "BigQuery client",
			OriginalErr: err,
		}
	}

	// Write prompt message to stderr for better visibility
	// Add multiple newlines to separate from other output
	fmt.Fprintf(os.Stderr, "\n\n\n")
	fmt.Fprintf(os.Stderr, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Fprintf(os.Stderr, "⚠️  BigQuery Authentication Required\n")
	fmt.Fprintf(os.Stderr, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Fprintf(os.Stderr, "Application Default Credentials (ADC) not found for connection '%s'.\n", connName)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "Would you like to run 'gcloud auth application-default login' now?\n")
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Fprintf(os.Stderr, "\n")

	// Prompt the user (promptui uses stderr by default, but we'll be explicit)
	prompt := promptui.Prompt{
		Label:     "Run 'gcloud auth application-default login'?",
		IsConfirm: true,
		Stdin:     os.Stdin,
	}

	result, promptErr := prompt.Run()
	if promptErr != nil || strings.ToLower(result) != "y" {
		fmt.Fprintf(os.Stderr, "\n❌ Operation cancelled. Please run 'gcloud auth application-default login' manually.\n\n")
		fmt.Fprintf(output, "   Operation cancelled. Please run 'gcloud auth application-default login' manually.\n\n")
		if flusher, ok := output.(interface{ Flush() }); ok {
			flusher.Flush()
		}
		return &ADCCredentialError{
			ClientType:  "BigQuery client",
			OriginalErr: err,
		}
	}

	// Run gcloud command
	fmt.Fprintf(os.Stderr, "\n🔐 Running: gcloud auth application-default login\n")
	fmt.Fprintf(os.Stderr, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
	fmt.Fprintf(output, "   Running: gcloud auth application-default login\n")
	if flusher, ok := output.(interface{ Flush() }); ok {
		flusher.Flush()
	}

	cmd := exec.Command("gcloud", "auth", "application-default", "login")
	cmd.Stdout = os.Stderr // gcloud output to stderr for visibility
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin

	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "\n")
		return errors.Wrapf(err, "failed to run 'gcloud auth application-default login': %v", err)
	}

	fmt.Fprintf(os.Stderr, "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n")
	fmt.Fprintf(os.Stderr, "✓ Successfully authenticated with gcloud.\n")
	fmt.Fprintf(os.Stderr, "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n")
	fmt.Fprintf(output, "   ✓ Successfully authenticated with gcloud.\n\n")
	if flusher, ok := output.(interface{ Flush() }); ok {
		flusher.Flush()
	}

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

// CheckADCCredentialsForPipeline checks ADC credentials for all BigQuery connections
// used in the pipeline before execution starts. This ensures credentials are available
// before any tasks begin running, avoiding prompts during parallel execution.
func CheckADCCredentialsForPipeline(ctx context.Context, p *pipeline.Pipeline, connGetter config.ConnectionGetter) error {
	// Collect unique BigQuery connection names from all assets
	bigQueryConnections := make(map[string]bool)

	for _, asset := range p.Assets {
		// Check if this is a BigQuery asset
		if !isBigQueryAssetType(asset.Type) {
			continue
		}

		// Get the connection name for this asset
		connName, err := p.GetConnectionNameForAsset(asset)
		if err != nil {
			// Skip assets where we can't determine the connection
			continue
		}

		bigQueryConnections[connName] = true
	}

	// Check each unique BigQuery connection
	for connName := range bigQueryConnections {
		conn := connGetter.GetConnection(connName)
		if conn == nil {
			continue
		}

		bqConn, ok := conn.(DB)
		if !ok {
			continue
		}

		// Check if this connection uses ADC
		if !bqConn.UsesApplicationDefaultCredentials() {
			continue
		}

		// Check and prompt for ADC credentials if needed
		if err := ensureADCCredentialsWithPrompt(ctx, connName, bqConn); err != nil {
			return err
		}
	}

	return nil
}

// isBigQueryAssetType checks if the given asset type is a BigQuery type.
func isBigQueryAssetType(assetType pipeline.AssetType) bool {
	mapping, ok := pipeline.AssetTypeConnectionMapping[assetType]
	return ok && mapping == "google_cloud_platform"
}
