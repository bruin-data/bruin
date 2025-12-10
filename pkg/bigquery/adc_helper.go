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

// ensureADCCredentialsWithPrompt checks for ADC credentials and prompts the user if needed.
// This is used before pipeline execution starts to ensure credentials are available.
func ensureADCCredentialsWithPrompt(ctx context.Context, connName string, conn DB) error {
	err := checkADCCredentials(ctx, conn)
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

	// Create the BigQuery client if it was nil (lazy initialization)
	// Type assert to *Client to access ensureClientInitialized method
	if bqClient, ok := conn.(*Client); ok {
		if err := bqClient.ensureClientInitialized(ctx); err != nil {
			return errors.Wrap(err, "failed to create BigQuery client after ADC authentication")
		}
	}

	return nil
}

// isGcloudAvailable checks if the gcloud CLI is available in the system PATH.
func isGcloudAvailable() bool {
	_, err := exec.LookPath("gcloud")
	return err == nil
}

// CheckADCCredentialsForPipeline checks ADC credentials for BigQuery connections
// used by the given assets before execution starts.
func CheckADCCredentialsForPipeline(ctx context.Context, p *pipeline.Pipeline, assets []*pipeline.Asset, connGetter config.ConnectionGetter) error {
	// Collect unique BigQuery connection names from provided assets
	bigQueryConnections := make(map[string]bool)

	for _, asset := range assets {
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
