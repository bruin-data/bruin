package main

import (
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
)

// generatePipelineFromTemplate generates a pipeline from templates by copying files
// and doing simple string replacements for platform-specific values (like bruin init)
func generatePipelineFromTemplate(templateDir, targetDir string, platform PlatformConfig, platformName string) error {
	// Copy pipeline.yml and add platform-specific defaults
	if err := copyAndCustomizePipelineYml(templateDir, targetDir, platform); err != nil {
		return errors.Wrap(err, "failed to copy pipeline.yml")
	}

	// Copy assets directory with platform-specific customizations
	assetsSource := filepath.Join(templateDir, "assets")
	assetsTarget := filepath.Join(targetDir, "assets")
	if err := copyAndCustomizeAssets(assetsSource, assetsTarget, platform); err != nil {
		return errors.Wrap(err, "failed to copy assets")
	}

	// Copy expectations directory (platform-specific expectations)
	// templateDir is like: .../templates/scd2-by-column-pipeline
	// We need to go to: .../{platformName}/test-pipelines/scd2-pipelines/scd2-by-column-pipeline/expectations
	expectationsSource := filepath.Join(filepath.Dir(filepath.Dir(templateDir)), platformName, "test-pipelines", "scd2-pipelines", "scd2-by-column-pipeline", "expectations")
	expectationsTarget := filepath.Join(targetDir, "expectations")
	if err := copyDir(expectationsSource, expectationsTarget); err != nil {
		return errors.Wrap(err, "failed to copy expectations directory")
	}

	return nil
}

// copyAndCustomizePipelineYml copies pipeline.yml and adds platform-specific defaults
func copyAndCustomizePipelineYml(templateDir, targetDir string, platform PlatformConfig) error {
	sourcePath := filepath.Join(templateDir, "pipeline.yml")
	targetPath := filepath.Join(targetDir, "pipeline.yml")

	content, err := os.ReadFile(sourcePath)
	if err != nil {
		return errors.Wrapf(err, "failed to read pipeline.yml template")
	}

	// Add platform-specific defaults
	lines := strings.Split(string(content), "\n")
	var result []string
	result = append(result, lines...)

	// Add default_connections and default type if not present
	hasDefaultConnections := false
	hasDefaultType := false
	for _, line := range lines {
		if strings.Contains(line, "default_connections:") {
			hasDefaultConnections = true
		}
		if strings.Contains(line, "default:") {
			hasDefaultType = true
		}
	}

	if !hasDefaultConnections || !hasDefaultType {
		// Find where to insert (after start_date)
		insertIndex := len(result)
		for i, line := range result {
			if strings.Contains(line, "start_date:") {
				insertIndex = i + 1
				break
			}
		}

		// Insert platform-specific defaults
		newLines := []string{}
		newLines = append(newLines, result[:insertIndex]...)
		if !hasDefaultConnections {
			newLines = append(newLines, "default_connections:")
			newLines = append(newLines, "    "+platform.PlatformConnection+": \""+platform.Connection+"\"")
		}
		if !hasDefaultType {
			newLines = append(newLines, "default:")
			newLines = append(newLines, "    type: "+platform.AssetType)
		}
		newLines = append(newLines, result[insertIndex:]...)
		result = newLines
	}

	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return errors.Wrapf(err, "failed to create target directory")
	}

	if err := os.WriteFile(targetPath, []byte(strings.Join(result, "\n")), 0o644); err != nil {
		return errors.Wrapf(err, "failed to write pipeline.yml")
	}

	return nil
}

// copyAndCustomizeAssets copies assets and adds platform-specific type and name
func copyAndCustomizeAssets(sourceDir, targetDir string, platform PlatformConfig) error {
	return filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		// Calculate relative path
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return err
		}

		// Create destination path
		dstPath := filepath.Join(targetDir, relPath)

		// Read source file
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		// Customize content for platform
		customized := customizeAssetContent(string(content), platform)

		// Create directory if needed
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return err
		}

		// Write customized content
		return os.WriteFile(dstPath, []byte(customized), info.Mode())
	})
}

// customizeAssetContent updates asset name with schema prefix if needed
// Type is not added here - it comes from pipeline.yml default
func customizeAssetContent(content string, platform PlatformConfig) string {
	// Simply replace "name: test.menu" with "name: {schema}.menu" if schema prefix is different
	// Most platforms use "test" as schema, so this might not change anything
	if platform.SchemaPrefix != "test" {
		content = strings.ReplaceAll(content, "name: test.menu", "name: "+platform.SchemaPrefix+".menu")
	}
	return content
}

// copyDir copies a directory recursively
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Calculate relative path from source
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		// Create destination path
		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Read source file
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		// Write destination file
		return os.WriteFile(dstPath, data, info.Mode())
	})
}

// copyResourceFile copies a resource file and customizes it for the platform
func copyResourceFile(templateDir, targetPath string, platform PlatformConfig) error {
	// Read the resource file
	content, err := os.ReadFile(templateDir)
	if err != nil {
		return errors.Wrapf(err, "failed to read resource file: %s", templateDir)
	}

	// Customize content
	customized := customizeAssetContent(string(content), platform)

	// Write to target
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return errors.Wrapf(err, "failed to create target directory")
	}

	if err := os.WriteFile(targetPath, []byte(customized), 0o644); err != nil {
		return errors.Wrapf(err, "failed to write resource file")
	}

	return nil
}
