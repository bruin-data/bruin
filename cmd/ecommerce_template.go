package cmd

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"text/template"

	"github.com/bruin-data/bruin/templates"
	"gopkg.in/yaml.v3"
)

// EcommerceTemplateData is the data passed to Go text/template files.
type EcommerceTemplateData struct {
	Warehouse string
	Payments  string
	Marketing string
	Ads       []string
	Analytics string

	// Derived booleans for easy template access.
	IsClickHouse bool
	IsBigQuery   bool
	IsSnowflake  bool
	IsStripe     bool
	HasFacebook  bool
	HasGoogle    bool
	HasTikTok    bool
	IsKlaviyo    bool
	IsHubSpot    bool
	IsGA4        bool
	IsMixpanel   bool
}

// newTemplateData builds the template data from user choices.
func newTemplateData(c *EcommerceChoices) *EcommerceTemplateData {
	return &EcommerceTemplateData{
		Warehouse: c.Warehouse,
		Payments:  c.Payments,
		Marketing: c.Marketing,
		Ads:       c.Ads,
		Analytics: c.Analytics,

		IsClickHouse: c.Warehouse == warehouseClickHouse,
		IsBigQuery:   c.Warehouse == warehouseBigQuery,
		IsSnowflake:  c.Warehouse == warehouseSnowflake,
		IsStripe:     c.Payments == paymentsStripe,
		HasFacebook:  slices.Contains(c.Ads, adsFacebook),
		HasGoogle:    slices.Contains(c.Ads, adsGoogle),
		HasTikTok:    slices.Contains(c.Ads, adsTikTok),
		IsKlaviyo:    c.Marketing == marketingKlaviyo,
		IsHubSpot:    c.Marketing == marketingHubSpot,
		IsGA4:        c.Analytics == analyticsGA4,
		IsMixpanel:   c.Analytics == analyticsMixpanel,
	}
}

// ManifestAsset describes a single asset entry in manifest.yml.
type ManifestAsset struct {
	Path     string            `yaml:"path"`
	Requires map[string]string `yaml:"requires,omitempty"`
}

// EcommerceManifest is the top-level structure of manifest.yml.
type EcommerceManifest struct {
	Pipeline struct {
		Path string `yaml:"path"`
	} `yaml:"pipeline"`
	Assets []ManifestAsset `yaml:"assets"`
}

// loadManifest reads and parses the ecommerce manifest from the embedded FS.
func loadManifest() (*EcommerceManifest, error) {
	data, err := templates.Templates.ReadFile("ecommerce/manifest.yml")
	if err != nil {
		return nil, fmt.Errorf("could not read ecommerce manifest: %w", err)
	}

	var m EcommerceManifest
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("could not parse ecommerce manifest: %w", err)
	}

	return &m, nil
}

// matchesChoices returns true when the asset's requirements are satisfied by the user's choices.
func (a *ManifestAsset) matchesChoices(c *EcommerceChoices) bool {
	for key, value := range a.Requires {
		switch key {
		case "payments":
			if c.Payments != value {
				return false
			}
		case "marketing":
			if c.Marketing != value {
				return false
			}
		case "ads":
			if !slices.Contains(c.Ads, value) {
				return false
			}
		case "analytics":
			if c.Analytics != value {
				return false
			}
		case "warehouse":
			if c.Warehouse != value {
				return false
			}
		}
	}
	return true
}

// isTemplate returns true when the file needs Go text/template processing.
func isTemplate(path string) bool {
	return strings.HasSuffix(path, ".tmpl")
}

// outputPath strips the .tmpl suffix to produce the final file name.
func outputPath(path string) string {
	return strings.TrimSuffix(path, ".tmpl")
}

// renderEcommerceTemplate reads a template file from the embedded FS and renders it with the given data.
func renderEcommerceTemplate(path string, data *EcommerceTemplateData) (string, error) {
	content, err := templates.Templates.ReadFile("ecommerce/" + path)
	if err != nil {
		return "", fmt.Errorf("could not read template %s: %w", path, err)
	}

	tmpl, err := template.New(path).Parse(string(content))
	if err != nil {
		return "", fmt.Errorf("could not parse template %s: %w", path, err)
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("could not render template %s: %w", path, err)
	}

	return buf.String(), nil
}

// readStaticFile reads a non-template file from the embedded FS.
func readStaticFile(path string) (string, error) {
	content, err := templates.Templates.ReadFile("ecommerce/" + path)
	if err != nil {
		return "", fmt.Errorf("could not read file %s: %w", path, err)
	}
	return string(content), nil
}

// buildEcommerceFiles returns all files to be generated, keyed by relative output path.
func buildEcommerceFiles(c *EcommerceChoices) (map[string]string, error) {
	manifest, err := loadManifest()
	if err != nil {
		return nil, err
	}

	data := newTemplateData(c)
	files := make(map[string]string)

	// Render pipeline config.
	if manifest.Pipeline.Path != "" {
		content, err := renderEcommerceTemplate(manifest.Pipeline.Path, data)
		if err != nil {
			return nil, err
		}
		files[outputPath(manifest.Pipeline.Path)] = content
	}

	// Process each asset entry.
	for i := range manifest.Assets {
		asset := &manifest.Assets[i]
		if !asset.matchesChoices(c) {
			continue
		}

		var content string
		if isTemplate(asset.Path) {
			content, err = renderEcommerceTemplate(asset.Path, data)
			if err != nil {
				return nil, err
			}
		} else {
			content, err = readStaticFile(asset.Path)
			if err != nil {
				return nil, err
			}
		}

		files[outputPath(asset.Path)] = content
	}

	return files, nil
}
