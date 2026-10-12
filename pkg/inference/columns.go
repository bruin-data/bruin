package inference

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/bruin-data/bruin/pkg/pipeline"
)

// Supported inference output column types.
const (
	colTypeString  = "string"
	colTypeBoolean = "boolean"
	colTypeInteger = "integer"
	colTypeNumber  = "number"
)

// TypeSafe question kinds.
const (
	kindChoice = "choice"
	kindNoul   = "noul"
	kindScore  = "score"
)

// outputColumn is the provider-independent generated field contract.
type outputColumn struct {
	Name, Type, Prompt string
	Choices            map[string]string
	Minimum, Maximum   *float64
	Levels             []string `json:",omitempty"`
	Threshold          *float64 `json:",omitempty"`
}

type requestGroup struct {
	provider, model, connection string
	apiKey                      string
	columns                     []outputColumn
}

// typeSafeKind derives the request primitive from a validated column.
func (c outputColumn) typeSafeKind() string {
	if c.Levels != nil {
		return kindScore
	}
	if c.Type == colTypeString {
		return kindChoice
	}
	return kindNoul
}

func validateTypeSafeColumn(column outputColumn) error {
	if column.Threshold != nil && (column.Type != colTypeBoolean || math.IsNaN(*column.Threshold) || *column.Threshold < 0 || *column.Threshold > 1) {
		return errors.New("threshold requires a boolean column and a finite value between 0 and 1")
	}
	switch column.typeSafeKind() {
	case kindScore:
		if column.Type != colTypeNumber || len(column.Levels) < 2 || len(column.Levels) > 10 {
			return errors.New("score requires type number and 2 to 10 ordered levels")
		}
		for _, level := range column.Levels {
			if strings.TrimSpace(level) == "" {
				return errors.New("score levels must have nonempty descriptions")
			}
		}
	case kindChoice:
		if len(column.Choices) < 2 || len(column.Choices) > 255 {
			return errors.New("choice requires type string and 2 to 255 choices")
		}
	case kindNoul:
		if column.Type != colTypeBoolean && column.Type != colTypeNumber {
			return errors.New("TypeSafe supports string Choice, boolean or number Noul, and number Score columns")
		}
	}
	return nil
}

func (c *assetConfig) readColumns(asset *pipeline.Asset) error {
	seen := make(map[string]bool)
	for _, column := range asset.Columns {
		if seen[column.Name] || column.Name == "" {
			return errors.New("inference column names must be nonempty and unique")
		}
		seen[column.Name] = true
		spec := column.Inference
		if spec == nil {
			continue
		}
		if column.PrimaryKey || strings.TrimSpace(spec.Prompt) == "" {
			return fmt.Errorf("inference column %q must have a prompt and cannot be a primary key", column.Name)
		}
		out := outputColumn{Name: column.Name, Type: column.Type, Prompt: spec.Prompt, Choices: spec.Choices, Minimum: spec.Minimum, Maximum: spec.Maximum, Levels: spec.Levels, Threshold: spec.Threshold}
		switch column.Type {
		case colTypeString, colTypeBoolean, colTypeInteger, colTypeNumber:
		default:
			return fmt.Errorf("inference column %q has an unsupported type", column.Name)
		}
		if spec.Minimum != nil && spec.Maximum != nil && *spec.Minimum > *spec.Maximum {
			return fmt.Errorf("inference column %q has invalid numeric bounds", column.Name)
		}
		if len(spec.Choices) != 0 && column.Type != colTypeString {
			return fmt.Errorf("inference column %q choices require a string type", column.Name)
		}
		if (spec.Minimum != nil || spec.Maximum != nil) && column.Type != colTypeInteger && column.Type != colTypeNumber {
			return fmt.Errorf("inference column %q numeric bounds require integer or number type", column.Name)
		}
		for _, bound := range []*float64{spec.Minimum, spec.Maximum} {
			if bound != nil && (math.IsNaN(*bound) || math.IsInf(*bound, 0) || (column.Type == colTypeInteger && math.Abs(*bound) > 1<<53)) {
				return fmt.Errorf("inference column %q has an unsupported numeric bound", column.Name)
			}
		}
		provider, connection := asset.InferenceProviderConnection(spec)
		model := c.model
		if provider != c.provider && spec.Model == "" {
			return fmt.Errorf("inference column %q changing provider requires a model", column.Name)
		}
		if spec.Connection != "" && strings.TrimSpace(spec.Connection) == "" {
			return fmt.Errorf("inference column %q connection must be a nonempty Bruin connection name", column.Name)
		}
		if spec.Model != "" {
			model = spec.Model
		}
		if !supportedProvider(provider) || strings.TrimSpace(model) == "" {
			return fmt.Errorf("inference column %q requires a supported provider and model", column.Name)
		}
		if provider == providerTypeSafe {
			if err := validateTypeSafeColumn(out); err != nil {
				return fmt.Errorf("TypeSafe column %q: %w", column.Name, err)
			}
		} else if out.Levels != nil || out.Threshold != nil {
			return fmt.Errorf("inference column %q levels and threshold require provider typesafe", column.Name)
		}
		c.groups = append(c.groups, requestGroup{provider: provider, model: model, connection: connection, columns: []outputColumn{out}})
		c.outputs = append(c.outputs, out)
	}
	return nil
}
