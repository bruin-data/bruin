package bruincloud

import (
	"encoding/json"
	"strings"
)

// Project represents a Bruin Cloud project.
type Project struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	Repo     Repo            `json:"repo"`
	Rollouts json.RawMessage `json:"rollouts"`
}

// Repo represents a git repository configuration.
type Repo struct {
	URL    string `json:"url"`
	Branch string `json:"branch"`
}

// Pipeline represents a Bruin Cloud pipeline.
type Pipeline struct {
	Name               string          `json:"name"`
	Description        *string         `json:"description"`
	Schedule           *string         `json:"schedule"`
	Assets             json.RawMessage `json:"assets"`
	Owner              json.RawMessage `json:"owner"`
	StartDate          string          `json:"start_date"`
	Project            string          `json:"project"`
	DefaultConnections json.RawMessage `json:"default_connections"`
	Commit             *string         `json:"commit"`
	Variables          json.RawMessage `json:"variables"`
	OxrScheduling      *bool           `json:"oxrScheduling"`
	Status             *string         `json:"status"`
}

// PipelineRun represents a pipeline run.
type PipelineRun struct {
	Project                   string          `json:"project"`
	Pipeline                  string          `json:"pipeline"`
	RunID                     string          `json:"run_id"`
	DataIntervalStart         json.RawMessage `json:"data_interval_start"`
	DataIntervalEnd           json.RawMessage `json:"data_interval_end"`
	StartDate                 json.RawMessage `json:"start_date"`
	EndDate                   json.RawMessage `json:"end_date"`
	WallTimeDuration          *float64        `json:"wall_time_duration"`
	WallTimeDurationHumanized *string         `json:"wall_time_duration_humanized"`
	TotalExecutionDuration    *float64        `json:"total_execution_duration"`
	Status                    string          `json:"status"`
	UnknownInstanceCount      int             `json:"unknown_instance_count"`
	Note                      *string         `json:"note"`
}

// Asset represents a pipeline asset.
type Asset struct {
	Project                 string          `json:"project"`
	Pipeline                string          `json:"pipeline"`
	ID                      string          `json:"id"`
	Name                    string          `json:"name"`
	Type                    string          `json:"type"`
	URI                     string          `json:"uri"`
	Description             *string         `json:"description"`
	Content                 *string         `json:"content"`
	Upstreams               json.RawMessage `json:"upstreams"`
	Downstream              json.RawMessage `json:"downstream"`
	Columns                 json.RawMessage `json:"columns"`
	CustomChecks            json.RawMessage `json:"custom_checks"`
	Owner                   json.RawMessage `json:"owner"`
	Materialization         json.RawMessage `json:"materialization"`
	Instance                *string         `json:"instance"`
	Tags                    json.RawMessage `json:"tags"`
	Connection              *string         `json:"connection"`
	Image                   *string         `json:"image"`
	Parameters              json.RawMessage `json:"parameters"`
	Metadata                json.RawMessage `json:"metadata"`
	MarkdownDescription     *string         `json:"markdown_description"`
	QualityScore            int             `json:"quality_score"`
	MaxPossibleQualityScore int             `json:"max_possible_quality_score"`
	QualityScorePercentage  int             `json:"quality_score_percentage"`
}

// AssetInstance represents an asset instance within a pipeline run.
// The API returns dynamic JSON, so we use json.RawMessage for the full response.
type AssetInstance struct {
	json.RawMessage
}

// LogEntry represents a log entry for an asset instance.
type LogEntry struct {
	json.RawMessage
}

// GlossaryEntity represents a glossary entity.
// The API returns dynamic JSON structures, so we use json.RawMessage.
type GlossaryEntity struct {
	json.RawMessage
}

// Agent represents a Bruin Cloud agent.
type Agent struct {
	ID          int     `json:"id"`
	Name        string  `json:"name"`
	Description *string `json:"description"`
}

// AgentThread represents a thread for an agent.
type AgentThread struct {
	ID        int    `json:"id"`
	AgentID   int    `json:"agent_id"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// AgentMessage represents a message in an agent thread.
type AgentMessage struct {
	ID                int             `json:"id"`
	Status            string          `json:"status"`
	OutputMessage     *string         `json:"output_message"`
	AgentLogs         json.RawMessage `json:"agent_logs"`
	QueryLogs         json.RawMessage `json:"query_logs"`
	OutputAttachments json.RawMessage `json:"output_attachments"`
	CreatedAt         string          `json:"created_at"`
	UpdatedAt         string          `json:"updated_at"`
}

// PipelineValidationError represents a pipeline validation error.
// The API returns dynamic JSON structures.
type PipelineValidationError struct {
	json.RawMessage
}

// ExtractDateString extracts a date string from the API's date JSON format.
// API dates come as {"date": "2026-03-06 20:22:29.753319", "timezone_type": 1, "timezone": "+00:00"}.
func ExtractDateString(raw json.RawMessage) string {
	if raw == nil {
		return ""
	}
	var dv struct {
		Date string `json:"date"`
	}
	if err := json.Unmarshal(raw, &dv); err != nil {
		return ""
	}
	if idx := strings.Index(dv.Date, "."); idx > 0 {
		return dv.Date[:idx]
	}
	return dv.Date
}

// AssetInstanceResponse represents the parsed response from the asset instances endpoint.
type AssetInstanceResponse struct {
	Message        string                       `json:"message"`
	AssetInstances map[string]AssetInstanceInfo `json:"asset_instances"`
	RunID          string                       `json:"run_id"`
}

// AssetInstanceInfo represents detailed information about an asset instance.
type AssetInstanceInfo struct {
	Asset                  string             `json:"asset"`
	Type                   string             `json:"type"`
	StartDate              string             `json:"startDate"`
	EndDate                string             `json:"endDate"`
	WallTimeDuration       float64            `json:"wallTimeDuration"`
	TotalExecutionDuration float64            `json:"totalExecutionDuration"`
	Status                 string             `json:"status"`
	IsFinished             bool               `json:"isFinished"`
	Steps                  AssetInstanceSteps `json:"steps"`
	StepIDs                []string           `json:"stepIds"`
}

// AssetInstanceSteps represents the steps of an asset instance.
type AssetInstanceSteps struct {
	Main   []StepInstance      `json:"main"`
	Checks AssetInstanceChecks `json:"checks"`
}

// StepInstance represents a step or check instance.
type StepInstance struct {
	Name       string  `json:"name"`
	StepID     string  `json:"stepId"`
	StartDate  string  `json:"startDate"`
	EndDate    string  `json:"endDate"`
	Duration   float64 `json:"duration"`
	TryNumber  int     `json:"tryNumber"`
	Status     string  `json:"status"`
	IsFinished bool    `json:"isFinished"`
}

// AssetInstanceChecks represents checks for an asset instance.
type AssetInstanceChecks struct {
	Column []AssetInstanceCheck `json:"column"`
	Custom []AssetInstanceCheck `json:"custom"`
}

// AssetInstanceCheck represents a named check with its step instance.
type AssetInstanceCheck struct {
	Name     string       `json:"name"`
	Instance StepInstance `json:"instance"`
}

// APIError represents an error response from the API.
type APIError struct {
	Message    string              `json:"message"`
	Errors     map[string][]string `json:"errors,omitempty"`
	StatusCode int                 `json:"-"`
}

func (e *APIError) Error() string {
	if len(e.Errors) > 0 {
		var b strings.Builder
		b.WriteString(e.Message)
		b.WriteString(":")
		for field, errs := range e.Errors {
			for _, err := range errs {
				b.WriteString(" ")
				b.WriteString(field)
				b.WriteString(" ")
				b.WriteString(err)
				b.WriteString(";")
			}
		}
		return b.String()
	}
	return e.Message
}

// SendAgentMessageResponse represents the response from sending a message to an agent.
type SendAgentMessageResponse struct {
	ThreadID  int `json:"thread_id"`
	MessageID int `json:"message_id"`
}
