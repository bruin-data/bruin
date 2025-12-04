package dataprocserverless

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/logging/logadmin"
	"google.golang.org/api/iterator"
)

type LogLine struct {
	Message string
	Source  string
}

// CloudLoggingConsumer streams logs from Cloud Logging for a Dataproc Serverless batch job.
type CloudLoggingConsumer struct {
	ctx       context.Context
	client    *logadmin.Client
	project   string
	region    string
	batchID   string
	lastFetch time.Time
	seenLogs  map[string]bool
}

func (l *CloudLoggingConsumer) Next() []LogLine {
	if l.seenLogs == nil {
		l.seenLogs = make(map[string]bool)
	}

	lines := []LogLine{}

	// Build filter for Dataproc Serverless batch logs
	// Dataproc Serverless logs are written to Cloud Logging with resource type dataproc_batch
	filter := fmt.Sprintf(
		`resource.type="cloud_dataproc_batch" `+
			`resource.labels.batch_id="%s" `+
			`resource.labels.location="%s" `+
			`timestamp >= "%s"`,
		l.batchID,
		l.region,
		l.lastFetch.UTC().Format(time.RFC3339),
	)

	it := l.client.Entries(l.ctx, logadmin.Filter(filter))

	for {
		entry, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			// Log errors silently and continue - logs are best effort
			break
		}

		// Create a unique key for this log entry to avoid duplicates
		logKey := fmt.Sprintf("%s-%s", entry.InsertID, entry.Timestamp.String())
		if l.seenLogs[logKey] {
			continue
		}
		l.seenLogs[logKey] = true

		// Extract the message from the payload
		message := extractMessage(entry.Payload)
		if message == "" {
			continue
		}

		// Determine the source (driver/executor) from labels
		source := "SPARK"
		if entry.Labels != nil {
			if componentType, ok := entry.Labels["component_type"]; ok {
				source = strings.ToUpper(componentType)
			}
		}

		lines = append(lines, LogLine{
			Message: message,
			Source:  source,
		})

		// Update last fetch time to the latest entry we've seen
		if entry.Timestamp.After(l.lastFetch) {
			l.lastFetch = entry.Timestamp
		}
	}

	return lines
}

func extractMessage(payload interface{}) string {
	switch p := payload.(type) {
	case string:
		return strings.TrimSpace(p)
	case map[string]interface{}:
		// Try common message fields
		if msg, ok := p["message"].(string); ok {
			return strings.TrimSpace(msg)
		}
		if msg, ok := p["textPayload"].(string); ok {
			return strings.TrimSpace(msg)
		}
		// For structured logs, try to get the full content
		if jsonPayload, ok := p["jsonPayload"].(map[string]interface{}); ok {
			if msg, ok := jsonPayload["message"].(string); ok {
				return strings.TrimSpace(msg)
			}
		}
	}
	return ""
}

// NoOpLogConsumer is a log consumer that does nothing.
type NoOpLogConsumer struct{}

func (l *NoOpLogConsumer) Next() []LogLine {
	return nil
}
