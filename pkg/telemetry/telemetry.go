package telemetry

import (
	"context"
	"io"
	"runtime"
	"time"

	"github.com/denisbrodbeck/machineid"
	"github.com/google/uuid"
	"github.com/rudderlabs/analytics-go/v4"
	"github.com/urfave/cli/v2"
)

const (
	url          = "https://getbruinbumlky.dataplane.rudderstack.com"
	startTimeKey = "telemetry_start"
)

type contextKey string

var (
	TelemetryKey = ""
	OptOut       = false
	AppVersion   = ""
	RunID        = ""
	client       analytics.Client
)

func Init() io.Closer {
	client = analytics.New(TelemetryKey, url)

	return client
}

func SendEvent(event string, properties analytics.Properties) {
	if RunID == "" {
		RunID = uuid.New().String()
	}
	if OptOut || TelemetryKey == "" {
		return
	}
	id, _ := machineid.ID()
	properties["run_id"] = RunID

	if client == nil {
		panic("Telemetry client not initialized")
	}

	// Enqueues a track event that will be sent asynchronously.
	_ = client.Enqueue(analytics.Track{
		AnonymousId:       id,
		Event:             event,
		OriginalTimestamp: time.Now().In(time.UTC),
		Context: &analytics.Context{
			App: analytics.AppInfo{
				Name:    "Bruin CLI",
				Version: AppVersion,
			},
			OS: analytics.OSInfo{
				Name: runtime.GOOS + " " + runtime.GOARCH,
			},
		},
		Properties: properties,
	})
}

func SendEventWithAssetStats(event string, stats map[string]int, context *cli.Context) {
	properties := analytics.Properties{
		"assets":        stats,
		"downstream":    context.Bool("downstream"),
		"push_metadata": context.Bool("push-metadata"),
		"full_refresh":  context.Bool("full-refresh"),
		"use_uv":        context.Bool("use-uv"),
		"force":         context.Bool("force"),
	}

	SendEvent(event, properties)
}

func BeforeCommand(c *cli.Context) error {
	start := time.Now()
	c.Context = context.WithValue(c.Context, contextKey(startTimeKey), start)
	SendEvent("command_start", analytics.Properties{
		"command": c.Command.Name,
	})
	return nil
}

func AfterCommand(context *cli.Context) error {
	start := context.Context.Value(contextKey(startTimeKey))
	SendEvent("command_end", analytics.Properties{
		"command":     context.Command.Name,
		"duration_ms": time.Since(start.(time.Time)).Milliseconds(),
	})
	return nil
}

func ErrorCommand(context *cli.Context, err error) {
	if err == nil {
		return
	}
	start := context.Context.Value(contextKey(startTimeKey))

	SendEvent("command_error", analytics.Properties{
		"command":     context.Command.Name,
		"duration_ms": time.Since(start.(time.Time)).Milliseconds(),
	})
}
