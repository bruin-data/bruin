package telemetry

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/denisbrodbeck/machineid"
	"github.com/google/uuid"
	"github.com/rudderlabs/analytics-go/v4"
	"github.com/urfave/cli/v3"
)

const (
	url          = "https://getbruinbumlky.dataplane.rudderstack.com"
	startTimeKey = "telemetry_start"
)

type contextKey string

var TelemetryKey string
var (
	OptOut       = false
	AppVersion   = ""
	RunID        = ""
	TemplateName = "" // Stores template name for init command (protected by lock)
	client       analytics.Client
	lock         sync.Mutex
)

// SetTemplateName stores the template name for telemetry (thread-safe).
func SetTemplateName(name string) {
	lock.Lock()
	defer lock.Unlock()
	TemplateName = name
}

func Init() io.Closer {
	client = analytics.New(TelemetryKey, url)

	return client
}

func SendEvent(event string, properties analytics.Properties) {
	lock.Lock()
	defer lock.Unlock()
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

func SendEventWithAssetStats(event string, stats map[string]int, cmd *cli.Command) {
	properties := analytics.Properties{
		"assets": stats,
	}

	if cmd != nil {
		properties["downstream"] = cmd.Bool("downstream")
		properties["push_metadata"] = cmd.Bool("push-metadata")
		properties["full_refresh"] = cmd.Bool("full-refresh")
		properties["use_pip"] = cmd.Bool("use-pip")
		properties["force"] = cmd.Bool("force")
	} else {
		properties["downstream"] = false
		properties["push_metadata"] = false
		properties["full_refresh"] = false
		properties["use_pip"] = false
		properties["force"] = false
	}

	SendEvent(event, properties)
}

func BeforeCommand(ctx context.Context, c *cli.Command) (context.Context, error) {
	start := time.Now()
	ctx = context.WithValue(ctx, contextKey(startTimeKey), start)
	SendEvent("command_start", analytics.Properties{
		"command": c.Name,
	})
	return ctx, nil
}

func AfterCommand(ctx context.Context, cmd *cli.Command) error {
	start := ctx.Value(contextKey(startTimeKey))
	durationMs := int64(-1)
	if start != nil {
		durationMs = time.Since(start.(time.Time)).Milliseconds()
	}
	properties := analytics.Properties{
		"command":     cmd.Name,
		"duration_ms": durationMs,
	}

	// Add template_name for init command (read and clear under lock)
	lock.Lock()
	if TemplateName != "" && cmd.Name == "init" {
		properties["template_name"] = TemplateName
		TemplateName = "" // Clear after use
	}
	lock.Unlock()

	SendEvent("command_end", properties)
	return nil
}

func ErrorCommand(ctx context.Context, cmd *cli.Command, err error) error {
	if err == nil {
		return nil
	}
	fmt.Println(err)
	start := ctx.Value(contextKey(startTimeKey))
	startTime, err2 := start.(time.Time)
	if !err2 {
		return nil
	}

	SendEvent("command_error", analytics.Properties{
		"command":     cmd.Name,
		"duration_ms": time.Since(startTime).Milliseconds(),
	})
	return nil
}
