package telemetry

import (
	"github.com/urfave/cli/v2"
	"runtime"
	"time"

	"github.com/denisbrodbeck/machineid"
	"github.com/rudderlabs/analytics-go/v4"
)

const url = "https://getbruinbumlky.dataplane.rudderstack.com"

var (
	TelemetryKey = ""
	OptOut       = false
	AppVersion   = ""
)

func SendEvent(event string, properties analytics.Properties) {
	if OptOut || TelemetryKey == "" {
		return
	}
	id, _ := machineid.ID()

	client := analytics.New(TelemetryKey, url)
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
