package telemetry

import (
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
