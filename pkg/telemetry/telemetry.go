package telemetry

import (
	"runtime"
	"time"

	"github.com/denisbrodbeck/machineid"
	"github.com/rudderlabs/analytics-go/v4"
)

const url = "https://getbruinbumlky.dataplane.rudderstack.com"

type Telemetry struct {
	telemetryKey string
	optOut       bool
	appVersion   string
}

func NewTelemetry(telemetryKey, version string, optOut bool) *Telemetry {
	return &Telemetry{
		telemetryKey: telemetryKey,
		optOut:       optOut,
		appVersion:   version,
	}
}

func (t *Telemetry) SendEvent(event string, properties analytics.Properties) {
	if t.optOut {
		return
	}
	id, _ := machineid.ID()

	client := analytics.New(t.telemetryKey, url)
	// Enqueues a track event that will be sent asynchronously.
	_ = client.Enqueue(analytics.Track{
		AnonymousId:       id,
		Event:             event,
		OriginalTimestamp: time.Now().In(time.UTC),
		Context: &analytics.Context{
			App: analytics.AppInfo{
				Name:    "Bruin CLI",
				Version: t.appVersion,
			},
			OS: analytics.OSInfo{
				Name: runtime.GOOS + " " + runtime.GOARCH,
			},
		},
		Properties: properties,
	})
}
