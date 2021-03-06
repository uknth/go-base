package metrics

import (
	net_http "net/http"

	kit_metrics "github.com/go-kit/kit/metrics"
)

type (
	// Counter extends kit_metrics.Counter
	Counter interface {
		kit_metrics.Counter
	}

	// Gauge extends kit_metrics.Gauge
	Gauge interface {
		kit_metrics.Gauge
	}

	// Histogram extends kit_metrics.Histogram
	Histogram interface {
		kit_metrics.Histogram
	}

	// Handler interface exposes metrics which support handler
	Handler interface {
		Handler() net_http.Handler
	}

	// Metrics standarizes the metrics interface used by the applications
	Metrics interface {
		NewCounter(name string, sampleRate float64) Counter
		NewHistogram(name string, sampleRate float64) Histogram
		NewGauge(name string) Gauge
	}
)
