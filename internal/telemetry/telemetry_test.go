package telemetry

import (
	"context"
	"testing"
)

// Setup with an exporter configured must succeed (the exporter connects lazily, so no collector
// is needed). This is the path every service takes under Docker Compose.
func TestSetupWithAnExporterConfigured(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	shutdown, err := Setup(context.Background(), "test-service")
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // don't wait to flush to a collector that isn't there
	_ = shutdown(ctx)
}

func TestSetupWithoutAnExporterIsANoOp(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	shutdown, err := Setup(context.Background(), "test-service")
	if err != nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
