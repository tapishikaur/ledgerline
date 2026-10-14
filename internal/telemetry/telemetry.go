// Package telemetry sets up OpenTelemetry tracing and gRPC instrumentation.
package telemetry

import (
	"context"
	"os"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc/filters"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc"
)

// Setup installs the W3C trace-context propagator (always, so trace IDs flow even without an
// exporter) and, if OTEL_EXPORTER_OTLP_ENDPOINT is set (e.g. http://jaeger:4317), an OTLP
// exporter that batches spans to it. Returns a function that flushes and stops tracing.
func Setup(ctx context.Context, service string) (func(context.Context) error, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") == "" {
		return func(context.Context) error { return nil }, nil
	}
	exporter, err := otlptracegrpc.New(ctx) // reads the endpoint and options from OTEL_* variables
	if err != nil {
		return nil, err
	}
	// Schemaless: merging attributes tagged with one semantic-convention schema version into the
	// SDK's default resource (tagged with another) fails with "conflicting Schema URL".
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(semconv.ServiceName(service)))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(provider)
	return provider.Shutdown, nil
}

// ServerOption instruments a gRPC server: every call becomes a span, continuing the caller's
// trace. Health checks (every 2 s from Docker) are left out: they'd bury the real traces.
func ServerOption() grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithFilter(filters.Not(filters.HealthCheck()))))
}

// DialOption instruments a gRPC client: every call becomes a span and carries the trace context.
func DialOption() grpc.DialOption { return grpc.WithStatsHandler(otelgrpc.NewClientHandler()) }
