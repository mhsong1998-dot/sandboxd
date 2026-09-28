package trace

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// InitOTLP enables the same W3C parent-based sampling used by Adxlet.
// sandboxd and its local collector share the node container network namespace.
func InitOTLP(ctx context.Context) (func(), error) {
	if os.Getenv("ADX_TRACE_ENABLED") != "true" {
		return func() {}, nil
	}
	ratio := 1.0
	if value := os.Getenv("ADX_TRACE_SAMPLE_RATIO"); value != "" {
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || parsed < 0 || parsed > 1 {
			return nil, fmt.Errorf("invalid ADX_TRACE_SAMPLE_RATIO: %q", value)
		}
		ratio = parsed
	}
	exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpoint("127.0.0.1:4318"), otlptracehttp.WithInsecure(), otlptracehttp.WithTimeout(2*time.Second))
	if err != nil {
		return nil, err
	}
	res := resource.NewWithAttributes("", attribute.String("service.name", "akernel-sandboxd"))
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))),
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = provider.Shutdown(shutdown)
	}, nil
}
