package app

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Telemetry struct {
	Provider trace.TracerProvider
	shutdown func(context.Context) error
}

var traceContext = propagation.TraceContext{}

func newTelemetry(role string) (*Telemetry, error) {
	t := &Telemetry{Provider: noop.NewTracerProvider()}
	if env("TRACING_ENABLED", "0") != "1" {
		return t, nil
	}
	rate, err := strconv.ParseFloat(env("TRACE_SAMPLE_RATIO", "0.1"), 64)
	if err != nil || rate < 0 || rate > 1 {
		return nil, fmt.Errorf("invalid TRACE_SAMPLE_RATIO")
	}
	// HTTP exporter starts without contacting the collector. Bounded asynchronous
	// batches drop telemetry on overload, never block coupon admission.
	exp, err := otlptracehttp.New(context.Background(), otlptracehttp.WithEndpointURL(env("OTEL_EXPORTER_OTLP_ENDPOINT", "http://jaeger:4318")), otlptracehttp.WithTimeout(2*time.Second), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
	if err != nil {
		return nil, err
	}
	provider := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(rate))), sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "flashcoupon-"+role))), sdktrace.WithBatcher(exp, sdktrace.WithMaxQueueSize(2048), sdktrace.WithBatchTimeout(500*time.Millisecond)))
	t.Provider = provider
	t.shutdown = provider.Shutdown
	return t, nil
}
func (s *Service) stage(ctx context.Context, name string) (context.Context, func(error)) {
	var tracer trace.Tracer = noop.NewTracerProvider().Tracer("flashcoupon")
	if s.Telemetry != nil {
		tracer = s.Telemetry.Provider.Tracer("flashcoupon")
	}
	ctx, span := tracer.Start(ctx, name)
	started := time.Now()
	return ctx, func(err error) {
		if s.Metrics != nil {
			s.Metrics.Stage.WithLabelValues(name).Observe(time.Since(started).Seconds())
		}
		if err != nil {
			span.SetStatus(codes.Error, "operation failed")
		}
		span.End()
	}
}
func carrier(ctx context.Context) string {
	m := propagation.MapCarrier{}
	traceContext.Inject(ctx, m)
	b, _ := json.Marshal(m)
	return string(b)
}
func restoreTrace(ctx context.Context, raw string) context.Context {
	m := propagation.MapCarrier{}
	if json.Unmarshal([]byte(raw), &m) != nil {
		return ctx
	}
	return traceContext.Extract(ctx, m)
}
func (s *Service) messageContext(ctx context.Context, raw string, retry bool) context.Context {
	original := restoreTrace(context.Background(), raw)
	if retry && trace.SpanContextFromContext(original).IsValid() {
		// New attempt remains in the originating trace and links to its predecessor.
		return trace.ContextWithRemoteSpanContext(ctx, trace.SpanContextFromContext(original))
	}
	return restoreTrace(ctx, raw)
}
func (s *Service) queueSpan(ctx context.Context, name string, from time.Time) {
	if s.Telemetry == nil || from.IsZero() {
		return
	}
	now := time.Now()
	if from.After(now) {
		from = now
	}
	_, span := s.Telemetry.Provider.Tracer("flashcoupon").Start(ctx, name, trace.WithTimestamp(from))
	span.SetAttributes(attribute.Float64("queue.wait_ms", now.Sub(from).Seconds()*1000))
	span.End(trace.WithTimestamp(now))
	s.Metrics.Stage.WithLabelValues(name).Observe(now.Sub(from).Seconds())
}
func (s *Service) traceHTTP() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/metrics" || c.Request.URL.Path == "/health/ready" || c.Request.URL.Path == "/health/live" {
			c.Next()
			return
		}
		ctx := traceContext.Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
		ctx, end := s.stage(ctx, "http.request")
		c.Request = c.Request.WithContext(ctx)
		traceContext.Inject(ctx, propagation.HeaderCarrier(c.Writer.Header()))
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		span := trace.SpanFromContext(ctx)
		span.SetName(c.Request.Method + " " + route)
		span.SetAttributes(attribute.String("http.route", route), attribute.Int("http.status_code", c.Writer.Status()))
		traceContext.Inject(ctx, propagation.HeaderCarrier(c.Writer.Header()))
		if c.Writer.Status() >= 500 {
			end(fmt.Errorf("request failed"))
		} else {
			end(nil)
		}
	}
}
